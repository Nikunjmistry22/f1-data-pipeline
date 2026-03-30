"""
src/extractor.py

F1 Data Pipeline — OpenF1 API → Supabase (raw schema)
Race sessions only. 8 tables. Analytics-driven, no bloat.

Tables populated per race session:
  raw.meetings          → GP weekend context (circuit, country)
  raw.sessions          → session metadata (confirms Race type)
  raw.drivers           → driver + team info (SCD Type 2 source for dbt)
  raw.laps              → lap times, sector splits, speed traps  ← core fact
  raw.stints            → tyre compound + age per stint          ← strategy
  raw.pit               → pit stop timing per driver             ← strategy
  raw.session_results   → final race standings, winner           ← outcome
  raw.car_data_lap_agg  → telemetry aggregated per lap (Polars)  ← performance

API call order (dependency chain):
  meetings + sessions   (year-level, calendar sync)
  drivers               (session_key)
  laps                  (session_key + driver_number, per driver)
  car_data → agg        (session_key + driver_number, needs laps for boundaries)
  stints                (session_key)
  pit                   (session_key)
  session_results       (session_key)
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import polars as pl
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("f1.extractor")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OPENF1_BASE_URL  = "https://api.openf1.org/v1"
REQUEST_TIMEOUT  = 60
RETRY_ATTEMPTS   = 3
RETRY_DELAYS     = [5, 15, 30]
CALL_PAUSE_SECS  = 1.0
BATCH_SIZE       = 500
DRS_ON_VALUES    = {10, 12, 14}
RACE_SESSION_NAMES = {"Race"}


# ---------------------------------------------------------------------------
# DB — direct psycopg2, bypasses PostgREST entirely
# Get SUPABASE_DB_URL from:
#   Supabase dashboard → Project Settings → Database
#   → Connection string → Transaction mode
# ---------------------------------------------------------------------------
_conn: psycopg2.extensions.connection | None = None


def get_db() -> psycopg2.extensions.connection:
    global _conn

    db_url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not db_url:
        raise EnvironmentError(
            "SUPABASE_DB_URL not set.\n"
            "  Local: add to .env\n"
            "  GitHub Actions: add as repository secret\n"
            "  Format: postgresql://postgres.REF:PASSWORD"
            "@aws-X-REGION.pooler.supabase.com:6543/postgres"
        )

    def _connect() -> psycopg2.extensions.connection:
        c = psycopg2.connect(
            db_url,
            connect_timeout=15,
            options="-c statement_timeout=300000",
        )
        c.autocommit = False
        return c

    if _conn is None or _conn.closed:
        _conn = _connect()
        return _conn

    try:
        _conn.cursor().execute("SELECT 1")
    except psycopg2.OperationalError:
        log.warning("DB connection stale — reconnecting")
        _conn = _connect()

    return _conn


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------
def _exec(sql: str, params: tuple = ()) -> None:
    conn = get_db()
    cur  = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _fetchall(sql: str, params: tuple = ()) -> list[dict]:
    conn = get_db()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        cur.close()


def _fetchone(sql: str, params: tuple = ()) -> dict | None:
    rows = _fetchall(sql, params)
    return rows[0] if rows else None

def _log_table_load(table: str, rows: int) -> None:
    log.info(f"[LOAD] {table:<30} → {rows:>6} rows")

def _upsert(schema: str, table: str, rows: list[dict], run_id: str) -> int:
    """
    Batch INSERT … ON CONFLICT DO NOTHING.
    Idempotent — duplicates silently skipped.
    JSONB columns (list/dict) are json.dumps'd automatically.
    """
    if not rows:
        log.info(f"    {schema}.{table}: 0 rows")
        return 0

    ts = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r["_loaded_at"] = ts
        r["_run_id"]    = run_id

    # Union of all keys (API may omit optional fields on some rows)
    seen: set[str] = set()
    cols: list[str] = []
    for r in rows:
        for k in r:
            if k not in seen:
                cols.append(k)
                seen.add(k)

    cols_sql = ", ".join(f'"{c}"' for c in cols)
    conn = get_db()
    cur  = conn.cursor()
    sent = 0

    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch  = rows[i : i + BATCH_SIZE]
            values = [
                tuple(
                    json.dumps(r.get(c))
                    if isinstance(r.get(c), (dict, list))
                    else r.get(c)
                    for c in cols
                )
                for r in batch
            ]
            psycopg2.extras.execute_values(
                cur,
                f'INSERT INTO {schema}."{table}" ({cols_sql}) '
                f'VALUES %s ON CONFLICT DO NOTHING',
                values,
                page_size=BATCH_SIZE,
            )
            sent += len(batch)

        conn.commit()
        log.info(f"    {schema}.{table}: {sent} rows sent")

    except Exception as exc:
        conn.rollback()
        log.error(f"    INSERT failed {schema}.{table}: {exc}")
        raise
    finally:
        cur.close()

    return sent


def _log_dq(run_id: str, session_key: int, table: str,
            check: str, passed: bool, details: str = "") -> None:
    try:
        _exec(
            "INSERT INTO pipeline.dq_log "
            "(run_id, session_key, table_name, check_name, passed, details) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (run_id, session_key, table, check, passed, details),
        )
    except Exception as exc:
        log.warning(f"DQ log write failed: {exc}")

# --------------------------------------------------------------------------
# Schema Control
# ---------------------------------------------------------------------------
ALLOWED_LAPS_COLUMNS = {
    "meeting_key",
    "session_key",
    "driver_number",
    "lap_number",
    "date_start",
    "duration_sector_1",
    "duration_sector_2",
    "duration_sector_3",
    "i1_speed",
    "i2_speed",
    "is_pit_out_lap",
    "lap_duration",
    "st_speed",
}
def _filter_columns(rows: list[dict], allowed: set[str]) -> list[dict]:
    return [
        {k: v for k, v in r.items() if k in allowed}
        for r in rows
    ]
# ---------------------------------------------------------------------------
# OpenF1 HTTP
# ---------------------------------------------------------------------------
def _call_api(endpoint: str, params: dict[str, Any]) -> list[dict]:
    url = f"{OPENF1_BASE_URL}/{endpoint}"

    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            resp = httpx.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                data = resp.json()
                log.info(f"  ✓ /{endpoint} {params} → {len(data)} rows")
                return data

            if resp.status_code == 429:
                wait = RETRY_DELAYS[min(attempt - 1, 2)] * 2
                log.warning(f"  Rate limited /{endpoint} — wait {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = RETRY_DELAYS[min(attempt - 1, 2)]
                log.warning(f"  HTTP {resp.status_code} /{endpoint} — wait {wait}s")
                time.sleep(wait)
                continue

            log.error(f"  HTTP {resp.status_code} /{endpoint}: {resp.text[:200]}")
            return []

        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as exc:
            wait = RETRY_DELAYS[min(attempt - 1, 2)]
            log.warning(f"  Network error /{endpoint}: {exc} — wait {wait}s")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(wait)

    raise RuntimeError(
        f"/{endpoint} failed after {RETRY_ATTEMPTS} attempts. params={params}"
    )


# ---------------------------------------------------------------------------
# car_data → per-lap aggregation via Polars
# ---------------------------------------------------------------------------
def _aggregate_car_data(
    raw_rows: list[dict],
    session_key: int,
    meeting_key: int,
    laps_rows: list[dict],
) -> list[dict]:
    """
    Aggregate 3.7Hz telemetry → per-lap metrics.
    ~400k raw rows → ~1400 aggregated rows per race (20 drivers × ~70 laps).

    Output columns match raw.car_data_lap_agg:
      session_key, meeting_key, driver_number, lap_number,
      avg_speed, max_speed, min_speed,
      avg_throttle, avg_rpm, max_rpm,
      avg_brake, drs_usage_pct, gear_changes, sample_count
    """
    if not raw_rows or not laps_rows:
        return []

    log.info(f"  Aggregating {len(raw_rows):,} telemetry rows …")

    boundaries = [
        {
            "driver_number": int(r["driver_number"]),
            "lap_number":    int(r["lap_number"]),
            "lap_start":     r["date_start"],
        }
        for r in laps_rows
        if r.get("date_start") and r.get("lap_number") and r.get("driver_number")
    ]
    if not boundaries:
        log.warning("  No lap boundaries — skipping aggregation")
        return []

    df = pl.DataFrame(raw_rows)
    for col in {"date", "driver_number", "speed", "throttle",
                "rpm", "brake", "drs", "n_gear"} - set(df.columns):
        df = df.with_columns(pl.lit(None).alias(col))

    df = df.with_columns([
        pl.col("date").cast(pl.Utf8)
          .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z",
                           strict=False),
        pl.col("driver_number").cast(pl.Int64, strict=False),
        pl.col("speed").cast(pl.Float64, strict=False),
        pl.col("throttle").cast(pl.Float64, strict=False),
        pl.col("rpm").cast(pl.Float64, strict=False),
        pl.col("brake").cast(pl.Float64, strict=False),
        pl.col("drs").cast(pl.Int64, strict=False),
        pl.col("n_gear").cast(pl.Int64, strict=False),
    ])

    laps_df = (
        pl.DataFrame(boundaries)
        .with_columns(
            pl.col("lap_start").cast(pl.Utf8)
              .str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z",
                               strict=False)
        )
        .sort(["driver_number", "lap_start"])
    )

    drs_on  = list(DRS_ON_VALUES)
    results = []

    for driver_num in df["driver_number"].drop_nulls().unique().to_list():
        drv  = df.filter(pl.col("driver_number") == driver_num).sort("date")
        dlps = laps_df.filter(pl.col("driver_number") == driver_num).sort("lap_start")

        if drv.is_empty() or dlps.is_empty():
            continue

        starts  = dlps["lap_start"].to_list()
        numbers = dlps["lap_number"].to_list()

        assigned = []
        for dt in drv["date"].to_list():
            if dt is None:
                assigned.append(None)
                continue
            lap = None
            for idx, (ls, ln) in enumerate(zip(starts, numbers)):
                if ls and dt >= ls:
                    if idx + 1 < len(starts) and starts[idx + 1]:
                        if dt < starts[idx + 1]:
                            lap = ln
                            break
                    else:
                        lap = ln
                        break
            assigned.append(lap)

        drv = (
            drv.with_columns(pl.Series("lap_number", assigned, dtype=pl.Int64))
               .filter(pl.col("lap_number").is_not_null())
               .sort(["lap_number", "date"])
        )
        if drv.is_empty():
            continue

        # Gear changes per lap (sequential diff, can't do in group_by)
        gear_ch: dict[int, int] = {}
        for ln in drv["lap_number"].unique().to_list():
            gears = (
                drv.filter(pl.col("lap_number") == ln)
                   .sort("date")["n_gear"].drop_nulls().to_list()
            )
            gear_ch[ln] = sum(
                1 for i in range(1, len(gears)) if gears[i] != gears[i - 1]
            )

        agg = (
            drv.group_by("lap_number")
               .agg([
                   pl.col("speed").mean().round(2).alias("avg_speed"),
                   pl.col("speed").max().cast(pl.Int64).alias("max_speed"),
                   pl.col("speed").min().cast(pl.Int64).alias("min_speed"),
                   pl.col("throttle").mean().round(2).alias("avg_throttle"),
                   pl.col("rpm").mean().round(2).alias("avg_rpm"),
                   pl.col("rpm").max().cast(pl.Int64).alias("max_rpm"),
                   ((pl.col("brake") == 100).mean() * 100).round(2).alias("avg_brake"),
                   (pl.col("drs").is_in(drs_on).mean() * 100).round(2).alias("drs_usage_pct"),
                   pl.len().alias("sample_count"),
               ])
               .with_columns([
                   pl.lit(session_key).alias("session_key"),
                   pl.lit(meeting_key).alias("meeting_key"),
                   pl.lit(driver_num).alias("driver_number"),
               ])
        )

        for r in agg.to_dicts():
            r["gear_changes"] = gear_ch.get(r["lap_number"], 0)
            results.append(r)

    log.info(f"  Aggregation done → {len(results)} lap-level rows")
    return results


# ---------------------------------------------------------------------------
# Main extraction — Race sessions only
# ---------------------------------------------------------------------------
def extract_session(
    session_key: int,
    meeting_key: int,
    year: int,
    run_id: str,
    session_name: str = "Race",
) -> dict[str, int]:
    """
    Extract all 8 tables for one Race session.
    Non-Race sessions are skipped cleanly (marked 'skipped' in schedule).
    """
    if session_name not in RACE_SESSION_NAMES:
        log.info(f"Skipping session_key={session_key} — '{session_name}' is not a Race")
        _exec(
            "UPDATE pipeline.sessions_schedule "
            "SET status='skipped', updated_at=NOW() WHERE session_key=%s",
            (session_key,),
        )
        return {}

    log.info("=" * 60)
    log.info(f"RACE  session_key={session_key}  meeting_key={meeting_key}  year={year}")
    log.info(f"run_id={run_id}")
    log.info("=" * 60)

    counts: dict[str, int] = {}

    _exec(
        "UPDATE pipeline.sessions_schedule "
        "SET status='running', updated_at=NOW() "
        "WHERE session_key=%s AND status='pending'",
        (session_key,),
    )

    try:
        # ── Step 1: drivers ──────────────────────────────────────────
        log.info("Step 1: drivers")
        drivers = _call_api("drivers", {"session_key": session_key})
        time.sleep(CALL_PAUSE_SECS)
        counts["drivers"] = _upsert("raw", "drivers", drivers, run_id)
        
        _log_dq(run_id, session_key, "raw.drivers", "non_empty",
                bool(drivers), f"{len(drivers)} rows")

        driver_numbers: list[int] = sorted({
            int(d["driver_number"])
            for d in drivers
            if d.get("driver_number") is not None
        })
        log.info(f"  {len(driver_numbers)} drivers: {driver_numbers}")

        # ── Steps 2–3: laps + car_data per driver ────────────────────
        log.info(f"Steps 2–3: laps + car_data (×{len(driver_numbers)} drivers)")

        all_laps:     list[dict] = []
        all_car_data: list[dict] = []

        for dn in driver_numbers:
            laps = _call_api("laps", {"session_key": session_key, "driver_number": dn})
            time.sleep(CALL_PAUSE_SECS)
            all_laps.extend(laps)

            car = _call_api("car_data", {"session_key": session_key, "driver_number": dn})
            time.sleep(CALL_PAUSE_SECS)
            all_car_data.extend(car)

        # counts["laps"] = _upsert("raw", "laps", all_laps, run_id)
        clean_laps = _filter_columns(all_laps, ALLOWED_LAPS_COLUMNS)
        counts["laps"] = _upsert("raw", "laps", clean_laps, run_id)
        _log_table_load("raw.laps", counts["laps"])

        log.info("Step 3b: aggregate car_data → car_data_lap_agg")
        car_agg = _aggregate_car_data(all_car_data, session_key, meeting_key, all_laps)
        counts["car_data_lap_agg"] = _upsert("raw", "car_data_lap_agg", car_agg, run_id)
        log.info(f"  {len(all_car_data):,} raw rows → {len(car_agg)} agg rows")
        _log_table_load("raw.car_data_lap_agg", counts["car_data_lap_agg"])

        # ── Step 4: stints ───────────────────────────────────────────
        log.info("Step 4: stints")
        stints = _call_api("stints", {"session_key": session_key})
        time.sleep(CALL_PAUSE_SECS)
        counts["stints"] = _upsert("raw", "stints", stints, run_id)
        _log_table_load("raw.stints", counts["stints"])

        # ── Step 5: pit ──────────────────────────────────────────────
        log.info("Step 5: pit")
        pit = _call_api("pit", {"session_key": session_key})
        time.sleep(CALL_PAUSE_SECS)
        counts["pit"] = _upsert("raw", "pit", pit, run_id)
        _log_table_load("raw.pit", counts["pit"])

        # ── Step 6: session_results ──────────────────────────────────
        log.info("Step 6: session_results")
        results = _call_api("session_result", {"session_key": session_key})
        time.sleep(CALL_PAUSE_SECS)
        for r in results:
            val = r.get("gap_to_leader")

            if val is not None:
                if isinstance(val, str):
                    # Keep as-is (requires TEXT column)
                    pass
                else:
                    r["gap_to_leader"] = str(val)
        counts["session_results"] = _upsert("raw", "session_results", results, run_id)
        _log_table_load("raw.session_results", counts["session_results"])

        # ── Mark done ────────────────────────────────────────────────
        _exec(
            "UPDATE pipeline.sessions_schedule "
            "SET status='done', extracted_at=NOW(), last_error=NULL, updated_at=NOW() "
            "WHERE session_key=%s",
            (session_key,),
        )

    except Exception as exc:
        log.error(f"Extraction failed: {exc}", exc_info=True)
        try:
            _exec("SELECT pipeline.fail_session(%s, %s)",
                  (session_key, str(exc)[:2000]))
        except Exception as e2:
            log.error(f"Could not update fail status: {e2}")
        raise

    log.info("=" * 60)
    log.info("Extraction complete:")
    total = 0
    for tbl, cnt in sorted(counts.items()):
        log.info(f"  {tbl:<25} {cnt:>6} rows")
        total += max(cnt, 0)
    log.info(f"  {'TOTAL':<25} {total:>6} rows")
    log.info("=" * 60)

    return counts


# ---------------------------------------------------------------------------
# Calendar sync — Race sessions only go into schedule
# ---------------------------------------------------------------------------
def sync_calendar(year: int, run_id: str) -> int:
    """
    Sync F1 calendar for a year.
    meetings + sessions → raw schema (dimension tables for dbt).
    Only Race sessions → pipeline.sessions_schedule.
    Returns count of Race sessions scheduled.
    """
    log.info(f"Syncing calendar year={year}")

    sessions = _call_api("sessions", {"year": year})
    time.sleep(CALL_PAUSE_SECS)
    meetings = _call_api("meetings",  {"year": year})
    time.sleep(CALL_PAUSE_SECS)

    if meetings:
        _upsert("raw", "meetings", meetings, run_id)
    if sessions:
        _upsert("raw", "sessions", sessions, run_id)

    if not sessions:
        log.warning(f"  No sessions for {year}")
        return 0

    race_sessions = [
        s for s in sessions
        if s.get("session_name") in RACE_SESSION_NAMES
    ]
    log.info(f"  {len(sessions)} total → {len(race_sessions)} Race sessions")

    ok = 0
    for s in race_sessions:
        sk           = s.get("session_key")
        date_end_raw = s.get("date_end")
        if not sk or not date_end_raw:
            continue

        try:
            date_end      = datetime.fromisoformat(
                str(date_end_raw).replace("Z", "+00:00")
            )
            trigger_after = date_end + timedelta(hours=1)
        except (ValueError, TypeError) as exc:
            log.warning(f"  Bad date_end for session {sk}: {exc}")
            continue

        try:
            _exec(
                """
                INSERT INTO pipeline.sessions_schedule
                    (session_key, meeting_key, year, country_name,
                     circuit_short_name, session_name, session_type,
                     date_start, date_end, gmt_offset, trigger_after, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'pending')
                ON CONFLICT (session_key) DO NOTHING
                """,
                (
                    int(sk),
                    int(s.get("meeting_key") or 0),
                    int(s.get("year")        or year),
                    s.get("country_name")    or "",
                    s.get("circuit_short_name"),
                    s.get("session_name")    or "",
                    s.get("session_type")    or "",
                    s.get("date_start"),
                    date_end_raw,
                    s.get("gmt_offset"),
                    trigger_after.isoformat(),
                ),
            )
            ok += 1
        except Exception as exc:
            log.error(f"  Could not schedule session {sk}: {exc}")

    log.info(f"  Scheduled {ok} Race sessions for {year}")
    return ok


# ---------------------------------------------------------------------------
# Watchdog — hourly cron entrypoint
# ---------------------------------------------------------------------------
def run_watchdog(github_run_id: str = "local") -> None:
    """
    1. Sync current + next year calendar (fast, idempotent)
    2. Off-season check — exit cleanly if >60 days to next race
    3. Find Race sessions where trigger_after <= NOW()
    4. Extract each one, notify Slack on success/failure
    """
    now = datetime.now(timezone.utc)
    log.info(f"Watchdog  {now.isoformat()}  run={github_run_id}")

    for yr in (now.year, now.year + 1):
        try:
            sync_calendar(yr, f"{github_run_id}_cal")
        except Exception as exc:
            log.warning(f"Calendar sync {yr} failed (non-fatal): {exc}")

    nxt = _fetchone(
        """
        SELECT session_key, country_name, session_name, trigger_after
        FROM pipeline.sessions_schedule
        WHERE status = 'pending' AND trigger_after > %s
        ORDER BY trigger_after LIMIT 1
        """,
        (now,),
    )

    if not nxt:
        log.info("No pending sessions — end of season or calendar not published.")
        _write_gha_output("action", "idle_end_of_season")
        sys.exit(0)

    trigger = nxt["trigger_after"]
    if isinstance(trigger, str):
        trigger = datetime.fromisoformat(trigger.replace("Z", "+00:00"))
    if trigger.tzinfo is None:
        trigger = trigger.replace(tzinfo=timezone.utc)

    days_until = (trigger - now).days
    if days_until > 60:
        log.info(
            f"Off-season: {days_until}d until "
            f"{nxt['country_name']} {nxt['session_name']}. Idle."
        )
        _write_gha_output("action", f"idle_off_season_{days_until}d")
        sys.exit(0)

    ready = _fetchall(
        """
        SELECT * FROM pipeline.sessions_schedule
        WHERE status IN ('pending','failed')
          AND trigger_after <= %s
          AND retry_count < 3
        ORDER BY trigger_after
        """,
        (now,),
    )

    if not ready:
        log.info(f"Nothing ready yet. Next in {days_until}d: {nxt['country_name']}")
        _write_gha_output("action", "idle_waiting")
        sys.exit(0)

    log.info(f"{len(ready)} session(s) ready to extract")
    overall_ok = True

    for s in ready:
        sk     = s["session_key"]
        mk     = s["meeting_key"]
        yr     = s["year"]
        sname  = s["session_name"]
        name   = f"{yr} {s['country_name']} {sname}"
        run_id = f"{github_run_id}_{sk}"

        try:
            _exec(
                "INSERT INTO pipeline.run_log "
                "(run_id, session_key, meeting_key, year, action, status) "
                "VALUES (%s,%s,%s,%s,'extract','running')",
                (run_id, sk, mk, yr),
            )
        except Exception:
            pass

        try:
            counts = extract_session(
                session_key=sk, meeting_key=mk, year=yr,
                run_id=run_id, session_name=sname,
            )
            _exec(
                "UPDATE pipeline.run_log "
                "SET status='success', rows_loaded=%s, ended_at=NOW() "
                "WHERE run_id=%s",
                (json.dumps(counts), run_id),
            )
            _send_slack_success(name, counts, run_id)
            log.info(f"✓ {name}")

        except Exception as exc:
            overall_ok = False
            err = str(exc)
            log.error(f"✗ {name}: {err}")
            try:
                _exec(
                    "UPDATE pipeline.run_log "
                    "SET status='failed', error_message=%s, ended_at=NOW() "
                    "WHERE run_id=%s",
                    (err[:2000], run_id),
                )
            except Exception:
                pass
            _send_slack_failure(name, err, run_id, s.get("retry_count", 0) + 1)

    sys.exit(0 if overall_ok else 1)


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------
def _send_slack(payload: dict) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL", "").strip()
    if not webhook:
        return
    try:
        httpx.post(webhook, json=payload, timeout=10)
    except Exception as exc:
        log.warning(f"Slack failed: {exc}")


def _send_slack_success(name: str, counts: dict[str, int], run_id: str) -> None:
    rows = "\n".join(f"• *{t}*: {c:,}" for t, c in sorted(counts.items()) if c >= 0)
    _send_slack({
        "text": f":checkered_flag: F1 Extract Complete — {name}",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn",
            "text": f":checkered_flag: *{name}*\n{rows}\n`{run_id}`"}}],
    })


def _send_slack_failure(name: str, error: str, run_id: str, attempts: int) -> None:
    left  = max(0, 3 - attempts)
    retry = f":arrows_counterclockwise: {left} retries left" if left else ":x: Max retries reached"
    _send_slack({
        "text": f":rotating_light: F1 Extract FAILED — {name}",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn",
            "text": (f":rotating_light: *FAILED* {name}\n"
                     f"```{error[:400]}```\n{retry}\n`{run_id}`")}}],
    })


def _write_gha_output(key: str, value: str) -> None:
    f = os.getenv("GITHUB_OUTPUT", "")
    if f:
        with open(f, "a") as fh:
            fh.write(f"{key}={value}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="F1 Pipeline Extractor")
    sub    = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("watchdog")
    p.add_argument("--run-id", default="local")

    p = sub.add_parser("sync")
    p.add_argument("--year", type=int, default=datetime.now().year)

    p = sub.add_parser("extract")
    p.add_argument("--session-key",  type=int, required=True)
    p.add_argument("--meeting-key",  type=int, required=True)
    p.add_argument("--year",         type=int, required=True)
    p.add_argument("--session-name", default="Race")

    p = sub.add_parser("backfill")
    p.add_argument("--year",    type=int, required=True)
    p.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    if args.cmd == "watchdog":
        run_watchdog(github_run_id=args.run_id)

    elif args.cmd == "sync":
        n = sync_calendar(args.year, f"cli_sync_{args.year}")
        print(f"Scheduled {n} Race sessions for {args.year}")

    elif args.cmd == "extract":
        counts = extract_session(
            session_key  = args.session_key,
            meeting_key  = args.meeting_key,
            year         = args.year,
            run_id       = f"cli_{args.session_key}",
            session_name = args.session_name,
        )
        print(f"Done: {counts}")

    elif args.cmd == "backfill":
        now = datetime.now(timezone.utc)
        sync_calendar(args.year, f"backfill_sync_{args.year}")

        past = _fetchall(
            """
            SELECT * FROM pipeline.sessions_schedule
            WHERE year = %s
              AND session_name = 'Race'
              AND date_end < %s
              AND status NOT IN ('done','skipped')
            ORDER BY date_end
            """,
            (args.year, now),
        )

        print(f"Found {len(past)} Race sessions to backfill for {args.year}")

        if args.dry_run:
            for s in past:
                print(
                    f"  {s['country_name']:<22} "
                    f"key={s['session_key']}  "
                    f"ends={str(s['date_end'])[:10]}"
                )
            sys.exit(0)

        ok = failed = 0
        for s in past:
            try:
                extract_session(
                    session_key  = s["session_key"],
                    meeting_key  = s["meeting_key"],
                    year         = s["year"],
                    run_id       = f"backfill_{s['session_key']}",
                    session_name = s["session_name"],
                )
                ok += 1
            except Exception as exc:
                log.error(f"  FAILED {s['country_name']}: {exc}")
                failed += 1
            time.sleep(3)

        print(f"\nBackfill done — {ok} succeeded, {failed} failed")
        sys.exit(0 if not failed else 1)