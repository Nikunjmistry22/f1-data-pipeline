"""
src/extractor/openf1_extractor.py

Pulls all OpenF1 endpoints for a given session_key.
Uses Polars for aggregation of high-frequency telemetry (car_data).
Inserts into Supabase raw schema with ON CONFLICT DO NOTHING (idempotent).
"""

from __future__ import annotations

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any

import httpx
import polars as pl
from supabase import create_client, Client

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
OPENF1_BASE = "https://api.openf1.org/v1"
MAX_RETRIES = 3
RETRY_BACKOFF = [5, 15, 30]          # seconds between retries
REQUEST_TIMEOUT = 60                  # seconds per HTTP request
RATE_LIMIT_PAUSE = 1.2               # seconds between API calls (polite)

# DRS "on" values per OpenF1 docs
DRS_ON_VALUES = {10, 12, 14}


# ---------------------------------------------------------------------------
# Supabase client factory
# ---------------------------------------------------------------------------
def get_supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]   # never use anon key in pipeline
    return create_client(url, key)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def fetch_endpoint(
    endpoint: str,
    params: dict[str, Any],
    run_id: str,
) -> list[dict]:
    """
    Fetch an OpenF1 endpoint with retry + exponential backoff.
    Returns list of records or raises after max retries.
    """
    url = f"{OPENF1_BASE}/{endpoint}"

    for attempt in range(MAX_RETRIES):
        try:
            with httpx.Client(timeout=REQUEST_TIMEOUT) as client:
                response = client.get(url, params=params)

            if response.status_code == 200:
                data = response.json()
                logger.info(
                    f"[{run_id}] {endpoint}: fetched {len(data)} records "
                    f"(attempt {attempt + 1})"
                )
                return data

            elif response.status_code == 429:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)] * 2
                logger.warning(f"[{run_id}] Rate limited on {endpoint}. Waiting {wait}s")
                time.sleep(wait)

            elif response.status_code >= 500:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                logger.warning(
                    f"[{run_id}] Server error {response.status_code} on {endpoint}. "
                    f"Waiting {wait}s"
                )
                time.sleep(wait)
            else:
                logger.error(
                    f"[{run_id}] Unexpected status {response.status_code} on {endpoint}"
                )
                return []

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            logger.warning(f"[{run_id}] Network error on {endpoint}: {e}. Waiting {wait}s")
            time.sleep(wait)

    raise RuntimeError(
        f"[{run_id}] Failed to fetch {endpoint} after {MAX_RETRIES} attempts"
    )


# ---------------------------------------------------------------------------
# Data Quality Checks
# ---------------------------------------------------------------------------
def run_dq_checks(
    table: str,
    records: list[dict],
    session_key: int,
    run_id: str,
    supabase: Client,
) -> bool:
    """
    Basic row count + null key checks before inserting.
    Logs to pipeline.dq_checks table.
    Returns True if all checks pass.
    """
    checks = []
    all_passed = True

    # Check 1: Non-empty
    checks.append({
        "run_id": run_id,
        "session_key": session_key,
        "table_name": f"raw.{table}",
        "check_name": "non_empty",
        "passed": len(records) > 0,
        "expected": "> 0",
        "actual": str(len(records)),
    })
    if len(records) == 0:
        all_passed = False

    # Check 2: No session_key mismatches
    if records:
        wrong_session = [
            r for r in records
            if r.get("session_key") and int(r["session_key"]) != session_key
        ]
        checks.append({
            "run_id": run_id,
            "session_key": session_key,
            "table_name": f"raw.{table}",
            "check_name": "session_key_consistency",
            "passed": len(wrong_session) == 0,
            "expected": "0 mismatches",
            "actual": str(len(wrong_session)),
        })
        if wrong_session:
            all_passed = False

    # Check 3: Row count sanity — flag if suspiciously large (potential loop/dup)
    MAX_EXPECTED = {
        "laps": 2500,           # 25 drivers x ~80 laps max
        "stints": 100,
        "pit": 100,
        "race_control": 500,
        "weather": 200,
        "session_results": 25,
        "starting_grid": 25,
        "drivers": 25,
        "overtakes": 500,
        "intervals": 500_000,   # high frequency
        "position": 500_000,
        "car_data": 2_000_000,  # before aggregation
        "team_radio": 500,
        "championship_drivers": 25,
        "championship_teams": 15,
    }
    max_exp = MAX_EXPECTED.get(table, 999_999)
    checks.append({
        "run_id": run_id,
        "session_key": session_key,
        "table_name": f"raw.{table}",
        "check_name": "row_count_sanity",
        "passed": len(records) <= max_exp,
        "expected": f"<= {max_exp}",
        "actual": str(len(records)),
    })
    if len(records) > max_exp:
        logger.warning(
            f"[{run_id}] DQ WARNING: {table} has {len(records)} rows "
            f"(expected <= {max_exp})"
        )

    # Insert check results
    try:
        supabase.schema("pipeline").table("dq_checks").insert(checks).execute()
    except Exception as e:
        logger.warning(f"[{run_id}] Could not write DQ checks: {e}")

    return all_passed


# ---------------------------------------------------------------------------
# Aggregation for high-frequency car_data using Polars
# ---------------------------------------------------------------------------
def aggregate_car_data(
    raw_records: list[dict],
    session_key: int,
    laps_data: list[dict],
) -> list[dict]:
    """
    OpenF1 car_data is ~3.7Hz = ~84k rows per driver per race.
    We use Polars to aggregate to per-lap metrics before DB insert.
    Full raw data goes to Supabase Storage (object store).

    Assigns each telemetry sample to a lap using lap date_start boundaries.
    """
    if not raw_records:
        return []

    logger.info(f"Aggregating {len(raw_records)} car_data rows with Polars...")

    # Build lap boundaries from laps data
    lap_boundaries = []
    sorted_laps = sorted(laps_data, key=lambda x: (x.get("driver_number", 0), x.get("lap_number", 0)))
    for lap in sorted_laps:
        if lap.get("date_start") and lap.get("driver_number") and lap.get("lap_number"):
            lap_boundaries.append({
                "driver_number": int(lap["driver_number"]),
                "lap_number": int(lap["lap_number"]),
                "lap_start_ts": lap["date_start"],
                "lap_duration": float(lap.get("lap_duration") or 0),
            })

    if not lap_boundaries:
        logger.warning("No lap boundaries available for car_data aggregation")
        return []

    # Load car_data into Polars
    df = pl.DataFrame(raw_records)

    # Ensure required columns exist
    required_cols = ["date", "driver_number", "speed", "throttle", "rpm", "brake", "drs"]
    for col in required_cols:
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).alias(col))

    df = df.with_columns([
        pl.col("date").cast(pl.Utf8).str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False),
        pl.col("driver_number").cast(pl.Int64),
        pl.col("speed").cast(pl.Float64),
        pl.col("throttle").cast(pl.Float64),
        pl.col("rpm").cast(pl.Float64),
        pl.col("brake").cast(pl.Float64),
        pl.col("drs").cast(pl.Int64),
    ])

    # Build lap boundaries DataFrame
    laps_df = pl.DataFrame(lap_boundaries).with_columns(
        pl.col("lap_start_ts").cast(pl.Utf8).str.to_datetime(
            format="%Y-%m-%dT%H:%M:%S%.f%z", strict=False
        )
    )

    # Join on driver + approximate lap time (range join)
    # For each car_data row, find which lap it belongs to
    results = []
    for driver_num in df["driver_number"].unique().to_list():
        driver_df = df.filter(pl.col("driver_number") == driver_num).sort("date")
        driver_laps = laps_df.filter(pl.col("driver_number") == driver_num).sort("lap_start_ts")

        if driver_laps.is_empty() or driver_df.is_empty():
            continue

        lap_starts = driver_laps["lap_start_ts"].to_list()
        lap_numbers = driver_laps["lap_number"].to_list()

        # Assign lap number to each telemetry row
        dates = driver_df["date"].to_list()
        lap_assignments = []
        for dt in dates:
            assigned = None
            for i, (ls, ln) in enumerate(zip(lap_starts, lap_numbers)):
                if dt >= ls:
                    # Check if before next lap start
                    if i + 1 < len(lap_starts):
                        if dt < lap_starts[i + 1]:
                            assigned = ln
                            break
                    else:
                        assigned = ln
                        break
            lap_assignments.append(assigned)

        driver_df = driver_df.with_columns(
            pl.Series("lap_number", lap_assignments)
        ).filter(pl.col("lap_number").is_not_null())

        if driver_df.is_empty():
            continue

        # Aggregate per lap
        agg = (
            driver_df
            .group_by("lap_number")
            .agg([
                pl.col("speed").mean().alias("avg_speed"),
                pl.col("speed").max().alias("max_speed"),
                pl.col("speed").min().alias("min_speed"),
                pl.col("throttle").mean().alias("avg_throttle"),
                pl.col("rpm").mean().alias("avg_rpm"),
                pl.col("rpm").max().alias("max_rpm"),
                (pl.col("brake") == 100).mean().alias("brake_pct"),
                pl.col("drs").is_in(list(DRS_ON_VALUES)).mean().alias("drs_pct"),
                pl.len().alias("sample_count"),
            ])
            .with_columns([
                pl.lit(session_key).alias("session_key"),
                pl.lit(driver_num).alias("driver_number"),
                (pl.col("brake_pct") * 100).round(2),
                (pl.col("drs_pct") * 100).round(2),
                pl.col("avg_speed").round(2),
                pl.col("avg_throttle").round(2),
                pl.col("avg_rpm").round(2),
            ])
        )
        results.extend(agg.to_dicts())

    logger.info(f"car_data aggregation complete: {len(results)} lap-level rows")
    return results


# ---------------------------------------------------------------------------
# Upload raw JSON to Supabase Storage
# ---------------------------------------------------------------------------
def upload_raw_to_storage(
    supabase: Client,
    data: list[dict],
    session_key: int,
    year: int,
    meeting_key: int,
    endpoint_name: str,
    run_id: str,
) -> str | None:
    """
    Upload raw JSON blob to Supabase Storage bucket 'raw-json'.
    Path: raw-json/{year}/{meeting_key}/{session_key}/{endpoint}.json
    Returns the storage path or None on failure.
    """
    if not data:
        return None

    bucket = "raw-json"
    path = f"{year}/{meeting_key}/{session_key}/{endpoint_name}.json"

    try:
        json_bytes = json.dumps(data, default=str).encode("utf-8")
        supabase.storage.from_(bucket).upload(
            path=path,
            file=json_bytes,
            file_options={"content-type": "application/json", "upsert": "true"},
        )
        logger.info(f"[{run_id}] Uploaded {endpoint_name} to storage: {path}")
        return path
    except Exception as e:
        logger.warning(f"[{run_id}] Storage upload failed for {endpoint_name}: {e}")
        return None


# ---------------------------------------------------------------------------
# Batch upsert helper
# ---------------------------------------------------------------------------
def batch_upsert(
    supabase: Client,
    schema: str,
    table: str,
    records: list[dict],
    run_id: str,
    batch_size: int = 500,
) -> int:
    """
    Insert records in batches. Uses upsert (on_conflict=ignore) for idempotency.
    Returns total rows inserted.
    """
    if not records:
        return 0

    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        try:
            (
                supabase
                .schema(schema)
                .table(table)
                .upsert(batch, on_conflict="ignore")
                .execute()
            )
            total += len(batch)
        except Exception as e:
            logger.error(f"[{run_id}] Batch insert failed for {schema}.{table}: {e}")
            raise
    return total


# ---------------------------------------------------------------------------
# Add audit columns to records
# ---------------------------------------------------------------------------
def add_audit_cols(
    records: list[dict],
    session_key: int,
    run_id: str,
) -> list[dict]:
    loaded_at = datetime.now(timezone.utc).isoformat()
    for r in records:
        r["_loaded_at"] = loaded_at
        r["_pipeline_run_id"] = run_id
        r["_source"] = "openf1"
        # Ensure session_key is always set
        if "session_key" not in r or r["session_key"] is None:
            r["session_key"] = session_key
    return records


# ---------------------------------------------------------------------------
# Main extraction function
# ---------------------------------------------------------------------------
def extract_session(
    session_key: int,
    meeting_key: int,
    year: int,
    run_id: str,
) -> dict[str, int]:
    """
    Pull all OpenF1 endpoints for a given session.
    Returns dict of {table: rows_inserted}.
    """
    supabase = get_supabase_client()
    rows_inserted: dict[str, int] = {}

    logger.info(f"[{run_id}] Starting extraction for session_key={session_key}")

    # Update pipeline status
    supabase.schema("pipeline").table("f1_sessions_schedule").update(
        {"status": "extracting", "extraction_started_at": datetime.now(timezone.utc).isoformat()}
    ).eq("session_key", session_key).execute()

    # -----------------------------------------------------------------------
    # Define all endpoints to extract
    # car_data is handled separately due to aggregation
    # -----------------------------------------------------------------------
    simple_endpoints = [
        ("drivers",             "drivers",              {"session_key": session_key}),
        ("laps",                "laps",                 {"session_key": session_key}),
        ("stints",              "stints",               {"session_key": session_key}),
        ("position",            "position",             {"session_key": session_key}),
        ("pit",                 "pit",                  {"session_key": session_key}),
        ("race_control",        "race_control",         {"session_key": session_key}),
        ("intervals",           "intervals",            {"session_key": session_key}),
        ("weather",             "weather",              {"session_key": session_key}),
        ("overtakes",           "overtakes",            {"session_key": session_key}),
        ("session_result",      "session_results",      {"session_key": session_key}),
        ("starting_grid",       "starting_grid",        {"session_key": session_key}),
        ("team_radio",          "team_radio",           {"session_key": session_key}),
        ("championship_drivers","championship_drivers", {"session_key": session_key}),
        ("championship_teams",  "championship_teams",   {"session_key": session_key}),
    ]

    # Cache laps for car_data aggregation
    laps_cache: list[dict] = []

    # -----------------------------------------------------------------------
    # Extract simple endpoints
    # -----------------------------------------------------------------------
    for api_endpoint, table_name, params in simple_endpoints:
        try:
            records = fetch_endpoint(api_endpoint, params, run_id)
            time.sleep(RATE_LIMIT_PAUSE)   # polite rate limiting

            # Upload raw to storage FIRST (before any transformation)
            upload_raw_to_storage(
                supabase, records, session_key, year,
                meeting_key, api_endpoint, run_id
            )

            # DQ checks
            run_dq_checks(table_name, records, session_key, run_id, supabase)

            # Add audit columns
            records = add_audit_cols(records, session_key, run_id)

            # Insert to DB
            n = batch_upsert(supabase, "raw", table_name, records, run_id)
            rows_inserted[table_name] = n
            logger.info(f"[{run_id}] {table_name}: {n} rows inserted")

            # Cache laps for later
            if table_name == "laps":
                laps_cache = records

        except Exception as e:
            logger.error(f"[{run_id}] Failed to extract {api_endpoint}: {e}")
            rows_inserted[table_name] = -1   # -1 = failed

    # -----------------------------------------------------------------------
    # Extract car_data (high frequency — aggregate with Polars)
    # -----------------------------------------------------------------------
    try:
        car_records = fetch_endpoint("car_data", {"session_key": session_key}, run_id)
        time.sleep(RATE_LIMIT_PAUSE)

        # Upload FULL raw telemetry to storage
        upload_raw_to_storage(
            supabase, car_records, session_key, year,
            meeting_key, "car_data_full", run_id
        )

        # Aggregate with Polars before DB insert
        agg_records = aggregate_car_data(car_records, session_key, laps_cache)
        agg_records = add_audit_cols(agg_records, session_key, run_id)

        n = batch_upsert(supabase, "raw", "car_data_lap_agg", agg_records, run_id)
        rows_inserted["car_data_lap_agg"] = n
        logger.info(f"[{run_id}] car_data_lap_agg: {n} rows (from {len(car_records)} telemetry samples)")

    except Exception as e:
        logger.error(f"[{run_id}] Failed to extract car_data: {e}")
        rows_inserted["car_data_lap_agg"] = -1

    # -----------------------------------------------------------------------
    # Update pipeline status
    # -----------------------------------------------------------------------
    failed_tables = [t for t, n in rows_inserted.items() if n == -1]
    final_status = "extracted" if not failed_tables else "failed"

    supabase.schema("pipeline").table("f1_sessions_schedule").update({
        "status": final_status,
        "extraction_ended_at": datetime.now(timezone.utc).isoformat(),
        "last_error": f"Failed tables: {failed_tables}" if failed_tables else None,
    }).eq("session_key", session_key).execute()

    if failed_tables:
        raise RuntimeError(
            f"[{run_id}] Extraction partially failed. Failed tables: {failed_tables}"
        )

    logger.info(f"[{run_id}] Extraction complete for session {session_key}. Summary: {rows_inserted}")
    return rows_inserted

if __name__ == "__main__":
    session_key = int(sys.argv[1])
    meeting_key = int(sys.argv[2])
    year = int(sys.argv[3])

    extract_session(session_key, meeting_key, year, "github_run")