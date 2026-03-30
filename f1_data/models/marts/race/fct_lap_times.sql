-- models/marts/race/fct_lap_times.sql
-- Every valid lap in every race with full context
-- Business metrics: lap time progression, sector analysis, tyre degradation

with laps as (
    select * from {{ ref('int_laps_enriched') }}
),

-- Session-level stats for relative comparisons
session_stats as (
    select
        session_key,
        percentile_cont(0.5) within group (
            order by lap_duration
        )                                               as median_lap_secs,
        min(lap_duration) filter (
            where is_valid_lap
        )                                               as session_fastest_lap_secs
    from laps
    where has_timing
    group by session_key
),

-- Driver-level fastest lap for teammate comparisons
driver_best as (
    select
        session_key,
        driver_number,
        min(lap_duration) filter (
            where is_valid_lap
        )                                               as driver_fastest_lap_secs,
        avg(lap_duration) filter (
            where is_valid_lap and not is_pit_out_lap
        )                                               as driver_avg_lap_secs
    from laps
    group by session_key, driver_number
),

final as (
    select
        l.session_key,
        l.meeting_key,
        l.season_year,
        l.country_name,
        l.circuit_name,
        l.driver_number,
        l.driver_name,
        l.driver_code,
        l.team_name,
        l.lap_number,

        -- Lap timing
        l.lap_duration                                  as lap_time_secs,
        l.duration_sector_1                             as sector_1_secs,
        l.duration_sector_2                             as sector_2_secs,
        l.duration_sector_3                             as sector_3_secs,

        -- Speed traps
        l.i1_speed,
        l.i2_speed,
        l.st_speed,

        -- Tyre
        l.compound,
        l.tyre_age_at_start,
        l.lap_in_stint,
        l.stint_number,
        l.is_fresh_tyre,

        -- Flags
        l.is_valid_lap,
        l.is_pit_out_lap,

        -- Relative pace metrics
        ss.session_fastest_lap_secs,
        ss.median_lap_secs,
        db.driver_fastest_lap_secs,

        -- Gap to session fastest (pace delta)
        l.lap_duration - ss.session_fastest_lap_secs   as gap_to_fastest_secs,
        -- Gap as % (normalised pace)
        round(
            (l.lap_duration - ss.session_fastest_lap_secs)
            / ss.session_fastest_lap_secs * 100
        , 3)                                            as pct_off_fastest,

        -- Telemetry
        l.avg_speed,
        l.max_speed,
        l.avg_throttle,
        l.avg_rpm,
        l.avg_brake,
        l.drs_usage_pct,
        l.gear_changes,

        l.lap_started_at

    from laps l
    left join session_stats ss on l.session_key = ss.session_key
    left join driver_best   db on l.session_key   = db.session_key
                               and l.driver_number = db.driver_number

    where l.has_timing   -- only laps with timing data
)

select * from final
order by season_year, meeting_key, driver_number, lap_number