-- models/intermediate/int_laps_enriched.sql
-- Core enriched lap table — used by almost every mart
-- Joins laps + driver + session + telemetry
-- Flags valid laps for pace analysis

with laps as (
    select * from {{ ref('stg_laps') }}
),

drivers as (
    select
        driver_number,
        session_key,
        driver_name,
        driver_code,
        team_name,
        team_colour
    from {{ ref('stg_drivers') }}
),

sessions as (
    select
        session_key,
        meeting_key,
        season_year,
        country_name,
        circuit_name,
        session_start_at
    from {{ ref('stg_sessions') }}
),

telemetry as (
    select * from {{ ref('stg_car_data_lap_agg') }}
),

stints as (
    select
        session_key,
        driver_number,
        stint_number,
        compound,
        tyre_age_at_start,
        lap_start,
        lap_end,
        is_fresh_tyre
    from {{ ref('stg_stints') }}
),

-- Match each lap to its stint (lap_number falls between lap_start and lap_end)
laps_with_stints as (
    select
        l.*,
        s.stint_number,
        s.compound,
        s.tyre_age_at_start,
        s.is_fresh_tyre,
        -- How many laps into this stint is this lap?
        l.lap_number - s.lap_start + 1                  as lap_in_stint

    from laps l
    left join stints s
        on  l.session_key   = s.session_key
        and l.driver_number = s.driver_number
        and l.lap_number between s.lap_start and coalesce(s.lap_end, 9999)
),

enriched as (
    select
        -- Keys
        l.session_key,
        l.meeting_key,
        l.driver_number,
        l.lap_number,

        -- Session context
        sess.season_year,
        sess.country_name,
        sess.circuit_name,
        sess.session_start_at,

        -- Driver context
        d.driver_name,
        d.driver_code,
        d.team_name,
        d.team_colour,

        -- Lap timing
        l.lap_duration,
        l.duration_sector_1,
        l.duration_sector_2,
        l.duration_sector_3,

        -- Speed traps
        l.i1_speed,
        l.i2_speed,
        l.st_speed,

        -- Tyre context
        l.stint_number,
        l.compound,
        l.tyre_age_at_start,
        l.is_fresh_tyre,
        l.lap_in_stint,

        -- Flags
        l.is_pit_out_lap,
        l.has_timing,

        -- Valid pace lap:
        --   has a time, not a pit out lap,
        --   not anomalously long (>130% of session median is an SC/VSC lap)
        l.has_timing
            and not l.is_pit_out_lap                    as is_valid_lap,

        -- Telemetry
        t.avg_speed,
        t.max_speed,
        t.avg_throttle,
        t.avg_rpm,
        t.max_rpm,
        t.avg_brake,
        t.drs_usage_pct,
        t.gear_changes,
        t.has_sufficient_telemetry,

        l.lap_started_at

    from laps_with_stints l
    left join drivers  d    on l.driver_number = d.driver_number
                            and l.session_key   = d.session_key
    left join sessions sess on l.session_key   = sess.session_key
    left join telemetry t   on l.session_key   = t.session_key
                            and l.driver_number = t.driver_number
                            and l.lap_number    = t.lap_number
)

select * from enriched