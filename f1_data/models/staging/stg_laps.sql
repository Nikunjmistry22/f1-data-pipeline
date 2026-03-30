-- models/staging/stg_laps.sql
-- Cleans raw.laps — the core fact table
-- Filters out laps with no timing data
-- Flags pit-out laps (degraded times, excluded from pace analysis)

with source as (
    select * from {{ source('raw', 'laps') }}
),

staged as (
    select
        session_key,
        meeting_key,
        driver_number,
        lap_number,

        -- Lap timing
        lap_duration,
        duration_sector_1,
        duration_sector_2,
        duration_sector_3,

        -- Derived: theoretical best lap = sum of best individual sectors
        -- Computed in intermediate layer, not here

        -- Speed traps
        i1_speed,           -- intermediate 1 speed km/h
        i2_speed,           -- intermediate 2 speed km/h
        st_speed,           -- speed trap km/h (highest speed point on circuit)

        -- Flags
        coalesce(is_pit_out_lap, false)                 as is_pit_out_lap,

        -- Valid lap = has a time, not a pit out lap, not an in-lap
        -- In-laps don't have an is_pit_in_lap flag from API so we use
        -- lap_duration being unusually long as a proxy in intermediate layer
        lap_duration is not null                        as has_timing,

        date_start::timestamptz                         as lap_started_at,
        _loaded_at

    from source
    where session_key   is not null
      and driver_number is not null
      and lap_number    is not null
)

select * from staged