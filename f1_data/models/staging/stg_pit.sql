-- models/staging/stg_pit.sql

with source as (
    select * from {{ source('raw', 'pit') }}
),

staged as (
    select
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        date::timestamptz                               as pit_entry_at,
        -- stop_duration = stationary time only (available 2024 US GP onwards)
        -- lane_duration = full pit lane time
        -- pit_duration  = deprecated alias for lane_duration
        coalesce(stop_duration, pit_duration)           as stop_duration_secs,
        coalesce(lane_duration, pit_duration)           as lane_duration_secs,
        -- Flag: under-cut risk window (stop < 2.5s is excellent)
        coalesce(stop_duration, pit_duration) < 2.5     as is_fast_stop,
        _loaded_at

    from source
    where session_key   is not null
      and driver_number is not null
)

select * from staged