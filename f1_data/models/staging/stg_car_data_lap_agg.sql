-- models/staging/stg_car_data_lap_agg.sql

with source as (
    select * from {{ source('raw', 'car_data_lap_agg') }}
),

staged as (
    select
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        -- Speed metrics
        avg_speed,
        max_speed,
        min_speed,
        -- Engine metrics
        avg_throttle,
        avg_rpm,
        max_rpm,
        -- Driving style metrics
        avg_brake,          -- % of lap on brakes
        drs_usage_pct,      -- % of lap with DRS open
        gear_changes,       -- number of gear changes in lap
        sample_count,       -- telemetry samples in this lap (data quality indicator)
        -- Flag: sufficient telemetry data for this lap
        sample_count >= 50                              as has_sufficient_telemetry,
        _loaded_at

    from source
    where session_key   is not null
      and driver_number is not null
      and lap_number    is not null
)

select * from staged