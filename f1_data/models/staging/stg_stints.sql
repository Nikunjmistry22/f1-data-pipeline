-- models/staging/stg_stints.sql

with source as (
    select * from {{ source('raw', 'stints') }}
),

staged as (
    select
        session_key,
        meeting_key,
        driver_number,
        stint_number,
        lap_start,
        lap_end,
        -- Normalise compound to uppercase
        upper(trim(compound))                           as compound,
        tyre_age_at_start,
        -- Derived stint length
        coalesce(lap_end, 0) - lap_start + 1           as stint_laps,
        -- Is this a fresh tyre?
        tyre_age_at_start = 0                           as is_fresh_tyre,
        _loaded_at

    from source
    where session_key   is not null
      and driver_number is not null
      and stint_number  is not null
)

select * from staged