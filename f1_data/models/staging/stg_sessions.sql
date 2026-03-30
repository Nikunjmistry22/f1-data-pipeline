-- models/staging/stg_sessions.sql
-- Cleans raw.sessions — only Race sessions matter downstream
-- but we keep all so joins don't break if session_key appears elsewhere

with source as (
    select * from {{ source('raw', 'sessions') }}
),

staged as (
    select
        session_key,
        meeting_key,
        year                            as season_year,
        trim(session_name)              as session_name,
        trim(session_type)              as session_type,
        trim(circuit_short_name)        as circuit_name,
        trim(country_name)              as country_name,
        trim(location)                  as location,
        date_start::timestamptz         as session_start_at,
        date_end::timestamptz           as session_end_at,
        gmt_offset,
        -- Flag for downstream filtering
        session_name = 'Race'           as is_race,
        _loaded_at

    from source
    where session_key is not null
)

select * from staged