-- models/staging/stg_meetings.sql
-- Cleans and types raw.meetings
-- One row per GP weekend

with source as (
    select * from {{ source('raw', 'meetings') }}
),

staged as (
    select
        meeting_key                                     as meeting_key,
        year                                            as season_year,
        trim(country_name)                              as country_name,
        trim(country_code)                              as country_code,
        trim(circuit_short_name)                        as circuit_name,
        trim(location)                                  as location,
        trim(meeting_name)                              as meeting_name,
        trim(meeting_official_name)                     as meeting_official_name,
        date_start::timestamptz                         as meeting_start_at,
        date_end::timestamptz                           as meeting_end_at,
        gmt_offset,
        _loaded_at

    from source
    where meeting_key is not null
)

select * from staged