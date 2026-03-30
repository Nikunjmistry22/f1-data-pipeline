-- models/staging/stg_drivers.sql
-- Cleans raw.drivers
-- One row per driver per session (team can change between sessions)
-- Adds change_hash for SCD Type 2 snapshot

with source as (
    select * from {{ source('raw', 'drivers') }}
),

sessions as (
    select
        session_key,
        season_year
    from {{ ref('stg_sessions') }}
),

staged as (
    select
        d.driver_number,
        d.session_key,
        d.meeting_key,
        trim(d.full_name)                               as driver_name,
        trim(d.broadcast_name)                          as broadcast_name,
        trim(d.name_acronym)                            as driver_code,
        trim(d.first_name)                              as first_name,
        trim(d.last_name)                               as last_name,
        trim(d.team_name)                               as team_name,

        -- Normalize team colour
        case
            when d.team_colour is null then null
            when left(d.team_colour, 1) = '#' then d.team_colour
            else '#' || d.team_colour
        end                                             as team_colour,

        trim(d.country_code)                            as country_code,
        d.headshot_url,
        s.season_year,

        d._loaded_at

    from source d
    left join sessions s
        on d.session_key = s.session_key

    where d.driver_number is not null
      and d.session_key   is not null
),

-- ✅ Deduplicate (important due to API duplicates)
deduped as (
    select *
    from (
        select *,
            row_number() over (
                partition by driver_number, session_key
                order by _loaded_at desc
            ) as rn
        from staged
    )
    where rn = 1
),

-- ✅ Add change hash (for snapshot detection)
final as (
    select
        *,
        md5(
            coalesce(driver_name, '') ||
            coalesce(driver_code, '') ||
            coalesce(team_name, '') ||
            coalesce(country_code, '')
        ) as change_hash
    from deduped
)

select * from final