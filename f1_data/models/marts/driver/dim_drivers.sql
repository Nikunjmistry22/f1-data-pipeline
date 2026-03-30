-- models/marts/driver/dim_drivers.sql
-- Current driver dimension — built from the SCD Type 2 snapshot
-- Use this for any "current team" lookups
-- Use scd_drivers directly for historical "what team was X driving for at race Y"

with snapshot as (
    select * from {{ ref('scd_drivers') }}
),

current_drivers as (
    select
        driver_number,
        driver_name,
        driver_code,
        broadcast_name,
        first_name,
        last_name,
        team_name,
        team_colour,
        country_code,
        headshot_url,
        latest_session_key,
        dbt_valid_from                  as active_since,
        dbt_valid_to                    as active_until,
        case 
            when dbt_valid_to is null then true 
            else false 
        end as is_current_record

    from snapshot
)

select * from current_drivers
order by driver_number