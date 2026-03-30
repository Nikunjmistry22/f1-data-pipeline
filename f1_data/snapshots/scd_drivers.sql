-- snapshots/scd_drivers.sql

{% snapshot scd_drivers %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'driver_number',
        strategy      = 'check',
        check_cols    = ['team_name'],
        invalidate_hard_deletes = false
    )
}}

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
    session_key        as latest_session_key,
    _loaded_at

from (
    select *,
        row_number() over (
            partition by driver_number
            order by session_key desc, _loaded_at desc
        ) as r
    from {{ ref('stg_drivers') }}
) t

where r = 1

{% endsnapshot %}