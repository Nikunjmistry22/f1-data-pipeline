-- models/intermediate/int_race_context.sql
-- Race outcome per driver per session — used by championship and race marts

with results as (
    select * from {{ ref('stg_session_results') }}
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

meetings as (
    select
        meeting_key,
        meeting_name,
        location
    from {{ ref('stg_meetings') }}
),

pit_summary as (
    select
        session_key,
        driver_number,
        count(*)                    as total_pit_stops,
        min(stop_duration_secs)     as fastest_stop_secs,
        sum(stop_duration_secs)     as total_stop_time_secs,
        sum(lane_duration_secs)     as total_pit_lane_time_secs
    from {{ ref('stg_pit') }}
    group by session_key, driver_number
),

enriched as (
    select
        r.session_key,
        r.meeting_key,

        -- Race context
        s.season_year,
        s.country_name,
        s.circuit_name,
        s.session_start_at          as race_date,
        m.meeting_name,
        m.location,

        -- Driver
        r.driver_number,
        d.driver_name,
        d.driver_code,
        d.team_name,
        d.team_colour,

        -- Race result
        r.finish_position,
        r.number_of_laps,
        r.gap_to_leader_secs,
        r.gap_to_leader_raw,
        r.race_duration_secs,
        r.is_winner,
        r.is_lapped,
        r.finished,
        r.dnf,
        r.dns,
        r.dsq,

        -- Pit stops
        coalesce(p.total_pit_stops, 0)      as total_pit_stops,
        p.fastest_stop_secs,
        p.total_stop_time_secs,
        p.total_pit_lane_time_secs

    from results r
    left join drivers  d on r.driver_number = d.driver_number
                         and r.session_key   = d.session_key
    left join sessions s on r.session_key   = s.session_key
    left join meetings m on r.meeting_key   = m.meeting_key
    left join pit_summary p on r.session_key   = p.session_key
                            and r.driver_number = p.driver_number
)

select * from enriched where total_pit_stops > 0 --Some sessions have no pit stop data, so filter those out for now