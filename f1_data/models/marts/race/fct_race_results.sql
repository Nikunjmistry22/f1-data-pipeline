-- models/marts/race/fct_race_results.sql
-- Final race standings with full context
-- One row per driver per race
-- Business metric: Who finished where, how far behind the winner, how many stops

with race_context as (
    select * from {{ ref('int_race_context') }}
),

-- Best lap per driver in the race
best_laps as (
    select
        session_key,
        driver_number,
        min(lap_duration)           as fastest_lap_secs,
        count(*) filter (
            where is_valid_lap
        )                           as valid_laps_count
    from {{ ref('int_laps_enriched') }}
    where is_valid_lap
    group by session_key, driver_number
),

-- Fastest lap in the whole race (the lap record holder)
race_fastest_lap as (
    select
        session_key,
        min(lap_duration)           as race_fastest_lap_secs
    from {{ ref('int_laps_enriched') }}
    where is_valid_lap
    group by session_key
),

final as (
    select
        rc.session_key,
        rc.meeting_key,
        rc.season_year,
        rc.country_name,
        rc.circuit_name,
        rc.race_date,
        rc.meeting_name,

        rc.driver_number,
        rc.driver_name,
        rc.driver_code,
        rc.team_name,
        rc.team_colour,

        -- Result
        rc.finish_position,
        rc.number_of_laps,
        rc.gap_to_leader_secs,
        rc.gap_to_leader_raw,
        rc.race_duration_secs,
        rc.is_winner,
        rc.is_lapped,
        rc.finished,
        rc.dnf,
        rc.dns,
        rc.dsq,

        -- Pit strategy
        rc.total_pit_stops,
        rc.fastest_stop_secs,
        rc.total_stop_time_secs,
        rc.total_pit_lane_time_secs,

        -- Lap time performance
        bl.fastest_lap_secs,
        rfl.race_fastest_lap_secs,
        -- Did this driver set the race fastest lap?
        bl.fastest_lap_secs = rfl.race_fastest_lap_secs    as set_fastest_lap,
        -- Gap to race fastest lap (how far off the pace)
        bl.fastest_lap_secs - rfl.race_fastest_lap_secs    as gap_to_fastest_lap_secs,
        bl.valid_laps_count

    from race_context rc
    left join best_laps bl
        on rc.session_key   = bl.session_key
        and rc.driver_number = bl.driver_number
    left join race_fastest_lap rfl
        on rc.session_key = rfl.session_key
)

select * from final
order by season_year, meeting_key, finish_position