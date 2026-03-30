-- models/marts/strategy/fct_pit_strategy.sql
-- Tyre strategy + pit stop analysis per driver per race
-- Business metrics: number of stops, compounds used, stop timing, undercut windows

with stints as (
    select * from {{ ref('stg_stints') }}
),

pit as (
    select * from {{ ref('stg_pit') }}
),

drivers as (
    select driver_number, session_key, driver_name, driver_code, team_name
    from {{ ref('stg_drivers') }}
),

sessions as (
    select session_key, meeting_key, season_year, country_name, circuit_name
    from {{ ref('stg_sessions') }}
),

-- Tyre degradation: avg pace per tyre age (from lap times + stints)
tyre_deg as (
    select
        l.session_key,
        l.driver_number,
        l.stint_number,
        l.compound,
        l.lap_in_stint,
        l.lap_duration,
        -- Pace delta vs first lap of this stint (measures degradation)
        l.lap_duration - first_value(l.lap_duration) over (
            partition by l.session_key, l.driver_number, l.stint_number
            order by l.lap_number
            rows between unbounded preceding and current row
        )                                               as deg_from_stint_start_secs
    from {{ ref('int_laps_enriched') }} l
    where l.is_valid_lap
),

-- Stint summary per driver
stint_summary as (
    select
        s.session_key,
        s.driver_number,
        s.stint_number,
        s.compound,
        s.tyre_age_at_start,
        s.lap_start,
        s.lap_end,
        s.stint_laps,
        s.is_fresh_tyre,
        -- Average degradation rate (secs per lap) across the stint
        avg(td.deg_from_stint_start_secs)               as avg_deg_rate_secs_per_lap
    from stints s
    left join tyre_deg td
        on  s.session_key   = td.session_key
        and s.driver_number = td.driver_number
        and s.stint_number  = td.stint_number
    group by
        s.session_key, s.driver_number, s.stint_number,
        s.compound, s.tyre_age_at_start,
        s.lap_start, s.lap_end, s.stint_laps, s.is_fresh_tyre
),

-- Pit stop details
pit_detail as (
    select
        p.session_key,
        p.driver_number,
        p.lap_number                                    as pit_lap,
        p.stop_duration_secs,
        p.lane_duration_secs,
        p.is_fast_stop,
        p.pit_entry_at,
        -- Pit stop number for this driver
        row_number() over (
            partition by p.session_key, p.driver_number
            order by p.lap_number
        )                                               as stop_number
    from pit p
),

-- Compound sequence per driver (e.g. "SOFT → MEDIUM")
compound_sequence as (
    select
        session_key,
        driver_number,
        string_agg(compound, ' → ' order by stint_number)  as tyre_strategy
    from stints
    group by session_key, driver_number
),

final as (
    select
        ss.session_key,
        ss.driver_number,
        sess.meeting_key,
        sess.season_year,
        sess.country_name,
        sess.circuit_name,
        d.driver_name,
        d.driver_code,
        d.team_name,

        -- Stint detail
        ss.stint_number,
        ss.compound,
        ss.tyre_age_at_start,
        ss.lap_start,
        ss.lap_end,
        ss.stint_laps,
        ss.is_fresh_tyre,
        ss.avg_deg_rate_secs_per_lap,

        -- Overall strategy
        cs.tyre_strategy,

        -- Pit stop for this stint (the stop that ended the stint)
        pd.pit_lap,
        pd.stop_number,
        pd.stop_duration_secs,
        pd.lane_duration_secs,
        pd.is_fast_stop

    from stint_summary ss
    left join sessions sess         on ss.session_key   = sess.session_key
    left join drivers  d            on ss.driver_number = d.driver_number
                                    and ss.session_key   = d.session_key
    left join compound_sequence cs  on ss.session_key   = cs.session_key
                                    and ss.driver_number = cs.driver_number
    -- Join pit stop that starts this next stint (pit on lap = last lap of current stint)
    left join pit_detail pd         on ss.session_key   = pd.session_key
                                    and ss.driver_number = pd.driver_number
                                    and ss.lap_end       = pd.pit_lap
)

select * from final
order by season_year, meeting_key, driver_number, stint_number