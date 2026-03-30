-- models/marts/driver/fct_driver_pace.sql
-- Driver pace analysis + head-to-head teammate comparison per race
-- Business metrics: qualifying pace delta, race pace delta, consistency

with laps as (
    select * from {{ ref('int_laps_enriched') }}
    where is_valid_lap
),

-- Per driver per race: pace summary
driver_race_pace as (
    select
        session_key,
        meeting_key,
        season_year,
        country_name,
        circuit_name,
        driver_number,
        driver_name,
        driver_code,
        team_name,
        team_colour,

        count(*)                                        as total_valid_laps,
        min(lap_duration)                               as fastest_lap_secs,
        avg(lap_duration)                               as avg_lap_secs,
        -- Lap time consistency: lower stddev = more consistent
        stddev(lap_duration)                            as lap_time_stddev,
        -- Median is more robust than avg (less affected by SC laps)
        percentile_cont(0.5) within group (
            order by lap_duration
        )                                               as median_lap_secs,

        -- Sector bests
        min(duration_sector_1)                          as best_sector_1_secs,
        min(duration_sector_2)                          as best_sector_2_secs,
        min(duration_sector_3)                          as best_sector_3_secs,

        -- Theoretical best lap (sum of best individual sectors)
        min(duration_sector_1)
        + min(duration_sector_2)
        + min(duration_sector_3)                        as theoretical_best_lap_secs,

        -- Top speed (best speed trap across all laps)
        max(st_speed)                                   as top_speed_kmh,

        -- Telemetry averages (where available)
        avg(avg_throttle)                               as avg_throttle_pct,
        avg(drs_usage_pct)                              as avg_drs_usage_pct,
        avg(avg_brake)                                  as avg_brake_pct

    from laps
    group by
        session_key, meeting_key, season_year, country_name,
        circuit_name, driver_number, driver_name, driver_code,
        team_name, team_colour
),

-- Teammate comparison — join same team same race
teammate_compare as (
    select
        a.session_key,
        a.driver_number,
        b.driver_number                                 as teammate_driver_number,
        b.driver_code                                   as teammate_driver_code,
        -- Who was faster?
        a.fastest_lap_secs - b.fastest_lap_secs        as gap_to_teammate_fastest_secs,
        a.median_lap_secs  - b.median_lap_secs          as gap_to_teammate_median_secs,
        a.fastest_lap_secs < b.fastest_lap_secs         as beat_teammate_on_pace

    from driver_race_pace a
    join driver_race_pace b
        on  a.session_key = b.session_key
        and a.team_name   = b.team_name
        and a.driver_number != b.driver_number
        -- Avoid duplicates: only keep where a < b by driver_number
        and a.driver_number < b.driver_number
),

final as (
    select
        d.*,
        -- Gap to session fastest lap
        d.fastest_lap_secs - min(d.fastest_lap_secs) over (
            partition by d.session_key
        )                                               as gap_to_session_fastest_secs,

        -- Teammate metrics
        tc.teammate_driver_number,
        tc.teammate_driver_code,
        tc.gap_to_teammate_fastest_secs,
        tc.gap_to_teammate_median_secs,
        tc.beat_teammate_on_pace,

        -- Gap between theoretical best and actual fastest (execution delta)
        d.fastest_lap_secs - d.theoretical_best_lap_secs   as execution_delta_secs

    from driver_race_pace d
    left join teammate_compare tc
        on d.session_key   = tc.session_key
        and d.driver_number = tc.driver_number
)

select * from final
order by season_year, meeting_key, fastest_lap_secs