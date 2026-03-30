-- models/marts/championship/fct_driver_season.sql
-- Driver championship standings after each race round
-- Business metrics: points trajectory, position changes, wins/podiums per season

with race_results as (
    select * from {{ ref('fct_race_results') }}
),

-- F1 points system 2010-present
-- 1st=25, 2nd=18, 3rd=15, 4th=12, 5th=10, 6th=8, 7th=6, 8th=4, 9th=2, 10th=1
-- +1 for fastest lap if finishing in top 10
points_map as (
    select * from (values
        (1, 25), (2, 18), (3, 15), (4, 12), (5, 10),
        (6, 8),  (7, 6),  (8, 4),  (9, 2),  (10, 1)
    ) as t(position, points)
),

-- Points per race per driver
race_points as (
    select
        r.season_year,
        r.session_key,
        r.meeting_key,
        r.country_name,
        r.race_date,
        r.driver_number,
        r.driver_name,
        r.driver_code,
        r.team_name,
        r.finish_position,
        r.is_winner,
        r.finished,
        r.dnf,
        r.set_fastest_lap,

        -- Base points
        coalesce(pm.points, 0)                          as base_points,

        -- Fastest lap bonus (+1 if set fastest lap AND finished in top 10)
        case
            when r.set_fastest_lap
                 and r.finished
                 and r.finish_position <= 10
            then 1 else 0
        end                                             as fastest_lap_bonus,

        -- Total points this race
        coalesce(pm.points, 0) + case
            when r.set_fastest_lap
                 and r.finished
                 and r.finish_position <= 10
            then 1 else 0
        end                                             as race_points

    from race_results r
    left join points_map pm on r.finish_position = pm.position
    where r.finished or r.finish_position is not null
),

-- Cumulative points across the season
cumulative as (
    select
        rp.*,
        -- Cumulative points up to and including this race
        sum(race_points) over (
            partition by season_year, driver_number
            order by race_date
            rows between unbounded preceding and current row
        )                                               as cumulative_points,

        -- Cumulative wins
        sum(case when is_winner then 1 else 0 end) over (
            partition by season_year, driver_number
            order by race_date
            rows between unbounded preceding and current row
        )                                               as cumulative_wins,

        -- Cumulative podiums (top 3)
        sum(case when finish_position <= 3
                  and finished then 1 else 0 end) over (
            partition by season_year, driver_number
            order by race_date
            rows between unbounded preceding and current row
        )                                               as cumulative_podiums,

        -- Cumulative DNFs
        sum(case when dnf then 1 else 0 end) over (
            partition by season_year, driver_number
            order by race_date
            rows between unbounded preceding and current row
        )                                               as cumulative_dnfs,

        -- Race number in the season
        row_number() over (
            partition by season_year, driver_number
            order by race_date
        )                                               as race_number_in_season

    from race_points rp
),

-- Championship position after each race
with_position as (
    select
        c.*,
        rank() over (
            partition by season_year, session_key
            order by cumulative_points desc
        )                                               as championship_position

    from cumulative c
)

select * from with_position
order by season_year, race_date, championship_position