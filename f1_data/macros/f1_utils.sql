-- macros/f1_utils.sql
-- Reusable macros for F1 pipeline


-- ── f1_points(position) ──────────────────────────────────────────────────────
-- Returns championship points for a given finishing position
-- 2010-present scoring system
{% macro f1_points(position_col) %}
    case {{ position_col }}
        when 1  then 25
        when 2  then 18
        when 3  then 15
        when 4  then 12
        when 5  then 10
        when 6  then 8
        when 7  then 6
        when 8  then 4
        when 9  then 2
        when 10 then 1
        else 0
    end
{% endmacro %}


-- ── format_lap_time(seconds) ─────────────────────────────────────────────────
-- Converts seconds (e.g. 91.234) to MM:SS.mmm string (e.g. "1:31.234")
{% macro format_lap_time(secs_col) %}
    to_char(
        floor({{ secs_col }} / 60)::int, 'FM0'
    ) || ':'
    || to_char(
        floor(mod({{ secs_col }}, 60))::int, 'FM00'
    ) || '.'
    || to_char(
        round(({{ secs_col }} - floor({{ secs_col }})) * 1000)::int, 'FM000'
    )
{% endmacro %}


-- ── is_valid_lap(lap_duration, is_pit_out_lap) ───────────────────────────────
-- Standard valid lap filter used across all models
-- Excludes: null times, pit out laps
{% macro is_valid_lap(duration_col, pit_out_col) %}
    {{ duration_col }} is not null
    and {{ duration_col }} > 0
    and not coalesce({{ pit_out_col }}, false)
{% endmacro %}


-- ── compound_color(compound) ─────────────────────────────────────────────────
-- Returns Pirelli tyre hex colours for BI tools
{% macro compound_color(compound_col) %}
    case upper({{ compound_col }})
        when 'SOFT'         then '#FF3333'   -- red
        when 'MEDIUM'       then '#FFD700'   -- yellow
        when 'HARD'         then '#EEEEEE'   -- white/light grey
        when 'INTERMEDIATE' then '#39B54A'   -- green
        when 'WET'          then '#0067FF'   -- blue
        else '#AAAAAA'                        -- unknown
    end
{% endmacro %}


-- ── race_number(season_year, race_date) ──────────────────────────────────────
-- Race number within a season (round 1, 2, 3...)
{% macro race_number(year_col, date_col) %}
    rank() over (
        partition by {{ year_col }}
        order by {{ date_col }}
    )
{% endmacro %}


-- ── pct_off_pace(lap_time, reference_time) ───────────────────────────────────
-- % gap to a reference lap time
-- e.g. how far is this lap from the session fastest
{% macro pct_off_pace(lap_col, ref_col) %}
    round(
        ({{ lap_col }} - {{ ref_col }}) / {{ ref_col }} * 100
    , 3)
{% endmacro %}