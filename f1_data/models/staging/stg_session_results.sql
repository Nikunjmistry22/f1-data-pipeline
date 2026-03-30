-- models/staging/stg_session_results.sql

with source as (
    select * from {{ source('raw', 'session_results') }}
),

staged as (
    select
        session_key,
        meeting_key,
        driver_number,
        position                                        as finish_position,
        number_of_laps,
        -- gap_to_leader stored as TEXT in raw (can be "+1 LAP", "0", or numeric)
        gap_to_leader                                   as gap_to_leader_raw,
        -- Parse numeric gap; null for lapped cars or leader
        case
            when gap_to_leader = '0'        then 0.0
            when gap_to_leader like '+%LAP%' then null
            when gap_to_leader is null       then null
            else gap_to_leader::numeric
        end                                             as gap_to_leader_secs,
        -- Is this driver lapped?
        gap_to_leader like '+%LAP%'                    as is_lapped,
        -- Duration: race total time in seconds (numeric)
        case
            when duration is null then null
            else duration::text::numeric
        end                                             as race_duration_secs,
        coalesce(dnf, false)                            as dnf,
        coalesce(dns, false)                            as dns,
        coalesce(dsq, false)                            as dsq,
        -- Finished = completed the race
        not coalesce(dnf, false)
            and not coalesce(dns, false)
            and not coalesce(dsq, false)                as finished,
        -- Race winner
        position = 1
            and not coalesce(dnf, false)
            and not coalesce(dns, false)                as is_winner,
        _loaded_at

    from source
    where session_key   is not null
      and driver_number is not null
)

select * from staged