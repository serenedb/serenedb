with

input(data) as (
    select *, row_number() over () as row_id,
    from read_csv('advent_of_code/solutions/year_2025/day_01/sample.data', header=false)
),

rotations as (
    select
        row_id,
        left(data, 1) as direction,
        substring(data from 2)::int as clicks,
        case direction
            when 'L' then -clicks
            when 'R' then clicks
        end as movement,
    from input
),

dial_positions as (
    from (select 50 as movement, 0 as row_id union all select movement, row_id from rotations)
    select sum(movement) over (order by row_id) % 100 as dial_position
)

select count(*)
from dial_positions
where dial_position = 0
;
