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
        clicks // 100 as additional_rotations,
    from input
),

dial_positions as (
    from (
            select 50 as movement, 0 as row_id
        union all by name
            from rotations
    )
    select
        *,
        sum(movement) over (order by row_id) % 100 as _dial_position,
        if(_dial_position < 0, _dial_position + 100, _dial_position) as dial_position,
),

previous_dial_positions as (
    from (
        select
            * exclude (movement),
            lag(dial_position) over (order by row_id) as prev_dial_position,
        from dial_positions
        qualify row_id != 0
    )
    select
        row_id,
        direction,
        clicks,
        prev_dial_position,
        dial_position,
        (1=1
            /* Don't double-count dial position cases */
            and prev_dial_position != 0
            and (0=1
                or dial_position = 0
                or case direction
                    when 'L' then prev_dial_position < dial_position
                    when 'R' then dial_position < prev_dial_position
                end
            )
        ) as encountered_zero,
        if(
            dial_position = 0 and prev_dial_position = 0,
            additional_rotations - 1,
            additional_rotations
        ) as additional_rotations,
)

select sum((encountered_zero)::int + additional_rotations) as score
from previous_dial_positions
;
