with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_06/sample.data', header=false)
),

/*
    x increments right (->)
    y increments down (v)
*/
grid as (
    from input
    select
        generate_subscripts(split(data, NULL), 1) as x,
        row_number() over () as y,
        unnest(split(data, NULL)) as cell,
),

directions(symbol, direction, rotate_90) as (
    values
        ('^', [ 0, -1], [ 1,  0]),
        ('>', [ 1,  0], [ 0,  1]),
        ('v', [ 0,  1], [-1,  0]),
        ('<', [-1,  0], [ 0, -1]),
),

/* While the guard is in the map, continue their journey */
journey(direction, x, y) as (
        select
            directions.direction,
            grid.x,
            grid.y,
        from grid
            inner join directions
                on grid.cell = directions.symbol
    union all
        from (
            select
                journey.*,
                directions.rotate_90,
                if(front.cell = '#', 'turn', 'move') as action,
            from journey
                inner join directions
                    using (direction)
                inner join grid as front
                    on  journey.x + journey.direction[1] = front.x
                    and journey.y + journey.direction[2] = front.y
        )
        select
            if(action = 'turn', rotate_90, direction),
            if(action = 'turn', x,         x + direction[1]) as x_,
            if(action = 'turn', y,         y + direction[2]) as y_,
        where (x_, y_) in (select (x, y) from grid)
)

select count(distinct (x, y))
from journey
;
