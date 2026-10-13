/*
    Works for the sample, but not sure about the input yet -- ran for 4+ hours!
*/

with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_16/sample-1.data', header=false)
),

grid as (
    from input
    select
        generate_subscripts(split(data, NULL), 1) as x,
        (row_number() over ()) as y,
        unnest(split(data, NULL)) as cell,
),

turns(direction, turn, turn_direction) as (
    values
        /* ^ */
        ([0, -1], 'f', [ 0, -1]),
        ([0, -1], 'l', [-1,  0]),
        ([0, -1], 'r', [ 1,  0]),

        /* > */
        ([1, 0], 'f', [1,  0]),
        ([1, 0], 'l', [0, -1]),
        ([1, 0], 'r', [0,  1]),

        /* v */
        ([0, 1], 'f', [ 0, 1]),
        ([0, 1], 'l', [ 1, 0]),
        ([0, 1], 'r', [-1, 0]),

        /* < */
        ([-1, 0], 'f', [-1,  0]),
        ([-1, 0], 'l', [ 0,  1]),
        ([-1, 0], 'r', [ 0, -1]),
),

maze(x, y, seen, direction, cost, is_finish) as (
        select
            x,
            y,
            [[x, y]],
            [1, 0],  /* start facing East */
            0,
            false,
        from grid
        where grid.cell = 'S'
    union all
        from (
            select
                maze.x + turns.turn_direction[1] as x_,
                maze.y + turns.turn_direction[2] as y_,
                maze.seen.list_append([x_, y_]) as seen,
                turns.turn_direction as direction,
                maze.cost + if(turns.turn = 'f', 1, 1001) as cost,
            from maze
                inner join turns
                    using (direction)
            where not maze.seen.list_contains([x_, y_])
        )
            inner join grid
                on  (x_, y_) = (grid.x, grid.y)
                and grid.cell != '#'
        select
            x_,
            y_,
            seen,
            direction,
            cost,
            grid.cell = 'E',
)

select min(cost)
from maze
where is_finish
;
