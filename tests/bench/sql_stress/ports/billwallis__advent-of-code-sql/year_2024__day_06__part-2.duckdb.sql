/*
    Works for the sample, but not sure about the input yet -- ran for 2+ hours!
*/
create schema if not exists day_06;
use day_06;


create or replace table day_06.directions as
    from (
    values
        ('^', [ 0, -1], [ 1,  0]),
        ('>', [ 1,  0], [ 0,  1]),
        ('v', [ 0,  1], [-1,  0]),
        ('<', [-1,  0], [ 0, -1]),

    ) as v(symbol, direction, rotate_90)
;
-- from directions;


create or replace table day_06.grid as
    -- from read_csv('advent_of_code\solutions\year_2024\day_06\sample.data', header=false)
    from read_csv('advent_of_code/solutions/year_2024/day_06/sample.data', header=false)
    select
        generate_subscripts(split(column0, NULL), 1) AS x,
        row_number() over () as y,
        unnest(split(column0, NULL)) as cell,
;
-- from grid;


/* ~10s */
create or replace table day_06.original_journey as

/* While the guard is in the map, continue their journey */
with recursive journey as (
        select
            directions.direction,
            grid.x,
            grid.y,
            1 as step,
        from grid
            inner join directions
                on grid.cell = directions.symbol
    union all
        from (
            select
                journey.*,
                if(front.cell = '#', 'turn', 'move') as action,
                directions.rotate_90,
            from journey
                inner join directions
                    using (direction)
                inner join grid as front
                    on  journey.x + journey.direction[1] = front.x
                    and journey.y + journey.direction[2] = front.y
        )
        select
            if(action = 'turn', rotate_90, direction),
            if(action = 'turn', x, x + direction[1]) as x_,
            if(action = 'turn', y, y + direction[2]) as y_,
            step + 1,
        where (x_, y_) in (select (x, y) from grid)
)

from journey
;
-- from day_06.original_journey;


/* should be 4374 (41 in sample) */
from day_06.original_journey
select count(distinct (x, y))
;


/* ~1m*/
/*
    For each original step, re-calculate with an obstruction in front.

    However, one iteration for each step is _slow_.

    Instead, since we're travelling in a straight line, we can jump straight
    to the last unobstructed space.
*/
create or replace table day_06.simulated_journeys as

/* New */
select
    orig.step,
    orig.direction,
    orig.x,
    orig.y,
    orig.next_step,
    route.is_loop,
    route.seen::struct(direction int[], x int, y int)[] as seen,
from (
    from day_06.original_journey
    select
        *,
        {'x': x + direction[1], 'y': y + direction[2], cell: '#'} as next_step,
    where 1=1
        /* only where the next step is in the grid */
        and (next_step['x'], next_step['y']) in (select (x, y) from day_06.grid)
        /* we can skip the ones where the next step would already be an obstruction */
        and (next_step['x'], next_step['y'], next_step['cell']) not in (select (x, y, cell) from day_06.grid)
) as orig
    cross join lateral (
        with recursive

        /* Make the next step an obstruction */
        grid_adj as (
            from day_06.grid
            select * replace (
                if(
                    (x, y) = (orig.next_step['x'], orig.next_step['y']),
                    orig.next_step['cell'],
                    cell
                ) as cell
            )
        ),

        journey_adj as (
                /* Reference outer query to make this correlated */
                select
                    orig.direction,
                    orig.x,
                    orig.y,
                    [(orig.direction, orig.x, orig.y)] as seen,
                    false as is_loop,
            union all
                from (
                    from (
                        from journey_adj
                        select *, (
                            select [x, y]
                            from grid_adj
                            where 1=1
                                and cell = '#'
                                and case journey_adj.direction
                                    when [-1,  0] /* < */ then (grid_adj.y = journey_adj.y and grid_adj.x <= journey_adj.x)
                                    when [ 0, -1] /* ^ */ then (grid_adj.x = journey_adj.x and grid_adj.y <= journey_adj.y)
                                    when [ 1,  0] /* > */ then (grid_adj.y = journey_adj.y and grid_adj.x >= journey_adj.x)
                                    when [ 0,  1] /* v */ then (grid_adj.x = journey_adj.x and grid_adj.y >= journey_adj.y)
                                end
                            order by case journey_adj.direction
                                when [-1,  0] /* < */ then -grid_adj.x
                                when [ 0, -1] /* ^ */ then -grid_adj.y
                                when [ 1,  0] /* > */ then grid_adj.x
                                when [ 0,  1] /* v */ then grid_adj.y
                            end
                            limit 1
                        ) as next_obstruction,
                        where not journey_adj.is_loop
                    )
                    select
                        *,
                        [
                            next_obstruction[1] - direction[1],
                            next_obstruction[2] - direction[2],
                        ] as next_step,
                        if([x, y] = next_step, 'turn', 'move') as action,
                    where next_obstruction is not null  /* null => off the map, not a loop */
                ) inner join day_06.directions using (direction)
                select
                    rotate_90,
                    if(action = 'turn', x, next_step[1]) as x_,
                    if(action = 'turn', y, next_step[2]) as y_,
                    list_append(seen, (rotate_90, x_, y_)),
                    list_contains(seen, (rotate_90, x_, y_)),
        )

        select max(is_loop), max_by(seen, len(seen))
        from journey_adj
    ) as route(is_loop, seen)
order by orig.step
;
-- from day_06.simulated_journeys where is_loop
-- ;


/* should be 1705 (6 in sample) */
select
    count(distinct next_step),
    count(distinct (next_step['x'], next_step['y'])),
from day_06.simulated_journeys
where is_loop
;


/* Plot the routes (~15m) */
copy (
    from (
        from day_06.simulated_journeys
        where is_loop
    ) as scenarios
    select
        row_number() over (order by step) as loop_id,
        step,
        x,
        y,
        direction,
        (
            with

            grid_adj as (
                from (
                    select x, y, if(cell not in ('.', '#'), '.', cell) as cell
                    from day_06.grid
                ) as grid_
                select grid_.* replace (
                    case (grid_.x, grid_.y)
                        when (scenarios.next_step['x'], scenarios.next_step['y'])
                            then 'O'
                        when (scenarios.x, scenarios.y)
                            then (select symbol from day_06.directions where directions.direction = scenarios.direction)
                            else grid_.cell
                    end as cell
                )
            ),

            seen as (
                from (
                    select
                        unnest(scenarios.seen, recursive:=true),
                        generate_subscripts(scenarios.seen, 1) as i,
                ) inner join day_06.directions using (direction)
                select x, y, directions.symbol,
                qualify 1 = row_number() over (partition by x, y order by i desc)
            )

            from (
                select y, string_agg(coalesce(seen.symbol, grid_adj.cell), '' order by grid_adj.x) as graph
                from grid_adj
                    left join seen
                        using (x, y)
                group by grid_adj.y
            )
            select string_agg(graph, chr(10) order by y)
        ) as route
)
to '/dev/null' (format parquet)
;


/* All adjusted graphs (runs out of memory) */
-- copy (
--     from (
--         from 'advent_of_code/solutions/year_2024/day_06/routes/routes.parquet'
--         select step, x, y, direction, route.replace('.', ' ') as route
--     ) as orig
--     select
--         -- step,
--         -- x,
--         -- y,
--         (
--             with
--
--             graph as (
--                 from (
--                     select
--                         generate_subscripts(split(orig.route, chr(10)), 1) as y,
--                         unnest(split(orig.route, chr(10))) as row_part,
--                 )
--                 select
--                     y,
--                     generate_subscripts(split(row_part, NULL), 1) as x,
--                     unnest(split(row_part, NULL)) as cell,
--             ),
--
--             neighbours as (
--                 select
--                     graph.x,
--                     graph.y,
--                     graph.cell,
--                     coalesce(east.cell, '') as cell__east,
--                     coalesce(west.cell, '') as cell__west,
--                     coalesce(north.cell, '') as cell__north,
--                     coalesce(south.cell, '') as cell__south,
--                 from graph
--                     asof left join graph as east
--                         on  graph.y = east.y
--                         and graph.x > east.x
--                         and east.cell != ' '
--                     asof left join graph as west
--                         on  graph.y = west.y
--                         and graph.x < west.x
--                         and west.cell != ' '
--                     asof left join graph as north
--                         on  graph.x = north.x
--                         and graph.y > north.y
--                         and north.cell != ' '
--                     asof left join graph as south
--                         on  graph.x = south.x
--                         and graph.y < south.y
--                         and south.cell != ' '
--             ),
--
--             graph_adj as (
--                 select
--                     x,
--                     y,
--                     cell,
--                     (cell__east = '>' or cell__west = '<') as x_flag,
--                     (cell__north = 'v' or cell__south = '^') as y_flag,
--                     case
--                         when (orig.x, orig.y) = (x, y) then case orig.direction
--                             when [ 1,  0] then 'R'
--                             when [-1,  0] then 'L'
--                             when [ 0,  1] then 'D'
--                             when [ 0, -1] then 'U'
--                                           else 'X'
--                         end
--                         when cell in ('#', '>', '<', 'v', '^') then cell
--                         when x_flag and y_flag                 then '+'
--                         when x_flag                            then '-'
--                         when y_flag                            then '|'
--                         when (x, y) in (select (x, y) from day_06.original_journey)
--                                                                then '•'
--                                                                else cell
--                     end as cell_adj,
--                 from neighbours
--             )
--
--             from (
--                 select y, string_agg(cell_adj, '' order by x) as graph
--                 from graph_adj
--                 group by y
--             )
--             select string_agg(graph, chr(10) order by y)
--         ) as route_adj,
-- ) to 'advent_of_code/solutions/year_2024/day_06/routes-adj.parquet'
-- ;


/* Plot the original journey */
copy (
    from (
        from day_06.grid
        select
            x,
            y,
            coalesce(
                if(grid.cell in ('^', 'v', '<', '>'), 'S', null),
                (
                    select directions.symbol
                    from day_06.original_journey
                        inner join day_06.directions
                            using (direction)
                    where grid.x = original_journey.x
                      and grid.y = original_journey.y
                    order by original_journey.step desc
                    limit 1
                ),
                grid.cell
            ) as cell,
    )

    select string_agg(cell, '' order by x) as graph
    group by y
    order by y
) to '/dev/null' (format csv, header false, quote '')
;
