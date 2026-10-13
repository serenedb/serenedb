with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_20/sample.data', header=false)
),

grid as (
    from input
    select
        row_number() over () as x,
        generate_subscripts(split(data, NULL), 1) as y,
        unnest(split(data, NULL)) as cell,
),

directions(direction) as (
    values
        ([ 0, -1]),  /* ^ */
        ([ 1,  0]),  /* > */
        ([ 0,  1]),  /* v */
        ([-1,  0]),  /* < */
),

maze(x, y, ps, cell, seen) as (
        select x, y, 0, cell, [[x, y]]
        from grid
        where cell = 'S'
    union all
        from (
            select
                maze.x + direction[1] as x,
                maze.y + direction[2] as y,
                maze.ps + 1 as ps,
                maze.seen,
            from maze, directions
            where maze.cell != 'E'
        ) inner join grid using (x, y)
        select
            x,
            y,
            ps,
            grid.cell,
            seen.list_append([x, y])
        where 1=1
            and grid.cell != '#'
            and not seen.list_contains([x, y])
),

valid_solution as materialized (
    select x, y, ps
    from maze
),

cheats as (
    select distinct
        valid_solution.x,
        valid_solution.y,
        valid_solution.ps,
        [cheat.x, cheat.y] as jump_to,
        cheat.ps - cheat.ps_spent - valid_solution.ps as ps_saved,
    from valid_solution
        cross join lateral (
            select
                grid.x,
                grid.y,
                vs.ps,
                abs(sol.x - grid.x) + abs(sol.y - grid.y) as ps_spent,
            from (select valid_solution.x, valid_solution.y) as sol
                inner join grid
                    on  abs(sol.x - grid.x) + abs(sol.y - grid.y) <= 2
                    and grid.cell != '#'
                left join valid_solution as vs
                    on (grid.x, grid.y) = (vs.x, vs.y)
        ) as cheat
)

select count(*)
from cheats
where ps_saved >= 100
;
