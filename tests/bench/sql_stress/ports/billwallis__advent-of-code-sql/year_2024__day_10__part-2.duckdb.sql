with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_10/sample.data', header=false)
),

grid as (
    select
        generate_subscripts(split(data, NULL), 1) AS x,
        row_number() over () as y,
        unnest(split(data, NULL))::int as height,
    from input
),

directions(direction) as (
    values
        ([ 0, -1]),
        ([ 1,  0]),
        ([ 0,  1]),
        ([-1,  0]),
),

hike(trailhead, x, y, height) as (
        select [x, y], x, y, height
        from grid
        where height = 0
    union all
        select
            hike.trailhead,
            grid.x,
            grid.y,
            grid.height,
        from hike
            cross join directions
            inner join grid
                on  hike.x + direction[1] = grid.x
                and hike.y + direction[2] = grid.y
                and hike.height + 1 = grid.height
)

from (
    select count(*) as score
    from hike
    where height = 9
    group by trailhead
)
select sum(score)
;
