with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_08/sample.data', header=false)
),

/*
    x increments right (->)
    y increments down (v)
*/
grid as (
    select
        generate_subscripts(split(data, NULL), 1) AS x,
        row_number() over () as y,
        unnest(split(data, NULL)) as cell,
        [x, y] as coords,
    from input
),

antennas as (
    select
        l.*,
        l.x - r.x as x_diff,
        l.y - r.y as y_diff,
    from (from grid where cell != '.') as l
        inner join grid as r
            on  l.cell = r.cell  /* same antenna type */
            and l.coords != r.coords
),

/* For each pair, generate all antinodes */
antinodes as (
        select x, y, coords, x_diff, y_diff
        from antennas
    union all
        select
            x + x_diff as x_,
            y + y_diff as y_,
            [x_, y_] as coords_,
            x_diff,
            y_diff,
        from antinodes
        where coords_ in (select coords from grid)
)

select count(distinct coords)
from antinodes
;
