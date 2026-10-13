with

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

antinodes as (
    select
        l.x - r.x as x_diff,
        l.y - r.y as y_diff,
        [l.x + x_diff, l.y + y_diff] as antinode_1,
        [r.x - x_diff, r.y - y_diff] as antinode_2,
    from (from grid where cell != '.') as l
        inner join grid as r
            on  l.cell = r.cell  /* same antenna type */
            and l.coords != r.coords
)

from (
          select antinode_1 from antinodes
    union select antinode_2 from antinodes
) as v(coords)
semi join grid using (coords)
select count(*)
;
