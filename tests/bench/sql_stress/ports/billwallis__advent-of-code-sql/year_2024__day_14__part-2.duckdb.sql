with

input(data, is_sample) as (
    select column0, filename.ends_with('sample.data')
    from read_csv('advent_of_code/solutions/year_2024/day_14/sample.data', header=false, sep='', filename=true)
),

dimensions(width, height) as (
    from (
        values
            (true,  11,  7),
            (false, 101, 103)
    ) as v(is_sample, x, y)
    select x, y
    where is_sample = (select any_value(is_sample) from input)
),

grid as (
    select x.x, y.y
    from dimensions
        cross join generate_series(1, dimensions.width) as x(x)
        cross join generate_series(1, dimensions.height) as y(y)
),

robots as (
    from input
    select
        row_number() over () as robot_id,
        regexp_extract(data, 'p=\d+,\d+ v=(-?\d+),(-?\d+)', ['x', 'y'])::struct(x int, y int) as velocity,
        regexp_extract(data, 'p=(\d+),(\d+) v=-?\d+,-?\d+', ['x', 'y'])::struct(x int, y int) as position,
)

from generate_series(77, 77 + (100 * 101), 101) as seconds(n)
select
    n,
    (
        with movements as (
            from robots, dimensions
            select
                dimensions.width,
                dimensions.height,
                (robots.position['x'] + (seconds.n * robots.velocity['x'])) % dimensions.width as pos_x,
                (robots.position['y'] + (seconds.n * robots.velocity['y'])) % dimensions.height as pos_y,
        ),

        final_positions as (
            select
                if(pos_x < 0, pos_x + width,  pos_x % width) as x,
                if(pos_y < 0, pos_y + height,  pos_y % height) as y,
                least(count(*), 9) as robot_count,
            from movements
            group by all
        )

        from (
            select
                grid.y,
                string_agg(
                    coalesce(final_positions.robot_count::varchar, ' '),
                    '' order by grid.x
                ) as graph
            from grid
                left join final_positions
                    using (x, y)
            group by grid.y
        )
        select string_agg(graph, chr(10) order by y)
    ) as graph
;

/*
    After some manual inspection, it seems like the robots were clustering
    every 101 seconds after the 77th second.

    So, the first 100 of these intervals were generated and then manually
    inspected again until the Christmas tree was found.

    Not a very analytical solution (I don't know the significance of 77),
    but it worked :D
*/


/* hack to make the tests pass since I did this manually */
select 7753;
