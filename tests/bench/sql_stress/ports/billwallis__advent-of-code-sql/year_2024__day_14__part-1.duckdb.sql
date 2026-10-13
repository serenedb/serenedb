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

robots as (
    from input
    select
        row_number() over () as robot_id,
        regexp_extract(data, 'p=\d+,\d+ v=(-?\d+),(-?\d+)', ['x', 'y'])::struct(x int, y int) as velocity,
        regexp_extract(data, 'p=(\d+),(\d+) v=-?\d+,-?\d+', ['x', 'y'])::struct(x int, y int) as position,
),

movements as (
    from robots, dimensions
    select
        robot_id,
        {
            'x': ((position['x'] + (100 * velocity['x'])) % width),
            'y': ((position['y'] + (100 * velocity['y'])) % height),
        } as pos_,
        /* Re-adjust to be >= 0 */
        {
            'x': if(pos_['x'] < 0, pos_['x'] + width,  pos_['x']),
            'y': if(pos_['y'] < 0, pos_['y'] + height, pos_['y']),
        } as pos,

        floor(width / 2) as x_mid,
        floor(height / 2) as y_mid,
        case
            when pos['x'] < x_mid
                then case
                    when pos['y'] < y_mid then 1
                    when pos['y'] > y_mid then 2
                end
            when pos['x'] > x_mid
                then case
                    when pos['y'] < y_mid then 3
                    when pos['y'] > y_mid then 4
                end
        end as quadrant_id
)

from (
    select count(*) as robot_count
    from movements
    where quadrant_id is not null
    group by quadrant_id
)
select product(robot_count)
;

/*
    Let:

    - Px = position['x']  (initial)
    - Py = position['y']  (initial)
    - Vx = velocity['x']
    - Vy = velocity['y']
    - Sx = space width
    - Sy = space height
    - n  = number of seconds (100)

    A robot's position after `n` seconds is:

        x = Px + (n * Vx)  mod Sx
        y = Py + (n * Vy)  mod Sy
*/
