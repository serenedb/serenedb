with recursive

input(data, row_id, is_sample) as (
    select
        column0,
        row_number() over (),
        filename.ends_with('sample.data'),
    from read_csv('advent_of_code/solutions/year_2024/day_18/sample.data', header=false, sep='', filename=true)
),

dimensions(width, height) as (
    from (
        values
            (true,  6,  6),
            (false, 70, 70)
    ) as v(is_sample, x, y)
    select x, y
    where is_sample = (select any_value(is_sample) from input)
),

grid as (
    select x.x, y.y
    from dimensions
        cross join generate_series(0, dimensions.width) as x(x)
        cross join generate_series(0, dimensions.height) as y(y)
),

directions(symbol, direction) as (
    values
        ('^', [ 0, -1]),
        ('>', [ 1,  0]),
        ('v', [ 0,  1]),
        ('<', [-1,  0]),
),

bytes as (
    from input
    select
        split(data, ',')[1] as x,
        split(data, ',')[2] as y,
    where row_id <= if(is_sample, 12, 1024)
),

walk(step, x, y, seen) as (
        select 0, 0, 0, []::int[][]
    union all (
        from (
            select
                walk.step + 1 as step,
                walk.x + direction[1] as x,
                walk.y + direction[2] as y,
                walk.seen,
            from walk
                cross join directions
        ) as steps
            inner join grid using (x, y)
            anti join bytes using (x, y)
        select distinct
            steps.step,
            steps.x,
            steps.y,
            steps.seen.list_concat(list([x, y]) over ()).list_distinct() as seen_,
        where not steps.seen.list_contains([x, y])
    )
)

select step
from walk
where (x, y) = (select (width, height) from dimensions)
;
