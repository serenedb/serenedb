with recursive

/*
    x and y are "typical" coordinates this time

    x increases to the right (>)
    y increases up (^)
*/

input as (
    from (
        from (
            select content.rtrim(e'\n') as content
            from read_text('advent_of_code/solutions/year_2025/day_12/sample.data')
        )
        select
            generate_subscripts(content.split(e'\n'), 1) as line_id,
            unnest(content.split(e'\n')) as line,
    )
    select
        *,
        sum(line = '') over (order by line_id) as shape_id,
        sum(line = '') over (order by line_id desc) as section_id,
),

/* Element-wise additive, not matrix multiplication */
rotation_matrix(x, y, dx, dy) as (
    values
        (1, 3,  2,  0),
        (1, 2,  1,  1),
        (1, 1,  0,  2),
        (2, 3,  1, -1),
        (2, 2,  0,  0),
        (2, 1, -1,  1),
        (3, 3,  0, -2),
        (3, 2, -1, -1),
        (3, 1, -2,  0),
),

shapes_without_rotations as (
    from (
        select
            shape_id,
            line,
            row_number() over (partition by shape_id order by line_id desc) as y,
        from input
        where 1=1
            and section_id != 0
            and line != ''
            and ':' not in line
    )
    select
        shape_id,
        generate_subscripts(line.split(e''), 1) as x,
        y,
        unnest(line.split(e'')) as char,
    order by
        shape_id,
        y desc,
        x
),
shapes(shape_id, rotation_id, x, y, char) as (
        select shape_id, 0, x, y, char
        from shapes_without_rotations
    union all
        select
            shapes.shape_id,
            shapes.rotation_id + 1,
            shapes.x + rotation_matrix.dx as x_,
            shapes.y + rotation_matrix.dy as y_,
            shapes.char,
        from shapes
            inner join rotation_matrix
                using (x, y)
        where shapes.rotation_id < 3
),

-- /* view shapes */
-- validate_shapes as (
--     pivot (
--         from (
--             from shapes
--             select
--                 shape_id,
--                 rotation_id,
--                 y,
--                 string_agg(char, '' order by x) as line,
--             group by all
--         )
--         select
--             shape_id,
--             rotation_id,
--             string_agg(line, e'\n' order by y desc) as shape,
--         group by all
--     )
--     on rotation_id
--     using any_value(shape)
--     group by shape_id
--     order by shape_id
-- )
-- from validate_shapes;

region_requirements as (
    from (
        select
            row_number() over (order by line_id) as region_id,
            line.split_part(': ', 1) as dimensions,
            line.split_part(': ', 2) as requirements,
        from input
        where section_id = 0
    )
    select
        region_id,
        dimensions.split_part('x', 1)::int as width,
        dimensions.split_part('x', 2)::int as length,
        -1 + generate_subscripts(requirements.split(' '), 1) as shape_id,
        unnest(requirements.split(' '))::int as required_number,
    order by
        region_id,
        shape_id
),

/* Try the first region */
region as (
    with

    grid as (
        select x.x, y.y
        from (
            select width, length
            from region_requirements
            where region_requirements.region_id = 1
            limit 1
        )
            cross join generate_series(1, width) as x(x)
            cross join generate_series(1, length) as y(y)
    )

    from grid
        /*
            Then lots of joins on shapes, trying each required shape's rotation
            in each space.

            e.g. for region 1:

                Join on every position that a single shape 4 can go in, of all rotations
                For each possibility, join another shape 4 (all rotations) into any remaining spaces
        */
    order by x, y desc
)

from region
;
