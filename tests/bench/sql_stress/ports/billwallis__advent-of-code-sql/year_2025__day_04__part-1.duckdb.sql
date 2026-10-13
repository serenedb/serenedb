with

input(content) as (
    select content.rtrim(e'\n')
    from read_text('advent_of_code/solutions/year_2025/day_04/sample.data')
),

/*
    Columns increment to the right (->)
    Rows increment down (v)
*/
search_directions(direction, col_i, row_i) as (
    values
        ('east',        1,  0),
        ('south-east',  1,  1),
        ('south',       0,  1),
        ('south-west', -1,  1),
        ('west',       -1,  0),
        ('north-west', -1, -1),
        ('north',       0, -1),
        ('north-east',  1, -1),
),

layout as (
    from (
        from input
        select
            generate_subscripts(content.split(e'\n'), 1) as row_id,
            unnest(content.split(e'\n')) as row,
    )
    select
        row_id,
        generate_subscripts(row.split(NULL), 1) as col_id,
        unnest(row.split(NULL)) as content,
        content = '@' as has_roll,
),

adjacent_rolls as (
    select
        layout.row_id,
        layout.col_id,
        layout.has_roll,
        search_directions.*,
        coalesce(adjacent.has_roll, false) as adj_has_roll,
    from layout
        cross join search_directions
        left join layout as adjacent
            on  layout.row_id + search_directions.row_i = adjacent.row_id
            and layout.col_id + search_directions.col_i = adjacent.col_id
)

from (
    select countif(adj_has_roll) as accessible_rolls
    from adjacent_rolls
    where has_roll
    group by row_id, col_id
    having accessible_rolls < 4
)
select count(*)
;
