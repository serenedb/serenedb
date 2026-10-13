with recursive

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
        unnest(row.split(NULL)) = '@' as has_roll,
),

current_layout(row_id, col_id, has_roll, current_rolls, i) using key (row_id, col_id) as (
        select row_id, col_id, has_roll, countif(has_roll) over (), 0 from layout
    union (
            with

            adjacent_rolls as (
                select
                    current_layout.row_id,
                    current_layout.col_id,
                    current_layout.has_roll,
                    current_layout.current_rolls,
                    current_layout.i,
                    coalesce(adjacent.has_roll, false) as adj_has_roll,
                from current_layout
                    cross join search_directions
                    left join current_layout as adjacent
                        on  current_layout.row_id + search_directions.row_i = adjacent.row_id
                        and current_layout.col_id + search_directions.col_i = adjacent.col_id
            )

            select
                row_id,
                col_id,
                (1=1
                    and any_value(has_roll)
                    and countif(adj_has_roll) >= 4
                ) as has_roll_,
                countif(has_roll_) over () as current_rolls_,
                any_value(i) + 1,
            from adjacent_rolls
            group by row_id, col_id
            qualify any_value(current_rolls) != current_rolls_
        )
)

from current_layout
select (select count(*) from layout where has_roll) - current_rolls
;
