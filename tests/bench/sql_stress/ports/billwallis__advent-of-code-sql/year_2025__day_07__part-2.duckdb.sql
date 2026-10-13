with recursive

input(content) as (
    select content.rtrim(e'\n')
    from read_text('advent_of_code/solutions/year_2025/day_07/sample.data')
),

grid as (
    from (
        from input
        select
            generate_subscripts(content.split(e'\n'), 1) as row_id,
            unnest(content.split(e'\n')) as row,
    )
    select
        row_id,
        generate_subscripts(row.split(NULL), 1) as col_id,
        unnest(row.split(NULL)) as item,
),

flow(col_id, row_id, timelines) as (
        select col_id, row_id, (item = 'S')::bigint
        from grid
        where row_id = 1
    union all (
        select
            flow.col_id + coalesce(v.x, 0) as col_id_,
            any_value(flow.row_id) + 1,
            sum(flow.timelines),
        from flow
            left join grid
                on  flow.col_id = grid.col_id
                and flow.row_id + 1 = grid.row_id
            left join (values (-1), (1)) as v(x)
                on grid.item = '^'
        where flow.row_id < (select max(row_id) from grid)
        group by col_id_
    )
)

select sum(timelines)
from flow
where row_id = (select max(row_id) from flow)
;
