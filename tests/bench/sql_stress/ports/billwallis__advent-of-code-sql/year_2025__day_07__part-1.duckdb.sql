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

flow(i, times_split, beam_cols) as (
        select row_id, 0, [col_id]
        from grid
        where row_id = 1 and item = 'S'
    union all (
        with next_splitters as (
            select col_id
            from grid
            where row_id = (select i + 1 from flow)
              and item = '^'
        )

        select
            flow.i + 1,
            flow.times_split + (
                select count(*)
                from next_splitters
                where contains(flow.beam_cols, next_splitters.col_id)
            ),
            (
                from (
                        select col_id - 1 as col_id
                        from next_splitters
                        where contains(flow.beam_cols, next_splitters.col_id)
                    union all
                        select col_id + 1
                        from next_splitters
                        where contains(flow.beam_cols, next_splitters.col_id)
                    union all
                        select col_id
                        from unnest(flow.beam_cols) as flow_unpacked(col_id)
                        where not exists(
                            from next_splitters
                            where flow_unpacked.col_id = next_splitters.col_id
                        )
                )
                select list(distinct col_id order by col_id)
            ),
        from flow
        where i < (select max(row_id) from grid)
    )
)

select times_split
from flow
qualify i = max(i) over ()
;
