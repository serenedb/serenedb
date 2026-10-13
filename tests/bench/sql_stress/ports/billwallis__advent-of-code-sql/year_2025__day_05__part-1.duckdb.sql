with

inventory as (
    from (
        select content.rtrim(e'\n') as content
        from read_text('advent_of_code/solutions/year_2025/day_05/sample.data')
    )
    select
        generate_subscripts(content.split(e'\n'), 1) as row_id,
        unnest(content.split(e'\n')) as row,
),

fresh_ranges as (
    from (
        from inventory
        qualify row_id < any_value(row_id) filter (where row = '') over ()
    )
    select
        row.split_part('-', 1)::bigint as lower_id,
        row.split_part('-', 2)::bigint as upper_id,
),

ingredient_ids as (
    select row::bigint as ingredient_id
    from inventory
    qualify row_id > any_value(row_id) filter (where row = '') over ()
)

select count(*) as fresh_count
from ingredient_ids
    semi join fresh_ranges
        on ingredient_ids.ingredient_id between fresh_ranges.lower_id
                                            and fresh_ranges.upper_id
;
