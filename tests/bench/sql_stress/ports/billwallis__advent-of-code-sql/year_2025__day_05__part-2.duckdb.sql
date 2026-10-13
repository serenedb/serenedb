with recursive

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
        row_number() over (order by lower_id, upper_id) as row_id,
),

merged_ranges(lower_id, upper_id, current_row) using key (lower_id) as (
        select lower_id, upper_id, row_id
        from fresh_ranges
        where row_id = 1
    union (
        with

        next_row as (
            from fresh_ranges
            where row_id = (select current_row + 1 from merged_ranges limit 1)
        ),

        overlapping_ranges as (
            select
                next_row.row_id as new_row_id,
                (1=1
                    and next_row.lower_id <= current_row.upper_id
                    and next_row.upper_id >= current_row.lower_id
                ) as has_overlap,
                /*
                    Since this input is sorted, current row's lower ID is always
                    no greater than the next row's lower ID
                */
                if(
                    has_overlap,
                    current_row.lower_id,
                    next_row.lower_id
                ) as new_lower_id,
                if(
                    has_overlap,
                    greatest(current_row.upper_id, next_row.upper_id),
                    next_row.upper_id
                ) as new_upper_id,
            from merged_ranges as current_row
                cross join next_row
        )

        from overlapping_ranges
        select
            new_lower_id,
            new_upper_id,
            new_row_id,
    )
)

select sum(1 + upper_id - lower_id) as fresh_ingredients
from merged_ranges
;
