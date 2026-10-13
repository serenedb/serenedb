with

input(content) as (
    select content.rtrim(e'\n')
    from read_text('advent_of_code/solutions/year_2025/day_06/sample.data')
),

lines as (
    from (
        from input
        select
            generate_subscripts(content.split(e'\n'), 1) as line_id,
            unnest(content.split(e'\n')) as _line,
            _line.trim().regexp_replace('\s+', ' ', 'g') as line
    )
    select
        line_id,
        max(line_id) over () as max_line_id,
        generate_subscripts(line.split(' '), 1) as problem_id,
        unnest(line.split(' ')) as item,
),

problems as (
    select
        problem_id,
        any_value(item) filter (where line_id = max_line_id) as operator,
        list(item order by line_id) filter (where line_id != max_line_id) as items,
    from lines
    group by problem_id
),

solutions as (
    select
        problem_id,
        items::bigint[] as items_,
        case operator
            when '*' then list_product(items_)
            when '+' then list_sum(items_)
        end as solution
    from problems
)

select sum(solution)::bigint as grand_total
from solutions
;
