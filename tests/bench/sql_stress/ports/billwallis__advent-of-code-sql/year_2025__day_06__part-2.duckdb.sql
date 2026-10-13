with recursive

input(content) as (
    select content.rtrim(e'\n').replace(' ', ' ')
    from read_text('advent_of_code/solutions/year_2025/day_06/sample.data')
),

lines as (
    from (
        from input
        select
            generate_subscripts(content.split(e'\n'), 1) as line_id,
            unnest(content.split(e'\n')) as line,
    )
    select
        line_id,
        max(line_id) over () as max_line_id,
        generate_subscripts(line.split(NULL), 1) as char_id,
        unnest(line.split(NULL)) as char,
),

problem_ids as (
    select
        line_id,
        char_id,
        sum(char != ' ') over by_char_id as problem_id,
        last_value(char.nullif(' ') ignore nulls) over by_char_id as operator,
    from lines
    where line_id = max_line_id
    window by_char_id as (order by char_id)
),

problem_metadata as (
    select
        problem_ids.problem_id,
        problem_ids.operator,
        lines.line_id,
        /* Reset the character ID for each problem */
        row_number() over (
            partition by problem_ids.problem_id, lines.line_id
            order by lines.char_id
        ) as char_id,
        lines.char,
    from lines
        inner join problem_ids
            using (char_id)
    where lines.line_id != lines.max_line_id
),

problems as (
    select
        problem_id,
        any_value(operator) as operator,
        string_agg(char, '' order by line_id).trim() as item
    from problem_metadata
    group by problem_id, char_id
    having item != ''
)

from (
    select
        problem_id,
        case any_value(operator)
            when '*' then product(item::bigint)
            when '+' then sum(item::bigint)
        end as solution
    from problems
    group by problem_id
)
select sum(solution)::bigint as grand_total
;
