with

input(data) as (
    select string_agg(column0)
    from read_csv('advent_of_code/solutions/year_2024/day_03/sample-2.data', header=false, sep='')
),

expressions as (
    from input
    select
        regexp_replace(data, 'don''t\(\).*?(do\(\)|$)', '', 'g') as instructions,
        unnest(regexp_extract_all(
            regexp_replace(data, 'don''t\(\).*?(do\(\)|$)', '', 'g'),
            'mul\(\d{1,3},\d{1,3}\)'
        )) as expression
)

from expressions
select sum(1
    * regexp_replace(
        expression,
        'mul\((\d{1,3}),(\d{1,3})\)',
        '\1'
    )::int
    * regexp_replace(
        expression,
        'mul\((\d{1,3}),(\d{1,3})\)',
        '\2'
    )::int
)
;
