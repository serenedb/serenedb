with

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_02/sample.data', header=false)
),

reports as (
    from (
        select
            row_number() over () as report_id,
            split(data, ' ') as report,
        from input
    )
    select
        report_id,
        unnest(report)::int as level,
        generate_subscripts(report, 1) AS index,
)

from (
    select
        report_id,
        level - lag(level) over (
            partition by report_id
            order by index
        ) as difference,
    from reports
)
select count(*) over ()
group by report_id
having 1=1
    and count(distinct sign(difference)) = 1  -- monotonic
    and max(abs(difference)) <= 3             -- bounded above
    and min(abs(difference)) >= 1             -- bounded below
;
