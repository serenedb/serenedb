with

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_01/sample.data', header=false)
),

split_input as (
    select
        data,
        split(data, ' ')[1]::int as left_list,
        split(data, ' ')[-1]::int as right_list,
    from input
),

right_list_frequency as (
    select
        right_list,
        count(*)::int as right_list_count
    from split_input
    group by right_list
)

select sum(
    split_input.left_list
    * coalesce(right_list_frequency.right_list_count, 0)
)
from split_input
    left join right_list_frequency
        on split_input.left_list = right_list_frequency.right_list
;
