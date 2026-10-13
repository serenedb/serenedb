with

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_01/sample.data', header=false)
),

ordered_input as (
    select
        data,
        split(data, ' ')[1]::int as left_list,
        split(data, ' ')[-1]::int as right_list,
        row_number() over (order by left_list) as left_rn,
        row_number() over (order by right_list) as right_rn,
    from input
)

select sum(abs(left_list.left_list - right_list.right_list))
from ordered_input as left_list
    inner join ordered_input as right_list
        on left_list.left_rn = right_list.right_rn
;
