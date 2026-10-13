with

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_23/sample.data', header=false)
),

/* Row for each direction to make it bidirectional */
links(computer_1, computer_2) as (
        select split(data, '-')[1], split(data, '-')[2]
        from input
    union
        select split(data, '-')[2], split(data, '-')[1]
        from input
),

trios as (
    select distinct
        list_sort([
            l1.computer_1,
            l1.computer_2,
            l2.computer_2,
        ]) as party,
    from links as l1
        inner join links as l2
            on l1.computer_2 = l2.computer_1
        inner join links as l3
            on  l2.computer_2 = l3.computer_1
            and l1.computer_1 = l3.computer_2
)

select count(*)
from trios
where 0=1
    or starts_with(party[1], 't')
    or starts_with(party[2], 't')
    or starts_with(party[3], 't')
;
