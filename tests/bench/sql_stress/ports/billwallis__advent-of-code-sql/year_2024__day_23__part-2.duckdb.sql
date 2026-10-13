with recursive

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

neighbours as (
    select
        computer_1 as computer,
        list(computer_2).list_append(computer_1).list_sort() as neighbours,
    from links
    group by computer_1
),

intersections(common) as (
    select list_intersect(n1.neighbours, n2.neighbours).list_sort()
    from neighbours as n1
        inner join neighbours as n2
            on n1.computer != n2.computer
)

select list_string_agg(common)
from intersections
group by common
having count(*) / 2 = (len(common) - 1) * len(common) / 2
order by len(common) desc
limit 1
;


/*
    All edges need the same `common` to be part of a party. Bidirectional
    edges means there are 2 rows per edge

    2 nodes: 1 edge   (1)          so 2 rows
    3 nodes: 3 edges  (2 + 1)      so 6 rows
    4 nodes: 6 edges  (3 + 2 + 1)  so 12 rows
    5 nodes: 10 edges (4 + ...)    so 20 rows
*/
