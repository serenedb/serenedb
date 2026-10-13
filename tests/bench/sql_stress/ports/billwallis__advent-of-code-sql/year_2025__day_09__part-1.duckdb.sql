with

input(p) as (
    from read_csv('advent_of_code/solutions/year_2025/day_09/sample.data', header=false) as i(x, y)
    select {x: x, y: y}
),

areas as (
    select
        l.p,
        r.p as q,
        (1
            * (1 + abs(l.p.x - r.p.x))
            * (1 + abs(l.p.y - r.p.y))
        ) as area,
    from input as l
        inner join input as r
            on l.p < r.p
)

select max(area)
from areas
;
