with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_11/sample.data', header=false)
),

stones as (
    from input
    select unnest(split(data, ' '))::bigint as stone
),

blinks(blink, stone) as (
        from stones
        select 0, stone
    union all
        from (
            from blinks
            select
                *,
                printf('%d', stone) as runes,
                len(runes) as l,
                case
                    when stone = 0 then [1]
                    when l % 2 = 0 then [runes[:l / 2], runes[l / 2 + 1:]]::bigint[]
                                   else [stone * 2024]
                end as stones_
        )
        select
            blink + 1 as blink_,
            unnest(stones_),
        where blink_ <= 25
)

select count(*)
from blinks
where blink = 25
;
