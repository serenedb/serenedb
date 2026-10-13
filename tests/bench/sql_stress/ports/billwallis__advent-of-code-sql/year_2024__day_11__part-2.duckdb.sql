/*
    This doesn't work yet -- struggling to implement this in a "clean" way
*/


create or replace table mappings(
    stone int,
    maps_to int,
    in_steps int,
);
insert into mappings values (-1, -1, 1);
insert into mappings
    with

    exponential_bounds(l, n_from, n_to, match) as (
            select -1, -1::bigint, -1::bigint, true
        union all
            from generate_series(4, 20, 2) as v(n)
            select
                n - 3,
                ceil(pow(10, n) / 2024),
                pow(10, n - 3) - 1,
                true,
    ),

    maps as (
        from (
            from generate_series(0, 10000) as v(n)
            select
                n,

                printf('%d', n) as runes,
                len(runes) as l,
                case
                    when n = 0     then [1]
                    when l % 2 = 0 then [runes[:l / 2], runes[l / 2 + 1:]]::bigint[]
                end as maps_to_,

                case when maps_to_ is null then printf('%d', n * 2024) end as runes_2024,
                len(runes_2024) as l_2024,
                coalesce(
                    maps_to_,
                    [runes_2024[:l_2024 / 2], runes_2024[l_2024 / 2 + 1:]]::bigint[]
                ) as maps_to,

                if(maps_to_ is null, 2, 1) as in_steps,
        ) left join exponential_bounds using (l)
        select
            n,
            unnest(case when n between n_from and n_to
                then [-1]  -- infinity
                else maps_to
            end) as maps_to,
            in_steps,
    )

    from maps
    select
        n,
        maps_to,
        in_steps,
;


from mappings
order by stone, maps_to
;





with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_11/sample.data', header=false)
),

stones as (
    from input
    select unnest(split(data, ' '))::bigint as stone
),

blinks(blink, stone, in_steps) as (
        from stones
        select 0, stone, 0
    union all
        from blinks inner join mappings using (stone)
        select
            blinks.blink + mappings.in_steps as blink_,
            mappings.maps_to,
            mappings.in_steps,
        where blinks.blink < 5
)

-- from blinks
-- order by blink;

select sum(case
    when blink = (select max(blink) - 1 from blinks)
        then 1
    when stone = -1
        then 1
        else 0.5
end) as stones
from blinks
where blink >= (select max(blink) - 1 from blinks)
;





select 0;
