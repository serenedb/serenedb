/*
    Works for the sample, but not sure about the input yet -- ran for 2+ hours!
*/
with

input(data, row_id) as (
    select *, row_number() over ()
    from read_csv('advent_of_code/solutions/year_2024/day_19/sample.data', header=false, sep='')
),

towels(towel) as (
    select unnest(split(data, ', '))
    from input
    where row_id < (select row_id from input where data is null)
),

targets(target) as (
    select data
    from input
    where row_id > (select row_id from input where data is null)
)

from (
    from targets
    select (
        with recursive combos(target, components) as (
                select towel, [towel]
                from towels
            union all
                select distinct
                    concat(combos.target, towels.towel) as target_,
                    combos.components.list_append(towels.towel) as components_,
                from combos, towels
                where starts_with(targets.target, target_)
        )

        select count(distinct components)
        from combos
        where targets.target = combos.target
    ) as number_of_options
)
select sum(number_of_options)
;
