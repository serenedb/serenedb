with recursive

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
),

combos(target, prev) as (
        select towel, ''
        from towels
    union all
        select distinct
            concat(combos.target, towels.towel) as target_,
            combos.target,
        from combos
            cross join towels
        where 1=1
            /* On track to be a `target` */
            and exists(
                from targets
                where targets.target.starts_with(target_)
            )
            /* Not an existing pattern */
            and not exists(
                from combos
                where target_ = combos.prev
            )
)

select count(*)
from targets
    semi join combos
        using (target)
;
