with

input(data, row_id) as (
    select *, row_number() over ()
    from read_csv('advent_of_code/solutions/year_2024/day_25/sample.data', header=false)
),

keys_and_locks as (
    from (
        from (
            from input
            select
                row_id,
                sum((data is null)::int) over (order by row_id rows unbounded preceding) as schematic_id,
                data as schematic,
        )
        select
            schematic_id,
            schematic,
            row_number() over (partition by schematic_id order by row_id) as row_id,
        where schematic is not null
    )
    select
        schematic_id,
        row_id,
        schematic,
        max(row_id = 1 and not schematic.contains('.')) over (partition by schematic_id) as is_lock,
),

schematics as (
    select
        schematic_id,
        is_lock,
        row_id,
        unnest(split(schematic, NULL)) as schematic_value,
        generate_subscripts(split(schematic, NULL), 1) as col_id,
    from keys_and_locks
    where schematic is not null
),

overlap as (
    select
        locks.schematic_id as lock_id,
        keys.schematic_id as key_id,
        row_id,
        col_id,
        (locks.schematic_value = '#' and keys.schematic_value = '#') as overlap,
    from schematics as locks
        left join schematics as keys
            using (row_id, col_id)
    where 1=1
        and locks.is_lock
        and not keys.is_lock
)

from (
    select
        lock_id,
        key_id,
        max(overlap) as overlap,
    from overlap
    group by all
)
select count(*)
where not overlap
;
