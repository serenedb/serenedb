with recursive

input as (
    from (
        select unnest(content.rtrim(e'\n').split(e'\n')).split(' ') as contents
        from read_text('advent_of_code/solutions/year_2025/day_10/sample.data')
    )
    select
        row_number() over () as machine_id,
        contents[1] as indicator_lights,
        contents[-1] as joltage_requirements,
        contents[2:-2] as button_wiring_schematics,
),

indicator_lights as (
    from (
        from input
        select
            machine_id,
            unnest(indicator_lights.split(NULL)) as char,
    )
    select
        machine_id,
        -1 + row_number() over (partition by machine_id) as light_id,
        char = '#' as is_on,
    where char in ('.', '#')
),
indicator_lights_lookup as (
    select
        machine_id,
        string_agg(is_on::int, '' order by light_id) as lights,
    from indicator_lights
    group by machine_id
),

button_wiring_schematics as (
    from (
        from input
        select
            machine_id,
            unnest(button_wiring_schematics) as button,
    )
    select
        machine_id,
        row_number() over (partition by machine_id) as button_id,
        button.substring(2, -2 + len(button)).split(',')::int[] as light_ids,
),

lights(machine_id, light_id, is_on, n, button_ids, lights) as (
        select machine_id, light_id, false, 0, '', ''
        from indicator_lights
    union all (
        with match_check as (
            select lights.*
            from lights
                left join indicator_lights_lookup
                    using (machine_id, lights)
            qualify 0 = count(indicator_lights_lookup.machine_id) over (
                partition by lights.machine_id
            )
        )

        from (
            select
                match_check.machine_id,
                match_check.light_id,
                if(
                    list_contains(buttons.light_ids, match_check.light_id),
                    not match_check.is_on,
                    match_check.is_on
                ) as is_on,
                match_check.n + 1 as n,
                match_check.button_ids || hex(buttons.button_id) as button_ids,
            from match_check
                inner join button_wiring_schematics as buttons
                    using (machine_id)
        )
        select
            *,
            string_agg(is_on::int, '' order by light_id) over (
                partition by machine_id, button_ids
            ) as lights,
    )
)

-- select *, count(indicator_lights_lookup.machine_id) over (
--                 partition by lights.machine_id
--             )
-- from lights
--     left join indicator_lights_lookup
--         using (machine_id, lights)
-- order by n desc, machine_id, button_ids, light_id
-- ;

-- select distinct machine_id
-- from (
--     from lights
--     qualify n = max(n) over (partition by machine_id)
-- )
--     inner join indicator_lights_lookup
--         using (machine_id, lights)
-- order by machine_id--, button_ids, light_id
-- ;

from (
    select machine_id, max(n) as min_presses
    from lights
    group by machine_id
    order by machine_id
)
select sum(min_presses)
;

-- 441 is too low
