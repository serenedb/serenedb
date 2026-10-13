with recursive

input(data, row_id) as (
    select *, row_number() over ()
    from read_csv('advent_of_code/solutions/year_2024/day_24/sample-2.data', header=false)
),

wires(wire_id, state) as (
    select split(data, ': ')[1], split(data, ': ')[2]::int
    from input
    where row_id < (select row_id from input where data is null)
),

gates as (
    from (
        select
            row_number() over (order by row_id) as gate_id,
            split(data, ' -> ')[1] as rule,
            split(data, ' -> ')[2] as target,
        from input
        where row_id > (select row_id from input where data is null)
    )
    select
        gate_id,
        target,
        split(rule, ' ')[1] as left_operand,
        split(rule, ' ')[2] as operation,
        split(rule, ' ')[3] as right_operand,
),

initial_states as (
        select wire_id, state
        from wires
    union
        select target, null
        from gates
),

apply(n, wire_id, state) as (
        select 0, wire_id, state
        from initial_states
    union all (
        with apply_rule as (
            from (
                select
                    operation,
                    target,
                    (select state from apply where apply.wire_id = gates.target) as target_state,
                    (select state from apply where apply.wire_id = gates.left_operand) as wire_l,
                    (select state from apply where apply.wire_id = gates.right_operand) as wire_r,
                from gates
            )
            select
                target as wire_id,
                case operation
                    when 'AND' then if(wire_l = 1 and wire_r = 1, 1, 0)
                    when 'OR'  then if(wire_l = 1 or  wire_r = 1, 1, 0)
                    when 'XOR' then if(wire_l != wire_r, 1, 0)
                end as state,
            where 1=1
                and target_state is null
                and wire_l is not null
                and wire_r is not null
            limit 1  /* We don't need a deterministic selection, so limit is fine */
        )

        select
            apply.n + 1 as n_,
            wire_id,
            coalesce(apply_rule.state, apply.state) as state_,
        from apply
            left join apply_rule
                using (wire_id)
        where n_ in (select gate_id from gates)
    )
),

z_wires(value) as (
    select string_agg(state, '' order by wire_id desc)
    from apply
    where 1=1
        and n = (select max(n) from apply)
        and wire_id.starts_with('z')
)

from z_wires
select (value::bitstring)::bigint
;
