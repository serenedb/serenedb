with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_07/sample.data', header=false)
),

equations as (
    from input
    select
        row_number() over () as equation_id,
        split(data, ': ') as _split,
        _split[1]::bigint as target,
        unnest(split(split(data, ': ')[-1], ' '))::bigint as number,
        generate_subscripts(split(split(data, ': ')[-1], ' '), 1) as index,
),

solutions(index, equation_id, target, total) as (
        select index, equation_id, target, number
        from equations
        where index = 1
    union all
        select
            equations.index,
            equations.equation_id,
            solutions.target,
            case ops.op
                when '+' then solutions.total + equations.number
                when '*' then solutions.total * equations.number
            end,
        from solutions
            cross join values ('+'), ('*') as ops(op)
            inner join equations
                on  solutions.equation_id = equations.equation_id
                and solutions.index + 1 = equations.index
)

from (
    select distinct equation_id, target
    from solutions
    qualify 1=1
        and index = max(index) over (partition by equation_id)
        and target = total
)
select sum(target)
;
