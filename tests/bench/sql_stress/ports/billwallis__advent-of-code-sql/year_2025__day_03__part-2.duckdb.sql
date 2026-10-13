with recursive

input(content) as (
    select content.rtrim(e'\n')
    from read_text('advent_of_code/solutions/year_2025/day_03/sample.data')
),

banks as (
    from (
        from input
        select
            generate_subscripts(content.split(e'\n'), 1) as bank_id,
            unnest(content.split(e'\n')) as bank,
    )
    select
        bank_id,
        generate_subscripts(bank.split(NULL), 1) as battery_id,
        unnest(bank.split(NULL)) as battery,
        len(bank) as bank_len,
),

joltage(bank_id, joltage, current_battery_id, i) as (
        select bank_id, battery, battery_id, 1
        from banks
    union all
        select
            joltage.bank_id,
            concat(joltage.joltage, banks.battery) as joltage_,
            banks.battery_id,
            joltage.i + 1,
        from joltage
            inner join banks
                on  joltage.bank_id = banks.bank_id
                and joltage.current_battery_id < banks.battery_id
                and banks.battery_id <= (banks.bank_len - (11 - joltage.i))
        where joltage.i < 12
        qualify 1 = row_number() over (
            partition by joltage.bank_id
            order by
                joltage_::bigint desc,
                battery_id
        )
),

max_joltage as (
    select bank_id, max(joltage::bigint) as max_joltage
    from joltage
    group by bank_id
)

select sum(max_joltage) as total_output_joltage
from max_joltage
;
