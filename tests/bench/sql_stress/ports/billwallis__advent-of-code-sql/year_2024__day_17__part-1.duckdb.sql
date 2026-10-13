with recursive

input(data, row_id) as (
    select *, row_number() over ()
    from read_csv('advent_of_code/solutions/year_2024/day_17/sample-1.data', header=false, sep='')
),

registers as (
    select
        max(regexp_extract(data, 'Register A: (\d+)', 1))::int as register_a,
        max(regexp_extract(data, 'Register B: (\d+)', 1))::int as register_b,
        max(regexp_extract(data, 'Register C: (\d+)', 1))::int as register_c,
    from input
    where row_id < (select row_id from input where data is null)
),

program as (
    from (
        select
            split(data.replace('Program: ', ''), ',') as program,
            -1 + generate_subscripts(split(data.replace('Program: ', ''), ','), 1) as instruction_pointer,
            unnest(split(data.replace('Program: ', ''), ','))::int as opcode,
        from input
        where row_id > (select row_id from input where data is null)
    )
    select
        instruction_pointer,
        opcode,
        lead(opcode) over (order by instruction_pointer) as operand,
),

play as (
        select
            0 as i,
            0 as instruction_pointer,
            register_a,
            register_b,
            register_c,
            []::int[] as stdout,
            null::int as opcode,
            null::int as operand,
        from registers
    union all (
        with

        instructions as (
            select
                play.i + 1 as i,
                play.instruction_pointer,
                play.register_a,
                play.register_b,
                play.register_c,
                play.stdout,

                program.opcode,
                program.operand,

                case program.operand
                    when 0 then 0
                    when 1 then 1
                    when 2 then 2
                    when 3 then 3
                    when 4 then register_a
                    when 5 then register_b
                    when 6 then register_c
                    when 7 then 7  /* Should not appear */
                end as combo_operand,
                if(
                    combo_operand % 8 < 0,
                    8 + (combo_operand % 8),
                    combo_operand % 8
                ) as combo_mod_8,
            from play
                inner join program
                    using (instruction_pointer)
        ),

        execution as (
            from instructions
            select
                i,
                opcode,
                operand,

                case when opcode = 3 and register_a != 0
                    then operand
                    else instruction_pointer + 2
                end as instruction_pointer,

                case opcode
                    when 0 then floor(register_a / power(2, combo_operand))
                           else register_a
                end as register_a,

                case opcode
                    when 1 then xor(register_b::bitstring, operand::bitstring)::int
                    when 2 then combo_mod_8
                    when 4 then xor(register_b::bitstring, register_c::bitstring)::int
                    when 6 then floor(register_a / power(2, combo_operand))
                           else register_b
                end as register_b,

                case opcode
                    when 7 then floor(register_a / power(2, combo_operand))
                           else register_c
                end as register_c,

                case opcode
                    when 5 then stdout.list_append(combo_mod_8)
                           else stdout
                end as stdout,
        )

        from execution
        select
            i,
            instruction_pointer,
            register_a,
            register_b,
            register_c,
            stdout,
            opcode,
            operand,
    )
)

select list_string_agg(stdout)
from play
qualify i = max(i) over ()
;
