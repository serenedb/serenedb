with recursive

input(data, row_id) as (
    select *, row_number() over ()
    from read_csv('advent_of_code/solutions/year_2024/day_17/sample-2.data', header=false, sep='')
),

/* Registers B and C start at 0, so can ignore parsing the register input */
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
        string_agg(opcode, ',') over () as target,
),

loop as (
        select
            power(8, 16)::bigint as register_a,
            (select target from program limit 1) as target,
            '' as stdout,
    union all (
        from loop
        select
            register_a + 1,
            target,
            (
                with recursive play as (
                        select
                            0 as i,
                            0 as instruction_pointer,
                            loop.register_a,
                            0::bigint as register_b,
                            0::bigint as register_c,
                            []::int[] as stdout,
                            null::int as opcode,
                            null::bigint as operand,
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
                                    when 1 then xor((register_b::bigint)::bitstring, (operand::bigint)::bitstring)::bigint
                                    when 2 then combo_mod_8
                                    when 4 then xor((register_b::bigint)::bitstring, (register_c::bigint)::bitstring)::bigint
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
            ) as stdout_,
        where target != stdout_
          /* toggle here */
          and register_a < power(8, 16) + 10
    )
)

select
    target,
    register_a,
    stdout,
    1 = row_number() over (partition by stdout order by register_a) as is_first
from loop
order by register_a
;

/* Pattern for sample-2.data */
from (
    select
        '0,3,5,4,3,0' as target,
        unnest(split('0,3,5,4,3,0', ','))::int as target_element,
        generate_subscripts(split('0,3,5,4,3,0', ','), 1) as exponent,
        power(8, exponent) as power_of_8,
)
select sum(target_element * power_of_8) as solution
;

-- 130721139626768 is too low
/* Pattern for input.data */
-- from (
--     select
--         '2,4,1,1,7,5,0,3,4,7,1,6,5,5,3,0' as target,
--         unnest(split(target, ','))::int as target_element,
--         generate_subscripts(split(target, ','), 1) as exponent,
--         power(8, exponent) as power_of_8,
-- )
-- select sum(target_element * power_of_8) as solution
-- ;
