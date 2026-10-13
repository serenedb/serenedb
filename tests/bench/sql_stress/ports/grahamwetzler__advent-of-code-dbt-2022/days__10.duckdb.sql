SET integer_division = true;

/*
  Day 10: Cathode-Ray Tube
  https://adventofcode.com/2022/day/10
*/

with instructions as (
  select row_number() over () as step
       , str_split(ins, ' ')[1] as instruction
       , str_split(ins, ' ')[2]::int as value
    from read_csv_auto('input/10.csv', header=false) _(ins)
)

, addx as (
  select *
    from instructions
   cross join range(1, 3) _(instruction_step)
   where instruction = 'addx'
   order by step
)

, steps as (
    from addx
   union all
  select *
       , 1 as instruction_step
    from instructions
   where instruction = 'noop'
   order by step
       , instruction_step
)

, run as (
select *
     , row_number() over () as cycle
     , sum(case
            when instruction_step = 1 and step = 1 then 1
            when instruction = 'addx' and instruction_step % 2 = 0 then value
           end) over (order by step, instruction_step) as x
  from steps
 order by cycle
)

, part_1 as (
  select cycle
       , cycle * lag(x) over (order by cycle) as signal_strength
    from run
)

, rows as (
  select row_number() over (partition by cycle % 40 order by cycle) as row
       , *
    from run
)

, cols as (
  select row_number() over (partition by row order by cycle) - 1 as col
       , *
    from rows
)

, pixels as (
  select *
       , coalesce(lag(x) over (order by cycle), 0) as prev_x
       , case
           when col between prev_x - 1 and prev_x + 1
            then '##'
            else '  '
         end as pixel
    from cols
)

, pixel_string as (
  select row
       , string_agg(pixel, '') as pixel
    from pixels
   group by 1
)

select 1 as part
     , sum(signal_strength)::text as answer
  from part_1
 where list_contains(range(20, 221, 40), cycle)
 union all
select 2 as part
     , string_agg(pixel, chr(10)) as answer
  from pixel_string