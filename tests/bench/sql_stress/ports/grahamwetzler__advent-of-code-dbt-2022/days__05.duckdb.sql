SET integer_division = true;

/*
  Day 5: Supply Stacks
  https://adventofcode.com/2022/day/5
*/with recursive stacks as (
  select regexp_replace(columns(*), '[\[\]]', '', 'g') as stack
    from read_csv_auto('input/05-1.csv', delim=',', nullstr='   ', header=false)
)

, instructions as (
  select row_number() over () as step
       , string_split_regex(instruction[6:], '( from | to )')::int[] as instructions
       , instructions[1] as move_n
       , instructions[2] as move_from
       , instructions[3] as move_to
    from read_csv_auto('input/05-2.csv', header=false) tbl(instruction)
)

, stacks_str(stack_1, stack_2, stack_3, stack_4, stack_5, stack_6, stack_7, stack_8, stack_9) as (
  select trim(reverse(string_agg(columns(*), '')))
    from stacks
)

, stacks_list as (
  select [
           stack_1,
           stack_2,
           stack_3,
           stack_4,
           stack_5,
           stack_6,
           stack_7,
           stack_8,
           stack_9,] as stacks
    from stacks_str
)

, move(step, part_1, part_2) as (
  select 0 as step
       , stacks as part_1
       , stacks as part_2
    from stacks_list

   union all

  select r.step + 1 as step
       , [
           case
            when move_from = 1
              then part_1[1][:move_n * -1 - 1]
            when move_to = 1
              then part_1[1] || reverse(right(part_1[move_from], move_n))
              else part_1[1]
           end,
           case
            when move_from = 2
              then part_1[2][:move_n * -1 - 1]
            when move_to = 2
              then part_1[2] || reverse(right(part_1[move_from], move_n))
              else part_1[2]
           end,
           case
            when move_from = 3
              then part_1[3][:move_n * -1 - 1]
            when move_to = 3
              then part_1[3] || reverse(right(part_1[move_from], move_n))
              else part_1[3]
           end,
           case
            when move_from = 4
              then part_1[4][:move_n * -1 - 1]
            when move_to = 4
              then part_1[4] || reverse(right(part_1[move_from], move_n))
              else part_1[4]
           end,
           case
            when move_from = 5
              then part_1[5][:move_n * -1 - 1]
            when move_to = 5
              then part_1[5] || reverse(right(part_1[move_from], move_n))
              else part_1[5]
           end,
           case
            when move_from = 6
              then part_1[6][:move_n * -1 - 1]
            when move_to = 6
              then part_1[6] || reverse(right(part_1[move_from], move_n))
              else part_1[6]
           end,
           case
            when move_from = 7
              then part_1[7][:move_n * -1 - 1]
            when move_to = 7
              then part_1[7] || reverse(right(part_1[move_from], move_n))
              else part_1[7]
           end,
           case
            when move_from = 8
              then part_1[8][:move_n * -1 - 1]
            when move_to = 8
              then part_1[8] || reverse(right(part_1[move_from], move_n))
              else part_1[8]
           end,
           case
            when move_from = 9
              then part_1[9][:move_n * -1 - 1]
            when move_to = 9
              then part_1[9] || reverse(right(part_1[move_from], move_n))
              else part_1[9]
           end,
         ] as part_1
       , [
           case
            when move_from = 1
              then part_2[1][:move_n * -1 - 1]
            when move_to = 1
              then part_2[1] || right(part_2[move_from], move_n)
              else part_2[1]
           end,
           case
            when move_from = 2
              then part_2[2][:move_n * -1 - 1]
            when move_to = 2
              then part_2[2] || right(part_2[move_from], move_n)
              else part_2[2]
           end,
           case
            when move_from = 3
              then part_2[3][:move_n * -1 - 1]
            when move_to = 3
              then part_2[3] || right(part_2[move_from], move_n)
              else part_2[3]
           end,
           case
            when move_from = 4
              then part_2[4][:move_n * -1 - 1]
            when move_to = 4
              then part_2[4] || right(part_2[move_from], move_n)
              else part_2[4]
           end,
           case
            when move_from = 5
              then part_2[5][:move_n * -1 - 1]
            when move_to = 5
              then part_2[5] || right(part_2[move_from], move_n)
              else part_2[5]
           end,
           case
            when move_from = 6
              then part_2[6][:move_n * -1 - 1]
            when move_to = 6
              then part_2[6] || right(part_2[move_from], move_n)
              else part_2[6]
           end,
           case
            when move_from = 7
              then part_2[7][:move_n * -1 - 1]
            when move_to = 7
              then part_2[7] || right(part_2[move_from], move_n)
              else part_2[7]
           end,
           case
            when move_from = 8
              then part_2[8][:move_n * -1 - 1]
            when move_to = 8
              then part_2[8] || right(part_2[move_from], move_n)
              else part_2[8]
           end,
           case
            when move_from = 9
              then part_2[9][:move_n * -1 - 1]
            when move_to = 9
              then part_2[9] || right(part_2[move_from], move_n)
              else part_2[9]
           end,
         ] as part_2
    from move r
    join instructions i
      on i.step = r.step + 1
)

, answers as (
  select unnest(list_apply(part_1, x -> x[-1])) as part_1
       , unnest(list_apply(part_2, x -> x[-1])) as part_2
    from move
 qualify row_number() over (order by step desc) = 1
)

select 1 as part
     , string_agg(part_1, '') as answer
  from answers
 union all
select 2 as part
     , string_agg(part_2, '') as answer
  from answers