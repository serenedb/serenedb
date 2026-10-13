SET integer_division = true;

with input as (
  select *
       , row_number() over () as row_id
    from read_csv('input/01.csv', header=False, delim='', columns={'calories': 'int'})
)

, elves as (
  select *
       , calories is null as blank_line
       , row_number() over (partition by blank_line order by row_id) as blank_line_row_id
       , row_id - blank_line_row_id as elf
    from input
)

, calories_by_elf as (
  select elf
       , sum(calories) as calories
    from elves
   where not blank_line
   group by 1
)

, top_3_elves as (
  select calories
    from calories_by_elf
   order by calories desc
   limit 3
)

select 1 as part
     , max(calories) as answer
  from calories_by_elf
 union all
select 2 as part
     , sum(calories) as answer
  from top_3_elves