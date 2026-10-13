SET integer_division = true;

/*
  Day 6: Tuning Trouble
  https://adventofcode.com/2022/day/6
*/

with input as (
  select unnest(str_split(char, NULL)) as buffer
    from read_csv_auto('input/06.csv', header=false) as chars(char)
)

, row_id as (
  select row_number() over () as id
       , buffer
    from input
)

, markers as (
  select id
       , length(list_distinct(list(buffer) over (order by id rows between 3 preceding and current row))) as packet_marker
       , length(list_distinct(list(buffer) over (order by id rows between 13 preceding and current row))) as message_marker
    from row_id
   order by id
)

select 1 as part
     , min(id) as answer
  from markers
 where packet_marker = 4
 union all
select 2 as part
     , min(id) as answer
  from markers
 where message_marker = 14