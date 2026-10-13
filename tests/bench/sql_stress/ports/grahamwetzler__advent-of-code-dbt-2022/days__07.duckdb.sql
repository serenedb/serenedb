SET integer_division = true;

/*
  Day 7: No Space Left On Device
  https://adventofcode.com/2022/day/7
*/

with recursive input as (
  select row_number() over () as row_id
       , replace(line, '$ ', '') as line_cleaned
       , str_split(line_cleaned, ' ') as parts
       , parts[1] as left_part
       , parts[2] as right_part
    from read_csv_auto('input/07.csv', header=false) tbl(line)
   where line != '$ ls'
)

, traverse as (
  select 0 as cmd
       , []::text[] as path
       , null::int as size

   union all

  select cmd + 1 as cmd
       , case when left_part = 'cd' then
          case
            when right_part = '/' then ['/']
            when right_part = '..' then array_pop_back(path)
            else list_append(path, right_part)
          end
          else path
         end as path
       , case
          when left_part ~ '[0-9]+'
          then left_part::int
         end as size
    from traverse
    join input
      on cmd + 1 = row_id
)

, dirs as (
  select distinct list_string_agg(path) as dir
    from traverse
)

, path_sizes as (
  select dir
       , sum(size) as size
    from dirs
    join traverse
      on list_string_agg(path) ^@ dir
   group by 1
)

, unnested as (
  select unnest(path) as dir
       , size
    from traverse
)

, dir_sizes as (
  select dir
       , sum(size) as size
    from unnested
   group by 1
)

select 1 as part
     , sum(size) filter (size <= 100000) as answer
  from path_sizes
 union all
select 2 as part
     , min(size) filter (size >= (select sum(size) from traverse) - 40000000) as answer
  from dir_sizes