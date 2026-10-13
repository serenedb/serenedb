SET integer_division = true;
SET scalar_subquery_error_on_multiple_rows = false;

/*
  Day 8: Treetop Tree House
  https://adventofcode.com/2022/day/8
*/

with grid as (
  select row_number() over () as row
       , str_split(row, NULL)::int[] as trees
    from read_csv('input/08.csv', columns={'row': 'text'}, header=false)
)

, unnested as (
  select row
       , unnest(trees) as tree
    from grid
)

, trees as (
  select row
       , row_number() over (partition by row order by row) as col
       , tree
    from unnested
)

, part_1 as (
  select case
          when t.row = 1 or t.col = 1 then 1 /* edge */
          when t.row = max(t.row) over () or t.col = max(t.col) over () then 1 /* edge */
          when t.tree > (select max(tree) /* left */
                           from trees l
                          where t.row = l.row
                            and t.col > l.col) then 1
          when t.tree > (select max(tree) /* right */
                           from trees r
                          where t.row = r.row
                            and t.col < r.col) then 1
          when t.tree > (select max(tree) /* up */
                           from trees u
                          where t.col = u.col
                            and t.row > u.row) then 1
          when t.tree > (select max(tree) /* down */
                           from trees d
                          where t.col = d.col
                            and t.row < d.row) then 1
         end as visible
    from trees t
)

, tree_positions as (
  select row
       , col
       , tree
       , (select first_value(col) over (order by col desc)
            from trees l
           where t.row = l.row
             and t.col > l.col
             and t.tree <= l.tree) as col_left
       , (select first_value(col) over (order by col)
            from trees l
           where t.row = l.row
             and t.col < l.col
             and t.tree <= l.tree) as col_right
       , (select first_value(row) over (order by row desc)
            from trees l
           where t.col = l.col
             and t.row > l.row
             and t.tree <= l.tree) as row_up
       , (select first_value(row) over (order by row)
            from trees l
           where t.col = l.col
             and t.row < l.row
             and t.tree <= l.tree) as row_down
    from trees t
)

, part_2 as (
select coalesce(col - col_left, col - 1) as to_left
     , coalesce(col_right - col, (select max(col) from trees) - col) as to_right
     , coalesce(row - row_up, row - 1) as to_up
     , coalesce(row_down - row, (select max(row) from trees) - row) as to_down
  from tree_positions
)


select 1 as part
     , sum(visible) as answer
  from part_1
 union all
select 2 as part
     , max(to_left * to_right * to_down * to_up) as answer
  from part_2