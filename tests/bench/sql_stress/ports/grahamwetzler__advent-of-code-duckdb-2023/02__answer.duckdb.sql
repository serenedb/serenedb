CREATE MACRO trim_null(string) AS CASE WHEN string = '' THEN NULL ELSE trim(string, E' \t\n\r\x0b\x0c') END;
with
  input as (
    select
      regexp_extract(#1, '(\d+)')::int as game_id
      , split(trim(#2), '; ') as subsets
    from read_csv_auto('02/input.csv', sep=':')
  )

  , subset_unnest as (
    select
      game_id
      , unnest(subsets) as subset
    from input
  )

  , counts as (
    select
      game_id
      , trim_null(
          regexp_extract(subset, '(\d+) blue', 1)
      )::int as blue
      , trim_null(
          regexp_extract(subset, '(\d+) red', 1)
      )::int as red
      , trim_null(
          regexp_extract(subset, '(\d+) green', 1)
      )::int as green
      , case
          when red > 12 or green > 13 or blue > 14
          then false
          else true
        end as possible
    from subset_unnest
  )

  , possible_games as (
    select
      game_id
      , bool_and(possible) as is_possible
    from counts
    group by all
    having is_possible
  )

  , part_1 as (
    select
      1 as part
      , sum(game_id) as answer
    from possible_games
  )

  , min_required_to_play as (
    select
      game_id
      , max(red) as red
      , max(blue) as blue
      , max(green) as green
    from counts
    group by all
  )

  , part_2 as (
    select
      2 as part
      , sum(red * blue * green) as answer
    from min_required_to_play
  )

from part_1
union all
from part_2
