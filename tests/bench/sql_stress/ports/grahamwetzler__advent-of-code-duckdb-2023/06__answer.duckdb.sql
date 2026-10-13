with
  input as (
    select
      #1 as key
      , unnest(
          regexp_extract_all(#2, '(\d+)')::int[]
      ) as value
    from read_csv_auto('06/input.csv', sep=':')
  )

  , row_numbered as (
    select
      row_number() over (partition by key) as row
      , key
      , value
    from input
  )

  , time as (
    select
      row
      , value as time
    from row_numbered
    where key = 'Time'
  )

  , distance as (
    select
      row
      , value as distance
    from row_numbered
    where key = 'Distance'
  )

  , records as (
    select
      time
      , distance
    from time
    join distance using (row)
  )

  , possiblities_part_1 as (
    select
      time
      , distance
      , unnest(
          generate_series(1, time)
      ) as charge_time
    from records
  )

  , check_times_part_1 as (
    select
      time
      , distance
      , charge_time * (time - charge_time) as traveled
    from possiblities_part_1
    where traveled > distance
  )

  , ways_to_record_part_1 as (
    select
      time
      , distance
      , count(*) as ways_to_record
    from check_times_part_1
    group by all
  )

  , records_part_2 as (
    select
      string_agg(time, '')::int as time
      , string_agg(distance, '')::int64 as distance
    from records
  )

  , possiblities_part_2 as (
    select
      time
      , distance
      , unnest(
          generate_series(1, time)
      ) as charge_time
    from records_part_2
  )

  , check_times_part_2 as (
    select
      time
      , distance
      , charge_time * (time - charge_time) as traveled
    from possiblities_part_2
    where traveled > distance
  )

  , ways_to_record_part_2 as (
    select
      time
      , distance
      , count(*) as ways_to_record
    from check_times_part_2
    group by all
  )

select
  1 as part
  , product(ways_to_record)::int as answer
from ways_to_record_part_1

union all

select
  2 as part
  , product(ways_to_record)::int as answer
from ways_to_record_part_2
