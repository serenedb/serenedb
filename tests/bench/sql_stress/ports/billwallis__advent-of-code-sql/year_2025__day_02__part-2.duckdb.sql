with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2025/day_02/sample.data', header=false, delim='')
),

ranges as (
    from (
        from input
        select unnest(split(data, ',')) as ids
    )
    select
        split_part(ids, '-', 1) as id_l,
        split_part(ids, '-', 2) as id_r,
),

max_len as (
    select max(len(id_r)) // 2 as max_len
    from ranges
),

/* Brute force _all_ the invalid IDs, rather than just those in the ranges */
digits(digit) as (
    from generate_series(1, (select power(10, max_len)::int from max_len))
),
all_invalid_ids as (
        select digit, concat(digit, digit) as id from digits
    union all
        select digit, concat(id, digit) as id_
        from all_invalid_ids
        where len(id_) <= (select max_len * 2 from max_len)
),
invalid_ids as (select distinct id from all_invalid_ids)

select sum(invalid_ids.id::bigint)
from invalid_ids
    semi join ranges
        on invalid_ids.id::bigint between ranges.id_l::bigint
                                      and ranges.id_r::bigint
;
