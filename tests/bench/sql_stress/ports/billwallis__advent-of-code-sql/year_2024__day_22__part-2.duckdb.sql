create or replace macro prune(number) as
    mod(number, 2^24)::bigint
;
create or replace macro mix_64(number) as
    prune(xor(number::bitstring, ((number * 2^6)::bigint)::bitstring)::bigint)
;
create or replace macro mix_32(number) as
    prune(xor(number::bitstring, (floor(number / 2^5)::bigint)::bitstring)::bigint)
;
create or replace macro mix_2048(number) as
    prune(xor(number::bitstring, ((number * 2^11)::bigint)::bitstring)::bigint)
;
create or replace macro mix(number) as
    mix_2048(mix_32(mix_64(number)))
;


with recursive

input(data, buyer_id) as (
    select *, row_number() over ()
    from read_csv('advent_of_code/solutions/year_2024/day_22/sample-2.data', header=false)
),

secret_numbers(buyer_id, n, secret_number) as (
        select buyer_id, 0, data::bigint
        from input
    union all
        select buyer_id, n + 1, mix(secret_number)
        from secret_numbers
        where n < 2000
),

prices as (
    select
        buyer_id,
        n,
        secret_number % 10 as price,
        price - lag(price) over (partition by buyer_id order by n) as diff,
    from secret_numbers
),

sequences as (
    from (
        from prices
        select
            *,
            list(diff) over (
                partition by buyer_id
                order by n
                rows 3 preceding
            ) as seq,
    )
    qualify 1=1
        and 1 = row_number() over (partition by buyer_id, seq order by n)
        and len(seq) = 4
)

select sum(price) as total_price
from sequences
group by seq
order by total_price desc
limit 1
;
