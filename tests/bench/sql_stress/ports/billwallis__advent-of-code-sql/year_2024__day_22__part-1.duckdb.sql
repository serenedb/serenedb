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

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_22/sample-1.data', header=false)
),

secret_numbers(n, secret_number) as (
        select 0, data::bigint
        from input
    union all
        select n + 1, mix(secret_number)
        from secret_numbers
        where n < 2000
)

select sum(secret_number)
from secret_numbers
where n = 2000
;
