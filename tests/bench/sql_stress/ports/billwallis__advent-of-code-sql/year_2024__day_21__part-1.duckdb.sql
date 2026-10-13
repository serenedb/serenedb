with recursive

input(data) as (
    select *
    from read_csv('advent_of_code/solutions/year_2024/day_21/sample.data', header=false)
),

numeric_keypad(x, y, key) as (
    values
        (0, 0, '7'),
        (1, 0, '8'),
        (2, 0, '9'),
        (0, 1, '4'),
        (1, 1, '5'),
        (2, 1, '6'),
        (0, 2, '1'),
        (1, 2, '2'),
        (2, 2, '3'),
        (1, 3, '0'),
        (2, 3, 'A'),
),

directional_keypad(x, y, key) as (
    values
        (1, 0, '^'),
        (2, 0, 'A'),
        (0, 1, '<'),
        (1, 1, 'v'),
        (2, 1, '>'),
),

codes as (
    from input
    select
        data as code,
        generate_subscripts(split(data, NULL), 1) as n,
        unnest(split(data, NULL)) as key,
),

press(code, n, key, presses, x, y) as (
        select distinct code, 0, 'A', 0, 2, 3  /* get from keypad CTE */
        from codes
    union all
        select
            press.code,
            press.n + 1,
            codes.key,
            1 + press.presses + abs(press.x - numeric_keypad.x) + abs(press.y - numeric_keypad.y),
            numeric_keypad.x,
            numeric_keypad.y,
        from press
            inner join codes
                on (press.code, press.n + 1) = (codes.code, codes.n)
            left join numeric_keypad
                on codes.key = numeric_keypad.key
)

from press
where n = (select max(n) from press)
order by code
;


select 0
;
