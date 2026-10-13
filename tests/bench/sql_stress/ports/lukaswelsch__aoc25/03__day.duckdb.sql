WITH data AS (
    SELECT regexp_split_to_array(column0, '') as split
    FROM read_csv_auto('03/example.csv', header = false, delim = '', columns={'column0': 'VARCHAR'})
),
a1 AS (
    SELECT list_max(split[1:-12]) as a1, list_position(split, a1) as d1, *
    FROM data
),
a2 AS (
    SELECT list_max(split[d1+1:-11]) as a2, list_position(split[d1+1:], a2) + d1 as d2, *
    FROM a1
),
a3 AS (
    SELECT list_max(split[d2+1:-10]) as a3, list_position(split[d2+1:], a3) + d2 as d3, *
    FROM a2
),
a4 AS (
    SELECT list_max(split[d3+1:-9]) as a4, list_position(split[d3+1:], a4) + d3 as d4, *
    FROM a3
),
a5 AS (
    SELECT list_max(split[d4+1:-8]) as a5, list_position(split[d4+1:], a5)+ d4 as d5, *
    FROM a4
),
a6 AS (
    SELECT list_max(split[d5+1:-7]) as a6, list_position(split[d5 + 1:], a6)+ d5 as d6,  *
    FROM a5
),
a7 AS (
    SELECT list_max(split[d6+1:-6]) as a7, list_position(split[d6+1:], a7)+ d6 as d7, *
    FROM a6
),
a8 AS (
    SELECT list_max(split[d7+1:-5]) as a8, list_position(split[d7+1:], a8)+ d7 as d8, *
    FROM a7
),
a9 AS (
    SELECT list_max(split[d8+1:-4]) as a9, list_position(split[d8+1:], a9)+ d8 as d9, *
    FROM a8
),
a10 AS (
    SELECT list_max(split[d9+1:-3]) as a10, list_position(split[d9+1:], a10)+ d9 as d10, *
    FROM a9
),
a11 AS (
    SELECT list_max(split[d10+1:-2]) as a11, list_position(split[d10+1:], a11)+ d10 as d11, *
    FROM a10
),
a12 AS (
    SELECT list_max(split[d11+1:]) as a12, list_position(split[d11+1:], a12)+ d11 as d12, *
    FROM a11
)
SELECT  SUM((a1 || a2 || a3 || a4  || a5  || a6  || a7 || a8 || a9 || a10 || a11 || a12)::HUGEINT) as result
FROM a12

