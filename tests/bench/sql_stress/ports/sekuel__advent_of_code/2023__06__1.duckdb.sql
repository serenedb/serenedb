WITH input AS (
    SELECT
        row_number() OVER () as id, 
        generate_subscripts(regexp_extract_all(string_split(column0, ':')[2], '\d+'), 1) AS race_id,
        unnest(regexp_extract_all(string_split(column0, ':')[2], '\d+')) AS val
    FROM read_csv_auto('./2023/06/input.txt', header=false)
), combined_input AS (
    SELECT
        'Part I' AS part,
        race_id,
        max(CASE id WHEN 1 THEN val END)::bigint AS duration,
        max(CASE id WHEN 2 THEN val END)::bigint AS distance
    FROM input
    GROUP BY 1,2
    UNION ALL
    SELECT 
        'Part II' as part,
        1 AS race_id,
        replace(string_agg(CASE id WHEN 1 THEN val END), ',','')::bigint AS duration,
        replace(string_agg(CASE id WHEN 2 THEN val END), ',','')::bigint AS distance
    FROM input
    GROUP BY 1,2
), speeds as (
    SELECT *, unnest(range(0, duration)) AS speed
    FROM combined_input
), fastest as ( 
    SELECT part, race_id, count(1) AS winnings 
    FROM speeds
    WHERE distance < speed * (duration - speed)
    GROUP BY 1,2
)
SELECT part, product(winnings)::int AS answer FROM fastest WHERE part = 'Part I' GROUP BY 1
UNION ALL
SELECT part, winnings FROM fastest WHERE part = 'Part II';