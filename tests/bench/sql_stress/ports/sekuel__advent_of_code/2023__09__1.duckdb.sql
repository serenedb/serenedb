WITH RECURSIVE input AS (
    SELECT
        row_number() OVER () AS rn,
        generate_subscripts(split(column0, ' '), 1) AS pos,
        unnest(split(column0, ' ')::bigint []) AS val
    FROM read_csv_auto('./2023/09/input.csv', sep='', header=false)
), diffs AS (
    SELECT 
        rn,
        pos,
        val,
        lead(val) OVER (PARTITION BY rn ORDER BY pos) AS next_val,
        (next_val - val) AS diff,
    FROM input
), init AS (
    SELECT
        rn,
        pos,
        val,
        next_val,
        diff,
        0 AS iter
    FROM diffs
    UNION ALL
    SELECT 
        init.rn,
        init.pos,
        init.diff AS val,
        lead(init.diff) OVER (PARTITION BY init.rn, init.iter ORDER BY init.pos) AS next_diff,
        next_diff - init.diff AS diff,
        iter + 1,
    FROM init
    QUALIFY diff IS NOT NULL
), first_and_last AS (
    SELECT
        rn,
        iter,
        array_agg(val ORDER BY iter)[1] AS first_value,
        array_agg(next_val ORDER BY iter)[-1] AS last_ext_value,
    FROM init
    WHERE diff IS NOT NULL
    GROUP BY rn, iter
), backwards AS (
    SELECT 
        rn,  
        SUM(first_value * (-1)^(iter))::int AS first_ext_value 
    FROM first_and_last 
    GROUP BY 1
)
SELECT 'Part I' AS part, sum(last_ext_value) AS answer FROM first_and_last
UNION ALL
SELECT 'Part II' AS part, sum(first_ext_value) AS answer FROM backwards;