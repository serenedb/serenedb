-- Basic Template
-- A simple time-based pattern.

WITH
colors AS (
    SELECT
        0.5 + 0.5 * sin((p.x::DOUBLE / 160::DOUBLE) + CAST(1.0 AS DOUBLE)) AS r,
        0.5 + 0.5 * cos((p.y::DOUBLE / 120::DOUBLE) + CAST(1.0 AS DOUBLE)) AS g,
        0.5 + 0.5 * sin(CAST(1.0 AS DOUBLE) * 0.5) AS b,
        p.x,
        p.y
    FROM
        ((SELECT unnest(range(160::INT)) AS x) 
         CROSS JOIN (SELECT unnest(range(120::INT)) AS y)) AS p
)
SELECT
    CAST(r AS DOUBLE) AS r,
    CAST(g AS DOUBLE) AS g,
    CAST(b AS DOUBLE) AS b
FROM
    colors
ORDER BY
    y ASC,
    x ASC