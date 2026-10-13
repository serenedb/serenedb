CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec08;

CREATE TABLE dec08 (
    line_number serial NOT NULL,
    line CHARACTER VARYING
);

\COPY dec08 (line) FROM '2022/dec08.input'
VACUUM ANALYZE dec08;

/* FIRST STAR */

WITH

trees (x, y, height) AS (
    SELECT t.x,
           d.line_number,
           CAST(t.height AS INTEGER)
    FROM dec08 AS d
    CROSS JOIN unnest(string_to_array(d.line, NULL)) WITH ORDINALITY AS t (height, x)
),

visible (x, y, visible) AS (
    SELECT x, y,
           height > COALESCE(MAX(height) OVER from_north, -1) or
           height > COALESCE(MAX(height) OVER from_east,  -1) or
           height > COALESCE(MAX(height) OVER from_south, -1) or
           height > COALESCE(MAX(height) OVER from_west,  -1)
    FROM trees
    WINDOW from_north AS (PARTITION BY x ORDER BY y ASC  ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW),
           from_east  AS (PARTITION BY y ORDER BY x DESC ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW),
           from_south AS (PARTITION BY x ORDER BY y DESC ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW),
           from_west  AS (PARTITION BY y ORDER BY x ASC  ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW)
)

SELECT COUNT(*) AS first_star
FROM visible AS v
WHERE v.visible
;

/* SECOND STAR */

WITH RECURSIVE

trees (x, y, height) AS (
    SELECT t.x,
           d.line_number,
           CAST(t.height AS INTEGER)
    FROM dec08 AS d
    CROSS JOIN unnest(string_to_array(d.line, NULL)) WITH ORDINALITY AS t (height, x)
),

flood (ox, oy, x, y, height, dx, dy, blocked) AS (
    SELECT x, y, x, y, height, dx, dy, false
    FROM trees
    CROSS JOIN (VALUES (0, -1), (1, 0), (0, 1), (-1, 0)) AS v (dx, dy)

    UNION ALL

    SELECT f.ox, f.oy, f.x + f.dx, f.y + f.dy, f.height, f.dx, f.dy, t.height >= f.height
    FROM flood AS f
    JOIN trees AS t ON (t.x, t.y) = (f.x + f.dx, f.y + f.dy)
    WHERE NOT f.blocked
),

lines (x, y, trees) AS (
    SELECT ox, oy, COUNT(*)
    FROM flood
    WHERE (x, y) <> (ox, oy)
    GROUP BY ox, oy, dx, dy
),

scores (x, y, score) AS (
    SELECT x, y, ARRAY_AGG(trees)
    FROM lines
    GROUP BY x, y
)

SELECT MAX(COALESCE(score[1], 0) * COALESCE(score[2], 0) * COALESCE(score[3], 0) * COALESCE(score[4], 0)) AS second_star
FROM scores
;
