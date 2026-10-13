CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec09;

CREATE TABLE dec09 (
    line_number bigserial NOT NULL,
    value text NOT NULL
);

\COPY dec09 (value) FROM '2021/dec09.input'
VACUUM ANALYZE dec09;

/* FIRST STAR */

WITH

/* Turn the grid into a bunch of x,y coordinates */
input (x, y, height) AS (
    SELECT ordinality, line_number, CAST(height AS integer)
    FROM dec09
    CROSS JOIN LATERAL unnest(string_to_array(value, NULL)) WITH ORDINALITY AS s (height)
)

/*
 * Find all the cells whose height is lower than its four orthogonal neighbors.
 */
SELECT sum(i.height + 1) AS first_star
FROM input AS i
LEFT JOIN input AS n ON (n.x, n.y) = (i.x, i.y-1)
LEFT JOIN input AS e ON (e.x, e.y) = (i.x+1, i.y)
LEFT JOIN input AS s ON (s.x, s.y) = (i.x, i.y+1)
LEFT JOIN input AS w ON (w.x, w.y) = (i.x-1, i.y)
WHERE i.height < ALL (ARRAY[
    COALESCE(n.height, 9),
    COALESCE(e.height, 9),
    COALESCE(s.height, 9),
    COALESCE(w.height, 9)])
;

/* SECOND STAR */

WITH RECURSIVE

input (x, y, height) AS (
    SELECT ordinality, line_number, CAST(height AS integer)
    FROM dec09
    CROSS JOIN LATERAL unnest(string_to_array(value, NULL)) WITH ORDINALITY AS s (height)
),

low_points (id, x, y, height) AS (
    SELECT row_number() OVER (ORDER BY i.x, i.y),
           i.x, i.y, i.height
    FROM input AS i
    LEFT JOIN input AS n ON (n.x, n.y) = (i.x, i.y-1)
    LEFT JOIN input AS e ON (e.x, e.y) = (i.x+1, i.y)
    LEFT JOIN input AS s ON (s.x, s.y) = (i.x, i.y+1)
    LEFT JOIN input AS w ON (w.x, w.y) = (i.x-1, i.y)
    WHERE i.height < ALL (ARRAY[
        COALESCE(n.height, 9),
        COALESCE(e.height, 9),
        COALESCE(s.height, 9),
        COALESCE(w.height, 9)])
),

/*
 * Starting with the low points, flood fill the surrounding neighbors until we
 * hit 9s or the edge of the grid.
 */
flood (id, x, y) AS (
    SELECT id, x, y
    FROM low_points

    UNION ALL

    SELECT f.id, i.x, i.y
    FROM flood AS f
    CROSS JOIN (VALUES (-1), (0), (1)) AS vx (dx)
    CROSS JOIN (VALUES (-1), (0), (1)) AS vy (dy)
    JOIN input AS i ON (i.x, i.y) = (f.x + dx, f.y + dy) AND i.height > f.height
    WHERE i.height < 9
      AND 0 IN (dx, dy) /* Make sure we're not moving diagonally */
)
CYCLE x, y SET is_cycle USING path,

largest_three (count) AS (
    /*
     * Whenever possible, it is better to do a SELECT DISTINCT rather than
     * putting the DISTINCT in the aggregate.  This is because Postgres has
     * several ways of doing the former but only one (slow) way of doing the
     * latter.
     */
    SELECT count(*)
    FROM (
        SELECT DISTINCT id, x, y
        FROM flood
        WHERE NOT is_cycle
    ) AS _
    GROUP BY id
    ORDER BY count DESC
    FETCH FIRST 3 ROWS ONLY
)

/* A product() aggregate would be nice here */
SELECT a[1] * a[2] * a[3] AS second_star
FROM (SELECT array_agg(count) AS a FROM largest_three) AS _
;
