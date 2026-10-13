CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec07;

CREATE TABLE dec07 (
    line_number bigserial NOT NULL,
    value text NOT NULL
);

\COPY dec07 (value) FROM '2021/dec07.input'
VACUUM ANALYZE dec07;

/* FIRST STAR */

/*
 * The ideal meeting position is the median of all the positions.  In SQL, the
 * median is expressed as PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pos).
 *
 * The query is then just summing up the fuel used to get to that median position.
 */

WITH

input (pos) AS (
    SELECT CAST(pos AS integer)
    FROM dec07
    CROSS JOIN LATERAL unnest(string_to_array(value, ',')) AS s (pos)
),

target (pos) AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY pos)
    FROM input
)

SELECT sum(abs(pos - (TABLE target))) AS first_star
FROM input
;

/* SECOND STAR */

/*
 * Now the ideal meeting position is just the integral average.  We could round
 * the avg() function, but instead we just calculate it ourselves using
 * integral division on the sum() and count().
 *
 * Since the fuel consumption is adding up numbers from 1 to n, we can use the
 * Gauss formula for that: n * (n+1) / 2
 */

WITH

input (pos) AS (
    SELECT CAST(pos AS integer)
    FROM dec07
    CROSS JOIN LATERAL unnest(string_to_array(value, ',')) AS s (pos)
),

target (pos) AS (
    SELECT sum(pos) / count(*)
    FROM input
)

SELECT sum(distance * (distance + 1) / 2) AS second_star
FROM (
    SELECT abs(pos - (TABLE target)) AS distance
    FROM input
) AS _
;
