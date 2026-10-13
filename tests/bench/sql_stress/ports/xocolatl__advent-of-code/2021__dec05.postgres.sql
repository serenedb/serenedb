CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec05;

CREATE TABLE dec05 (
    line_number bigserial NOT NULL,
    value text NOT NULL,

    /* Parse out the different columns on the fly */

    x1 bigint GENERATED ALWAYS AS (
         CAST((regexp_match(value, '^(\d+),(\d+) -> (\d+),(\d+)$'))[1] AS bigint)
        ) STORED,

    y1 bigint GENERATED ALWAYS AS (
         CAST((regexp_match(value, '^(\d+),(\d+) -> (\d+),(\d+)$'))[2] AS bigint)
        ) STORED,

    x2 bigint GENERATED ALWAYS AS (
         CAST((regexp_match(value, '^(\d+),(\d+) -> (\d+),(\d+)$'))[3] AS bigint)
        ) STORED,

    y2 bigint GENERATED ALWAYS AS (
         CAST((regexp_match(value, '^(\d+),(\d+) -> (\d+),(\d+)$'))[4] AS bigint)
        ) STORED
);

\COPY dec05 (value) FROM '2021/dec05.input'

/*
 * Interestingly, it is easier to do the second star today than it is the
 * first; and again we are using just one query to do them both.
 *
 * We start by generating all the points on all the lines, and then we simply
 * group them together and filter out the "safe" zones.  For part one, we also
 * filter out diagonal lines.
 */

WITH RECURSIVE

lines (x1, y1, x2, y2, x, y) AS (
    SELECT x1, y1, x2, y2, x1, y1
    FROM dec05

    UNION ALL

    SELECT x1, y1, x2, y2,
           CASE WHEN x < x2 THEN x+1
                WHEN x > x2 THEN x-1
                ELSE x
           END,
           CASE WHEN y < y2 THEN y+1
                WHEN y > y2 THEN y-1
                ELSE y
           END
    FROM lines
    WHERE x <> x2 OR y <> y2
),

first_star (value) AS (
    SELECT x, y, count(*)
    FROM lines
    WHERE x1 = x2 OR y1 = y2
    GROUP BY x, y
    HAVING count(*) > 1
),

second_star (value) AS (
    SELECT x, y, count(*)
    FROM lines
    GROUP BY x, y
    HAVING count(*) > 1
)

SELECT (SELECT count(*) FROM first_star)  AS first_star,
       (SELECT count(*) FROM second_star) AS second_star
;
