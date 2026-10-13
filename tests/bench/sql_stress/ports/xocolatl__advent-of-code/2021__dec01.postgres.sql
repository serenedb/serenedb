CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec01;

CREATE TABLE dec01 (
    line_number bigserial NOT NULL,
    value bigint NOT NULL
);

\COPY dec01 (value) FROM '2021/dec01.input'
VACUUM ANALYZE dec01;

/* FIRST STAR */

/*
 * The subquery is needed here because SQL doesn't know how to filter a window
 * clause.  Some implementations have a QUALIFY clause for this purpose, but
 * that is neither in the Standard nor in PostgreSQL.
 */

SELECT count(*) AS first_star
FROM (
    SELECT value > lag(value) OVER (ORDER BY line_number) AS increase
    FROM dec01
) AS _
WHERE increase
;

/* SECOND STAR */

/*
 * This is the same basic query as the previous one, but since we can't nest
 * window functions, we have to nest entire query expressions.
 */

SELECT count(*) AS second_star
FROM (
    SELECT sum > lag(sum) OVER (ORDER BY line_number) AS increase
    FROM (
        SELECT line_number,
               count(*) OVER w,
               sum(value) OVER w
        FROM dec01
        WINDOW w AS (ORDER BY line_number ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
    ) AS _
    WHERE count = 3
) AS _
WHERE increase
;

