CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec04;

CREATE TABLE dec04 (
    line_number bigserial NOT NULL,
    line text
);

\COPY dec04 (line) FROM '2022/dec04.input'
VACUUM ANALYZE dec04;

/* FIRST STAR */

WITH

pairs (assignment1, assignment2) AS (
    SELECT int8range(CAST(m[1] AS BIGINT), CAST(m[2] AS BIGINT), '[]'),
           int8range(CAST(m[3] AS BIGINT), CAST(m[4] AS BIGINT), '[]')
    FROM dec04 AS d
    CROSS JOIN LATERAL (SELECT regexp_match(d.line, '^(\d+)-(\d+),(\d+)-(\d+)$') WHERE d.line ~ '^(\d+)-(\d+),(\d+)-(\d+)$') AS lm (m)
)

SELECT COUNT(*) AS first_star
FROM pairs
WHERE assignment1 @> assignment2 OR assignment2 @> assignment1
;

/* SECOND STAR */

WITH

pairs (assignment1, assignment2) AS (
    SELECT int8range(CAST(m[1] AS BIGINT), CAST(m[2] AS BIGINT), '[]'),
           int8range(CAST(m[3] AS BIGINT), CAST(m[4] AS BIGINT), '[]')
    FROM dec04 AS d
    CROSS JOIN LATERAL (SELECT regexp_match(d.line, '^(\d+)-(\d+),(\d+)-(\d+)$') WHERE d.line ~ '^(\d+)-(\d+),(\d+)-(\d+)$') AS lm (m)
)

SELECT COUNT(*) AS second_star
FROM pairs
WHERE assignment1 && assignment2
;

/* Standard SQL */

/*
 * The following query is Standard SQL and calculates both stars at once.
 *

WITH

pairs (assignment1, assignment2) AS (
    SELECT PERIOD(CAST(REGEX_SUBSTRING('[0-9]+' IN d.line OCCURENCE 1) AS BIGINT),
                  CAST(REGEX_SUBSTRING('[0-9]+' IN d.line OCCURENCE 2) AS BIGINT) + 1),
           PERIOD(CAST(REGEX_SUBSTRING('[0-9]+' IN d.line OCCURENCE 3) AS BIGINT),
                  CAST(REGEX_SUBSTRING('[0-9]+' IN d.line OCCURENCE 4) AS BIGINT) + 1)
    FROM dec04 AS d
)

SELECT COUNT(*) FILTER (WHERE assignment1 CONTAINS assignment2 OR assignment2 CONTAINS assignment1) AS first_star,
       COUNT(*) FILTER (WHERE assignment1 OVERLAPS assignment2) AS second_star
FROM pairs

 *
 */
