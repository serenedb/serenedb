CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec06;

CREATE TABLE dec06 (
    line_number bigserial NOT NULL,
    line CHARACTER VARYING
);

\COPY dec06 (line) FROM '2022/dec06.input'
VACUUM ANALYZE dec06;

/* FIRST STAR */

WITH

fours (position, vals) AS (
    SELECT s.ordinality, array_agg(s.c) OVER w
    FROM dec06 AS d
    CROSS JOIN LATERAL unnest(string_to_array(d.line, NULL)) WITH ORDINALITY AS s (c)
    WINDOW w AS (ORDER BY s.ordinality ROWS 3 PRECEDING)
)

SELECT position AS first_star
FROM fours
WHERE CARDINALITY(vals) = 4
  AND vals[1] NOT IN (vals[2], vals[3], vals[4])
  AND vals[2] NOT IN (vals[3], vals[4])
  AND vals[3] NOT IN (vals[4])
ORDER BY position
FETCH FIRST ROW ONLY
;

/* SECOND STAR */

WITH

fourteens (position, vals) AS (
    SELECT s.ordinality, array_agg(s.c) OVER w
    FROM dec06 AS d
    CROSS JOIN LATERAL unnest(string_to_array(d.line, NULL)) WITH ORDINALITY AS s (c)
    WINDOW w AS (ORDER BY s.ordinality ROWS 13 PRECEDING)
)

SELECT u.position AS second_star
FROM fourteens AS f
CROSS JOIN LATERAL (
    SELECT f.position, array_agg(DISTINCT x)
    FROM UNNEST(f.vals) AS u (x)
) AS u (position, vals)
WHERE CARDINALITY(u.vals) = 14
ORDER BY u.position
FETCH FIRST ROW ONLY
;
