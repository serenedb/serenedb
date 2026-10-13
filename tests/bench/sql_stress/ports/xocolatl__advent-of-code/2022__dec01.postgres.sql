CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec01;

CREATE TABLE dec01 (
    line_number bigserial NOT NULL,
    value bigint
);

\COPY dec01 (value) FROM '2022/dec01.input' NULL ''
VACUUM ANALYZE dec01;

/* FIRST STAR */

SELECT SUM(value) AS first_star
FROM (
    SELECT value,
           COUNT(*) FILTER (WHERE value IS NULL) OVER (ORDER BY line_number) AS grp
    FROM dec01
)
GROUP BY grp
ORDER BY first_star DESC
FETCH FIRST ROW ONLY
;

/* SECOND STAR */

SELECT SUM(calories) AS second_star
FROM (
    SELECT SUM(value) AS calories
    FROM (
        SELECT value,
               COUNT(*) FILTER (WHERE value IS NULL) OVER (ORDER BY line_number) AS grp
        FROM dec01
    )
    GROUP BY grp
    ORDER BY calories DESC
    FETCH FIRST 3 ROWS ONLY
)
;
