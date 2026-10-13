CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec05;

CREATE TABLE dec05 (
    line_number bigserial NOT NULL,
    line CHARACTER VARYING
);

\COPY dec05 (line) FROM '2022/dec05.input'
VACUUM ANALYZE dec05;

/* FIRST STAR */

WITH RECURSIVE

stacks (stacks) AS (
    SELECT json_build_array(
                TRIM(string_agg(SUBSTRING(d.line FROM  2 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM  6 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 10 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 14 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 18 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 22 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 26 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 30 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 34 FOR 1), '' ORDER BY d.line_number)))
    FROM dec05 AS d
    WHERE d.line LIKE '%[%'
),

moves (step, num, src, dst) AS (
    SELECT ROW_NUMBER() OVER (ORDER BY d.line_number),
           CAST(m[1] AS INTEGER),
           CAST(m[2] AS INTEGER),
           CAST(m[3] AS INTEGER)
    FROM dec05 AS d
    CROSS JOIN LATERAL (SELECT regexp_match(d.line, 'move (\d+) from (\d+) to (\d)') WHERE d.line ~ 'move (\d+) from (\d+) to (\d)') AS lm (m)
),

run (step, num, src, dst, stacks) AS (
    SELECT CAST(0 AS BIGINT), 0, 0, 0, s.stacks
    FROM stacks AS s

    UNION ALL

    SELECT m.step, m.num, m.src, m.dst,
           json_set(json_set(r.stacks,
                               '$[' || (m.dst-1) || ']',
                               to_json(reverse(SUBSTRING(r.stacks->>(m.src-1) FOR m.num)) || (r.stacks->>(m.dst-1)))),
                     '$[' || (m.src-1) || ']',
                     to_json(SUBSTRING(r.stacks->>(m.src-1) FROM m.num+1)))
    FROM run AS r
    JOIN moves AS m ON m.step = r.step + 1
)

SELECT SUBSTRING(r.stacks->>0 FOR 1)
    || SUBSTRING(r.stacks->>1 FOR 1)
    || SUBSTRING(r.stacks->>2 FOR 1)
    || SUBSTRING(r.stacks->>3 FOR 1)
    || SUBSTRING(r.stacks->>4 FOR 1)
    || SUBSTRING(r.stacks->>5 FOR 1)
    || SUBSTRING(r.stacks->>6 FOR 1)
    || SUBSTRING(r.stacks->>7 FOR 1)
    || SUBSTRING(r.stacks->>8 FOR 1) AS first_star
FROM run AS r
ORDER BY r.step DESC
FETCH FIRST ROW ONLY
;

/* SECOND STAR */

/* This is the exact same query as the first star, but without the reverse() call */

WITH RECURSIVE

stacks (stacks) AS (
    SELECT json_build_array(
                TRIM(string_agg(SUBSTRING(d.line FROM  2 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM  6 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 10 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 14 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 18 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 22 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 26 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 30 FOR 1), '' ORDER BY d.line_number)),
                TRIM(string_agg(SUBSTRING(d.line FROM 34 FOR 1), '' ORDER BY d.line_number)))
    FROM dec05 AS d
    WHERE d.line LIKE '%[%'
),

moves (step, num, src, dst) AS (
    SELECT ROW_NUMBER() OVER (ORDER BY d.line_number),
           CAST(m[1] AS INTEGER),
           CAST(m[2] AS INTEGER),
           CAST(m[3] AS INTEGER)
    FROM dec05 AS d
    CROSS JOIN LATERAL (SELECT regexp_match(d.line, 'move (\d+) from (\d+) to (\d)') WHERE d.line ~ 'move (\d+) from (\d+) to (\d)') AS lm (m)
),

run (step, num, src, dst, stacks) AS (
    SELECT CAST(0 AS BIGINT), 0, 0, 0, s.stacks
    FROM stacks AS s

    UNION ALL

    SELECT m.step, m.num, m.src, m.dst,
           json_set(json_set(r.stacks,
                               '$[' || (m.dst-1) || ']',
                               to_json(SUBSTRING(r.stacks->>(m.src-1) FOR m.num) || (r.stacks->>(m.dst-1)))),
                     '$[' || (m.src-1) || ']',
                     to_json(SUBSTRING(r.stacks->>(m.src-1) FROM m.num+1)))
    FROM run AS r
    JOIN moves AS m ON m.step = r.step + 1
)

SELECT SUBSTRING(r.stacks->>0 FOR 1)
    || SUBSTRING(r.stacks->>1 FOR 1)
    || SUBSTRING(r.stacks->>2 FOR 1)
    || SUBSTRING(r.stacks->>3 FOR 1)
    || SUBSTRING(r.stacks->>4 FOR 1)
    || SUBSTRING(r.stacks->>5 FOR 1)
    || SUBSTRING(r.stacks->>6 FOR 1)
    || SUBSTRING(r.stacks->>7 FOR 1)
    || SUBSTRING(r.stacks->>8 FOR 1) AS second_star
FROM run AS r
ORDER BY r.step DESC
FETCH FIRST ROW ONLY
;

