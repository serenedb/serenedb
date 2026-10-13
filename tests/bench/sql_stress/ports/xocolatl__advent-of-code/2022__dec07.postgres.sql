CREATE SCHEMA IF NOT EXISTS aoc2022;
SET SCHEMA 'aoc2022';
DROP TABLE IF EXISTS dec07;

CREATE TABLE dec07 (
    line_number serial NOT NULL,
    line CHARACTER VARYING
);

\COPY dec07 (line) FROM '2022/dec07.input'
VACUUM ANALYZE dec07;

/* FIRST STAR */

WITH RECURSIVE

structure (line_number, line, path, file, size) AS (
    VALUES (0,
            '',
            CAST(ARRAY[] AS text ARRAY),
            '',
            CAST(0 AS BIGINT)
    )

    UNION ALL

    SELECT d.line_number,
           d.line,
           CASE WHEN v.new_path = '..' THEN TRIM_ARRAY(s.path, 1)
                WHEN v.new_path IS NOT NULL THEN array_append(s.path, v.new_path)
           ELSE s.path
           END,
           v.file,
           v.size
    FROM structure AS s
    JOIN dec07 AS d ON d.line_number = s.line_number + 1
    CROSS JOIN LATERAL (VALUES (
        (regexp_match(d.line, '^\$ cd (.*)$'))[1],
        (regexp_match(d.line, '^[0-9]* (.*)$'))[1],
        CAST(NULLIF((regexp_match(d.line, '^([0-9]*).*$'))[1], '') AS BIGINT)
    )) AS v (new_path, file, size)
),

paths (path) AS (
    SELECT DISTINCT path
    FROM structure AS s
    WHERE CARDINALITY(path) > 0
),

sizes (path, size) AS (
    SELECT p.path, SUM(s.size)
    FROM paths AS p
    JOIN structure AS s ON s.path[:CARDINALITY(p.path)] = p.path
    GROUP BY p.path
)

SELECT SUM(size) AS first_star
FROM sizes
WHERE size < 100000
;

/* SECOND STAR */

WITH RECURSIVE

structure (line_number, line, path, file, size) AS (
    VALUES (0,
            '',
            CAST(ARRAY[] AS text ARRAY),
            '',
            CAST(0 AS BIGINT)
    )

    UNION ALL

    SELECT d.line_number,
           d.line,
           CASE WHEN v.new_path = '..' THEN TRIM_ARRAY(s.path, 1)
                WHEN v.new_path IS NOT NULL THEN array_append(s.path, v.new_path)
           ELSE s.path
           END,
           v.file,
           v.size
    FROM structure AS s
    JOIN dec07 AS d ON d.line_number = s.line_number + 1
    CROSS JOIN LATERAL (VALUES (
        (regexp_match(d.line, '^\$ cd (.*)$'))[1],
        (regexp_match(d.line, '^[0-9]* (.*)$'))[1],
        CAST(NULLIF((regexp_match(d.line, '^([0-9]*).*$'))[1], '') AS BIGINT)
    )) AS v (new_path, file, size)
),

paths (path) AS (
    SELECT DISTINCT path
    FROM structure AS s
    WHERE CARDINALITY(path) > 0
),

sizes (path, size) AS (
    SELECT p.path, SUM(s.size)
    FROM paths AS p
    JOIN structure AS s ON s.path[:CARDINALITY(p.path)] = p.path
    GROUP BY p.path
),

unused (size) AS (
    SELECT 70000000 - size
    FROM sizes
    WHERE path = ARRAY['/']
)

SELECT MIN(size) AS second_star
FROM sizes
WHERE (TABLE unused) + size >= 30000000
;
