CREATE SCHEMA IF NOT EXISTS aoc2021;
SET SCHEMA 'aoc2021';
DROP TABLE IF EXISTS dec12;

CREATE TABLE dec12 (
    line_number bigserial NOT NULL,
    src text NOT NULL,
    dest text NOT NULL
);

\COPY dec12 (src, dest) FROM '2021/dec12.input' DELIMITER '-'
VACUUM ANALYZE dec12;

/* FIRST STAR */

WITH RECURSIVE

conns (src, dest) AS (
    SELECT src, dest FROM dec12
    UNION DISTINCT
    SELECT dest, src FROM dec12
),

map (src, dest, path) AS (
    SELECT src, dest, ARRAY[src]
    FROM conns
    WHERE src = 'start'

    UNION ALL

    SELECT c.src,
           c.dest,
           array_append(m.path, c.src)
    FROM map AS m
    JOIN conns AS c ON c.src = m.dest
    WHERE (c.src <> all (m.path) OR c.src = upper(c.src))
      AND m.src <> 'end'
)

SELECT count(DISTINCT path) AS first_star
FROM map
WHERE src = 'end'
;

/* SECOND STAR */

WITH RECURSIVE

conns (src, dest) AS (
    SELECT src, dest FROM dec12
    UNION DISTINCT
    SELECT dest, src FROM dec12
),

map (src, dest, path, double) AS (
    SELECT src, dest, ARRAY[src], CAST(NULL AS text)
    FROM conns
    WHERE src = 'start'

    UNION ALL

    SELECT c.src,
           c.dest,
           array_append(m.path, c.src),
           CASE WHEN m.double IS NULL
                 AND c.src = lower(c.src)
                 AND c.src = ANY (m.path)
                THEN c.src
                ELSE m.double
           END
    FROM map AS m
    JOIN conns AS c ON c.src = m.dest
    WHERE (   c.src <> all (m.path)
           OR c.src = upper(c.src)
           OR m.double IS NULL)
      AND m.src <> 'end'
      AND c.dest <> 'start'
)

SELECT count(DISTINCT path) AS second_star
FROM map
WHERE src = 'end'
;
