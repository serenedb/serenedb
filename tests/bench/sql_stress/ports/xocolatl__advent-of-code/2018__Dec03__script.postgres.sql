\set ON_ERROR_STOP true

CREATE TABLE dec03 (
    rownum bigserial,
    input text
);

/* Import the input file.  We're using perl to remove the final newline. */
\COPY dec03 (input) FROM PROGRAM 'perl -pe ''chomp if eof'' data/2018/Dec03/input.dat'
VACUUM ANALYZE dec03;


/* Part One */
WITH
inputs (id, x1, y1, x2, y2) AS (
    SELECT m[1]::integer,
           m[2]::integer,
           m[3]::integer,
           m[2]::integer + m[4]::integer - 1,
           m[3]::integer + m[5]::integer - 1
    FROM dec03,
         LATERAL (SELECT regexp_match(input, '^#(\d+) @ (\d+),(\d+): (\d+)x(\d+)$')) AS lm(m)
)
SELECT count(*)
FROM (
    SELECT 1
    FROM inputs,
         generate_series(x1, x2) AS x,
         generate_series(y1, y2) AS y
    GROUP BY x, y
    HAVING count(*) > 1
) _;

/* Part Two */
WITH
inputs (id, x1, y1, x2, y2) AS (
    /* This CTE is identical to Part One */
    SELECT m[1]::integer,
           m[2]::integer,
           m[3]::integer,
           m[2]::integer + m[4]::integer - 1,
           m[3]::integer + m[5]::integer - 1
    FROM dec03,
         LATERAL (SELECT regexp_match(input, '^#(\d+) @ (\d+),(\d+): (\d+)x(\d+)$')) AS lm(m)
)
SELECT id
FROM inputs AS a
WHERE NOT EXISTS (
    SELECT 1 FROM inputs AS b
    WHERE a.id <> b.id
      AND a.x1 <= b.x2 AND b.x1 <= a.x2
      AND a.y1 <= b.y2 AND b.y1 <= a.y2
    );
