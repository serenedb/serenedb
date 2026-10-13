\set ON_ERROR_STOP true

CREATE TABLE dec10 (
    rownum bigserial,
    input text
);

/* Import the input file.  We're using perl to remove the final newline. */
\COPY dec10 (input) FROM PROGRAM 'perl -pe ''chomp if eof'' data/2018/Dec10/input.dat'
VACUUM ANALYZE dec10;


/* Part One */
WITH RECURSIVE
input (x, y, vx, vy) AS (
    SELECT m[1]::integer,                                                                                              
           m[2]::integer,
           m[3]::integer,
           m[4]::integer
    FROM dec10,
         LATERAL (SELECT regexp_match(input, '^position=<\s*(-?\d+),\s*(-?\d+)> velocity=<\s*(-?\d+),\s*(-?\d+)>$')) AS lm(m)
),
loop (iteration, area) AS (
    SELECT 0, (max(x)-min(x))::bigint*(max(y)-min(y))
    FROM input
    UNION ALL
    SELECT iteration+1, new_area.x
    FROM loop,
         LATERAL (SELECT (max(x+(iteration+1)*vx)-min(x+(iteration+1)*vx))::bigint*(max(y+(iteration+1)*vy)-min(y+(iteration+1)*vy)) FROM input) AS new_area(x)
    WHERE area > new_area.x
),
seconds (s) AS (
    SELECT iteration
    FROM loop
    ORDER BY area
    FETCH FIRST 1 ROW ONLY
),
vectors (x, y) AS (
    SELECT x+s*vx,
           y+s*vy
    FROM input, seconds
)
SELECT string_agg(CASE WHEN EXISTS (SELECT 1 FROM vectors WHERE (x, y) = (i, j)) THEN '#' ELSE ' ' END, '' ORDER BY i) AS result
FROM generate_series((SELECT min(x) FROM vectors), (SELECT max(x) FROM vectors)) AS i,
     generate_series((SELECT min(y) FROM vectors), (SELECT max(y) FROM vectors)) AS j
GROUP BY j
ORDER BY j;

/* Part Two */
WITH RECURSIVE
input (x, y, vx, vy) AS (
    SELECT m[1]::integer,                                                                                              
           m[2]::integer,
           m[3]::integer,
           m[4]::integer
    FROM dec10,
         LATERAL (SELECT regexp_match(input, '^position=<\s*(-?\d+),\s*(-?\d+)> velocity=<\s*(-?\d+),\s*(-?\d+)>$')) AS lm(m)
),
loop (iteration, area) AS (
    SELECT 0, (max(x)-min(x))::bigint*(max(y)-min(y))
    FROM input
    UNION ALL
    SELECT iteration+1, new_area.x
    FROM loop,
         LATERAL (SELECT (max(x+(iteration+1)*vx)-min(x+(iteration+1)*vx))::bigint*(max(y+(iteration+1)*vy)-min(y+(iteration+1)*vy)) FROM input) AS new_area(x)
    WHERE area > new_area.x
)
SELECT iteration
FROM loop
ORDER BY area
FETCH FIRST 1 ROW ONLY;
