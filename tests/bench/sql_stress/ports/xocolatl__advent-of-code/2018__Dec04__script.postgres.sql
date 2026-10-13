\set ON_ERROR_STOP true

CREATE TABLE dec04 (
    rownum bigserial,
    input text
);

/* Import the input file.  We're using perl to remove the final newline. */
\COPY dec04 (input) FROM PROGRAM 'perl -pe ''chomp if eof'' data/2018/Dec04/input.dat'
VACUUM ANALYZE dec04;


/* Part One */
with
inputs (time, guard, action) AS (
    SELECT m[1]::timestamp without time zone,
           CASE WHEN m[2] ~ '^\d+$' THEN m[2]::integer END,
           CASE WHEN m[2] IN ('up', 'asleep') THEN m[2] END
    FROM dec04,
         LATERAL (SELECT regexp_match(input, '^\[([^]]+)\] .*?(\d+|up|asleep)')) AS lm(m)
),
sleeps (guard, asleep, up) AS (
    SELECT (SELECT _i.guard
            FROM inputs AS _i
            WHERE _i.time <= inputs.time
              AND _i.guard IS NOT null
            ORDER BY _i.time DESC
            FETCH FIRST 1 ROW ONLY) AS guard,
           time AS asleep,
           (SELECT _i.time
            FROM inputs AS _i
            WHERE _i.time >= inputs.time
              AND _i.action = 'up'
            ORDER BY _i.time
            FETCH FIRST 1 ROW ONLY) AS up
    FROM inputs
    WHERE action = 'asleep'
)
SELECT guard * mode() WITHIN GROUP (ORDER BY min)
FROM sleeps,
     generate_series(extract(minute FROM asleep)::integer, extract(minute FROM up)::integer - 1) AS min
GROUP BY guard
ORDER BY count(*) DESC
FETCH FIRST 1 ROW ONLY;

/*
 * Part Two
 *
 * The two CTEs inputs and sleeps are identical to Part One.
 */
with
inputs (time, guard, action) AS (
    SELECT m[1]::timestamp without time zone,
           CASE WHEN m[2] ~ '^\d+$' THEN m[2]::integer END,
           CASE WHEN m[2] IN ('up', 'asleep') THEN m[2] END
    FROM dec04,
         LATERAL (SELECT regexp_match(input, '^\[([^]]+)\] .*?(\d+|up|asleep)')) AS lm(m)
),
sleeps (guard, asleep, up) AS (
    SELECT (SELECT _i.guard
            FROM inputs AS _i
            WHERE _i.time <= inputs.time
              AND _i.guard IS NOT null
            ORDER BY _i.time DESC
            FETCH FIRST 1 ROW ONLY) AS guard,
           time AS asleep,
           (SELECT _i.time
            FROM inputs AS _i
            WHERE _i.time >= inputs.time
              AND _i.action = 'up'
            ORDER BY _i.time
            FETCH FIRST 1 ROW ONLY) AS up
    FROM inputs
    WHERE action = 'asleep'
)
SELECT guard * min
FROM sleeps,
     generate_series(extract(minute FROM asleep)::integer, extract(minute FROM up)::integer - 1) AS min
GROUP BY guard, min
ORDER BY count(*) DESC
FETCH FIRST 1 ROW ONLY;
