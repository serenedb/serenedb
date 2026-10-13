-- Put everything in a schema so its easy to reset. This also makes
-- this automatically reset so we can just run it many times.
DROP SCHEMA IF EXISTS day07 CASCADE;
CREATE SCHEMA day07;

-- Raw input
CREATE TABLE day07.inputs (
  line    TEXT NOT NULL
);

-- Use \COPY rather than COPY so its client-side in psql
\COPY day07.inputs (line) FROM 'day07/input.txt';

\echo 'PART 1 RESULTS';
WITH pos AS (
  -- turn input into row-based ints
  SELECT x::int
  FROM   day07.inputs,
         LATERAL regexp_split_to_table(line, ',') as x(x)
), bounds AS (
  -- find min/max place crabs can move
  SELECT min(x) as min_x, max(x) max_x
  FROM   pos
), targets AS (
  -- for each place they can move, find the fuel cost
  SELECT target, sum(abs(target - x)) as fuel
  FROM   bounds,
         pos,
         LATERAL generate_series(min_x, max_x) as target
  GROUP BY target
)
SELECT   target, fuel as answer
FROM     targets
ORDER BY fuel
LIMIT    1;

\echo 'PART 2 RESULTS';
WITH pos AS (
  -- turn input into row-based ints
  SELECT x::int
  FROM   day07.inputs,
         LATERAL regexp_split_to_table(line, ',') as x(x)
), bounds AS (
  -- find min/max place crabs can move
  SELECT min(x) as min_x, max(x) max_x
  FROM   pos
), targets AS (
  -- for each place they can move, find the distance
  SELECT target, abs(target - x) as distance
  FROM   bounds,
         pos,
         LATERAL generate_series(min_x, max_x) as target
), fuel AS (
  -- calculate fuel, n(n+1)/2 is formula for 1 + 2 + 3 ... + distance
  SELECT target, (distance * (distance + 1) / 2) as fuel
  FROM   targets
)
SELECT   target, sum(fuel) as answer
FROM     fuel
GROUP BY target
ORDER BY answer
LIMIT    1;
