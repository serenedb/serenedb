SET integer_division = true;
-- AoC 2022, Day 9 (Part 2)

CREATE MACRO follow(t,Δx,Δy) AS CASE WHEN abs(Δx) > 1 OR abs(Δy) > 1
                                     THEN {x:t.x+sign(Δx), y:t.y+sign(Δy)}
                                     ELSE t
                                END;

-- Runs in about 9 × 4 seconds on my MacBook Pro (M1 Pro)

CREATE TEMP TABLE H AS
  WITH
  input(row, dist, dir) AS (
    SELECT ROW_NUMBER() OVER () AS row, unnest(range(c.dist)) AS dist, c.dir
    FROM   read_csv_auto('2022/9/input.txt', SEP=' ', header=false) AS c(dir,dist)
  ),
  -- relative movements of head H
  head(move, Δx, Δy) AS (
    SELECT ROW_NUMBER() OVER (ORDER BY i.row, i.dist) AS move, d.Δx, d.Δy
    FROM   input AS i NATURAL JOIN (VALUES ('L',-1, 0),
                                           ('R', 1, 0),
                                           ('U', 0,-1),
                                           ('D', 0, 1)) AS d(dir,Δx,Δy)
  )
  -- absolute positions of head H
  SELECT h.move, {x:SUM(h.Δx) OVER moves, y:SUM(h.Δy) OVER moves} AS xy
  FROM   head AS h
  WINDOW moves AS (ORDER BY h.move);

-- Now iterate the follow computation (see Part 1, CTE T) 9 times...
-- (TODO: reformulate this in PostgreSQL using nested CTEs?)

CREATE TEMP TABLE knot1 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot2 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot1 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot3 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot2 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot4 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot3 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot5 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot4 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot6 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot5 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot7 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot6 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

CREATE TEMP TABLE knot8 AS
  WITH RECURSIVE
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, knot7 AS H
    WHERE  T.move + 1 = H.move
  )
  TABLE T;

-- Part 2: knot 9 is the tail T
WITH RECURSIVE
T(move, xy) AS (
  SELECT 0 AS move, {x:0, y:0} AS xy
    UNION ALL
  SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
  FROM   T, knot8 AS H
  WHERE  T.move + 1 = H.move
)
SELECT COUNT(DISTINCT T.xy) AS visited
FROM   T;
