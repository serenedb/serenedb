SET integer_division = true;
-- AoC 2022, Day 9 (Part 1)

CREATE MACRO follow(t,Δx,Δy) AS CASE WHEN abs(Δx) > 1 OR abs(Δy) > 1
                                     THEN {x:t.x+sign(Δx), y:t.y+sign(Δy)}
                                     ELSE t
                                END;

-- Runs in about 226 seconds on my MacBook Pro (M1 Pro)
-- See the variant at ⁂ for a *much* faster query in which table
-- H is materialized before it is fed into the CTE T (⁂ appears
-- to be a performance bug in DuckDB 0.6.0).

-- WITH RECURSIVE
-- input(row, dist, dir) AS (
--   SELECT ROW_NUMBER() OVER () AS row, unnest(range(c.dist)) AS dist, c.dir
--   FROM   read_csv_auto('2022/9/input.txt', SEP=' ', header=false) AS c(dir,dist)
-- ),
-- -- relative movements of head H
-- head(move, Δx, Δy) AS (
--   SELECT ROW_NUMBER() OVER (ORDER BY i.row, i.dist) AS move, d.Δx, d.Δy
--   FROM   input AS i NATURAL JOIN (VALUES ('L',-1, 0),
--                                          ('R', 1, 0),
--                                          ('U', 0,-1),
--                                          ('D', 0, 1)) AS d(dir,Δx,Δy)
-- ),
-- -- absolute positions of head H
-- H(move, x, y)  AS (
--   SELECT h.move, SUM(h.Δx) OVER moves AS x, SUM(h.Δy) OVER moves AS y
--   FROM   head AS h
--   WINDOW moves AS (ORDER BY h.move)
-- ),
-- -- tail T follows H
-- T(move, xy) AS (
--   SELECT 0 AS move, {x:0, y:0} AS xy
--     UNION ALL
--   SELECT H.move, follow(T.xy, H.x - T.xy.x, H.y - T.xy.y) AS xy
--   FROM   T, H
--   WHERE  T.move + 1 = H.move
-- )
-- SELECT COUNT(DISTINCT T.xy) AS visited
-- FROM   T;

/*

APL code that inspired this solution:

--                                       follow   dx,dy
--                                    ┌───────────┐┌─┐
-- follows ← {p ← 0 0 ⋄ (⊂0 0),{p+←(××(1⍨<⌈/⍤|))⍵-p ⋄ p}¨⍵}
-- argument of follows is sequence of positions for the head H

1. compute absolute positions of H (SUM SCAN over moves), starting from (0,0)
2. start with current T position (tx,ty) = (0,0)
3. iterate over positions (hx,hy) in H:
   -- compute difference (dx,dy) between (hx,hy) and (tx,ty)
   -- new current position of T (tx,ty) = (tx,ty) + follow(dx,dy)

*/


-----------------------------------------------------------------------
-- ⁂ Variant query that materializes H and thus appears to
--    avoid a performance bug in DuckDB 0.6.0

-- Runs in about 4 seconds on my MacBook Pro (M1 Pro)

CREATE TEMPORARY TABLE H AS
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

WITH RECURSIVE
  -- tail T follows H
  T(move, xy) AS (
    SELECT 0 AS move, {x:0, y:0} AS xy
      UNION ALL
    SELECT H.move, follow(T.xy, H.xy.x - T.xy.x, H.xy.y - T.xy.y) AS xy
    FROM   T, H
    WHERE  T.move + 1 = H.move
  )
  SELECT COUNT(DISTINCT T.xy) AS visited
  FROM   T;
