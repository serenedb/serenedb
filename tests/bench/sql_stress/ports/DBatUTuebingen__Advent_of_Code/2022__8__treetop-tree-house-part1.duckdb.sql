SET integer_division = true;
-- AoC 2022, Day 8 (Part 1)

WITH
input(y, row, x) AS (
  SELECT ROW_NUMBER () OVER ()      AS y,
         string_split(c.line, NULL)   AS row,
         generate_subscripts(string_split(c.line, NULL),1) AS x
  FROM   read_csv_auto('2022/8/input.txt', ALL_VARCHAR=true, SEP='', header=false) AS c(line)
),
grid(x, y, h) AS (
  SELECT i.x, i.y, i.row[i.x] :: int AS h
  FROM   input AS i
),
visibility(x, y, "visible?") AS (
  SELECT g.x, g.y,
         COALESCE(g.h > MAX(g.h) OVER from_left,  true) OR
         COALESCE(g.h > MAX(g.h) OVER from_right, true) OR
         COALESCE(g.h > MAX(g.h) OVER from_top,   true) OR
         COALESCE(g.h > MAX(g.h) OVER from_below, true) AS "visible?"
  FROM   grid AS g
  WINDOW from_left  AS (PARTITION BY g.y ORDER BY g.x ASC  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
         from_right AS (PARTITION BY g.y ORDER BY g.x DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
         from_top   AS (PARTITION BY g.x ORDER BY g.y ASC  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
         from_below AS (PARTITION BY g.x ORDER BY g.y DESC ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
)
SELECT COUNT(*) FILTER (WHERE v."visible?") AS visible
FROM   visibility AS v;
