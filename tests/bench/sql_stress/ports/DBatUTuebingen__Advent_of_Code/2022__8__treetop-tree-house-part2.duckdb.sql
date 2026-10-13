SET integer_division = true;
-- AoC 2022, Day 8 (Part 2)

-- minimal distance a↔︎b (or a↔︎c if b is NULL)
CREATE MACRO distance(a,b,c) AS MIN(abs(COALESCE(b,c) - a));

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
dims(w,h) AS (
  SELECT MAX(g.x) AS w, MAX(g.y) AS h
  FROM   grid AS g
),
dists(x, y, dist) AS (
  -- measure distance to trees to the right of tree @ (g.x, g.y) that are
  -- as high or higher (if there is no such tree we can see the grid's full width d.w)
  SELECT g.x, g.y, distance(g.x, g1.x, d.w) AS dist
  FROM   dims AS d, grid AS g LEFT JOIN grid AS g1
         ON g.y = g1.y AND g.x < g1.x AND g.h <= g1.h
  GROUP BY g.x, g.y
    UNION ALL
  -- measure distance to trees to the left
  SELECT g.x, g.y, distance(g.x, g1.x, 1) AS dist
  FROM   grid AS g LEFT JOIN grid AS g1
         ON g.y = g1.y AND g.x > g1.x AND g.h <= g1.h
  GROUP BY g.x, g.y
    UNION ALL
  -- measure distance to trees below
  SELECT g.x, g.y, distance(g.y, g1.y, d.h) AS dist
  FROM   dims AS d, grid AS g LEFT JOIN grid AS g1
         ON g.x = g1.x AND g.y < g1.y AND g.h <= g1.h
  GROUP BY g.x, g.y
    UNION ALL
  -- measure distance to trees above
  SELECT g.x, g.y, distance(g.y, g1.y, 1) AS dist
  FROM   grid AS g LEFT JOIN grid AS g1
         ON g.x = g1.x AND g.y > g1.y AND g.h <= g1.h
  GROUP BY g.x, g.y
)
SELECT PRODUCT(d.dist) :: int AS score
FROM   dists AS d
GROUP BY d.x, d.y
ORDER BY score DESC
LIMIT 1;
