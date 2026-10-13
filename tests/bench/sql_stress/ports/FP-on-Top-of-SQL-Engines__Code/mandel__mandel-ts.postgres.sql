-- Render an ASCII art approximation of the Mandelbrot Set
--
-- This has been directly adapted from SQLite's "Outlandish Recursive Query Examples"
-- found at https://www.sqlite.org/lang_with.html


-----------------------------------------------------------------------
-- Translation into a UDF using trampolined style
-- SQL UDF to approximate the Mandelbrot Set at point (cx, cy)
--
DROP FUNCTION IF EXISTS mTS(int, float, float, float, float);

CREATE FUNCTION mTS(iter int, cx float, cy float, x float, y float) RETURNS int AS
$$
WITH RECURSIVE tramp(fn, iter, cx, cy, x, y) AS (
  SELECT 'm', iter, cx, cy, x, y
    UNION ALL
  SELECT _.*
  FROM tramp AS t, LATERAL
          (SELECT 'm', t.iter + 1, t.cx, t.cy,
                   t.x^2 - t.y^2 + t.cx,
                   2.0 * t.x * t.y + t.cy
          WHERE t.fn = 'm' AND (t.x^2 + t.y^2 < 4.0 AND t.iter < 28)
            UNION ALL
          SELECT 'finish', t.iter, t.cx, t.cy, t.x, t.y
          WHERE t.fn = 'm' AND NOT (t.x^2 + t.y^2 < 4.0 AND t.iter < 28)
          ) AS _
) SELECT t.iter FROM tramp AS t WHERE t.fn = 'finish';
$$
LANGUAGE SQL IMMUTABLE STRICT;

-- Resolution in pixels on the y axis (N ⩾ 5 for sensible results)


-- Define regions on x/y axes and approximate the Mandelbrot Set in
-- the resulting x/y area
--
WITH
xaxis(x) AS (
  SELECT i AS x
  FROM   generate_series(-2.0, 1.2, (1.2 - (-2.0)) / (3 * :N)) AS i
),
yaxis(y) AS (
  SELECT i AS y
  FROM   generate_series(-1.0, 1.0, (1.0 - (-1.0)) / :N) AS i
),
m2(iter, cx, cy) AS (
  SELECT mTS(0, x, y, 0.0, 0.0) AS iter, x AS cx, y AS cy
  FROM   xaxis AS x, yaxis AS y
),
-- Render the result by approximating the function results
-- using ASCII characters of increasing densit.y of "ink"
--
a(cy, t) AS (
  SELECT cy, string_agg(substr(' .+*#', 1 + LEAST(iter / 7, 4), 1), NULL ORDER BY cx) AS t
  FROM   m2
  GROUP BY cy
)
SELECT t
FROM   a
ORDER BY cy;
