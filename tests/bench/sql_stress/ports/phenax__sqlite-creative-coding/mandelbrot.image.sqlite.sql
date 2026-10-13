INSERT OR REPLACE INTO images (id, width, height) VALUES ('mandelbrot', 400, 400) RETURNING id;

WITH RECURSIVE
  image AS (SELECT *, 0.008 AS scale, (width * 5)/7 AS ox, (height / 2) AS oy FROM images WHERE id = 'mandelbrot'),
  horizontal(x) AS
    (SELECT width FROM image UNION ALL SELECT x - 1 FROM horizontal WHERE x > 1),
  vertical(y) AS
    (SELECT height FROM image UNION ALL SELECT y - 1 FROM vertical WHERE y > 1),
  mando(i, px, py, cx, cy, x, y) AS (
    SELECT 0, CAST(0.0 AS DOUBLE PRECISION), CAST(0.0 AS DOUBLE PRECISION),
      (SELECT (horizontal.x - ox)*scale FROM image), (SELECT (vertical.y - oy)*scale FROM image),
      horizontal.x, vertical.y
    FROM vertical, horizontal
    UNION ALL
    SELECT i + 1, px*px - py*py + cx, 2.0*px*py + cy, cx, cy, x, y FROM mando
      WHERE (px*px + py*py) < 4.0 AND i < 20
  ),
  mandogrouped(x, y, i) AS (SELECT x, y, max(i) FROM mando GROUP BY x, y),
  colors(i, r, g, b) AS (
    SELECT 19, 255, 150, 150 UNION ALL
    SELECT 10, 150, 120, 120 UNION ALL
    SELECT 7, 50,  100, 100 UNION ALL
    SELECT 0 , 0,   50,  50
  ),
  _pixels(x, y, r, g, b) AS (
    SELECT x, y, c.r, c.g, c.b
      FROM mandogrouped g
      JOIN colors c ON c.i = (SELECT max(i) FROM colors WHERE i <= g.i)
  )
INSERT INTO pixels (image_id, x, y, r, g, b) SELECT 'mandelbrot', x, y, r, g, b FROM _pixels;
