INSERT OR REPLACE INTO images (id, width, height) VALUES ('circle', 400, 400) RETURNING id;

WITH RECURSIVE
  image AS (SELECT * FROM images WHERE id = 'circle'),
  circle AS (SELECT width/2 AS cx, height/2 AS cy, 160 AS radius FROM image),
  horizontal(x) AS
    (SELECT width FROM image UNION ALL SELECT x - 1 FROM horizontal WHERE x > 1),
  vertical(y) AS
    (SELECT height FROM image UNION ALL SELECT y - 1 FROM vertical WHERE y > 1),
  _pixels(x, y, r, g, b) AS (SELECT
    x, y,
    150,
    255 * GREATEST(0, LEAST(1, (SELECT (x - cx)*(x - cx) + (y - cy)*(y - cy) - radius*radius FROM circle))),
    150
    FROM vertical, horizontal
  )
INSERT INTO pixels (image_id, x, y, r, g, b) SELECT 'circle', x, y, r, g, b FROM _pixels;
