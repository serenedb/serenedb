INSERT OR REPLACE INTO images (id, width, height) VALUES ('voronoi', 400, 400) RETURNING id;

WITH RECURSIVE
  image AS (SELECT *, 100 AS segments FROM images WHERE id = 'voronoi'),
  horizontal(x) AS
    (SELECT width FROM image UNION ALL SELECT x - 1 FROM horizontal WHERE x > 1),
  vertical(y) AS
    (SELECT height FROM image UNION ALL SELECT y - 1 FROM vertical WHERE y > 1),
  points(px, py, count) AS (
    SELECT 0, 0, 0 UNION ALL
    SELECT abs(CAST(random() * 9223372036854775807 AS BIGINT))%width, abs(CAST(random() * 9223372036854775807 AS BIGINT))%height, count + 1 FROM points, image WHERE count < segments
  ),
  _pixels(x, y, r, g, b) AS (
    SELECT x, y,
      0,
      (SELECT color FROM
        (SELECT (100*(px + 1)*(py + 1)) % 255 AS color,
                POW(x - px, 2) + POW(y - py, 2) as distance
          FROM points ORDER BY distance ASC LIMIT 1)),
      40
    FROM vertical, horizontal, image
  )
INSERT INTO pixels (image_id, x, y, r, g, b) SELECT 'voronoi', x, y, r, g, b FROM _pixels;
