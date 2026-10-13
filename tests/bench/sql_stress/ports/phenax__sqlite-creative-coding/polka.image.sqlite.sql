INSERT OR REPLACE INTO images (id, width, height) VALUES ('polka', 400, 400) RETURNING id;

WITH RECURSIVE
  image AS (SELECT *, width/10 AS gapx, height/10 AS gapy, 16.0 AS size FROM images WHERE id = 'polka'),
  horizontal(x) AS
    (SELECT width FROM image UNION ALL SELECT x - 1 FROM horizontal WHERE x > 1),
  vertical(y) AS
    (SELECT height FROM image UNION ALL SELECT y - 1 FROM vertical WHERE y > 1),
  dotsx(dotx, "column") AS (
    SELECT 0, 0 UNION ALL
    SELECT dotx + gapx, "column" + 1 FROM dotsx, image WHERE dotx < width
  ),
  dotsy(doty, row) AS (
    SELECT 0, 0 UNION ALL
    SELECT doty + gapy, row + 1 FROM dotsy, image WHERE doty < height
  ),
  _pixels(x, y, r, g, b) AS (
    SELECT x, y,
      IFNULL((SELECT 60 * abs((row % 5) - ("column" % 5)) FROM dotsx, dotsy
        WHERE POW(x - dotx - gapx*(row%2)/2.0, 2) + POW(y - doty, 2) < size*size LIMIT 1), 0),
      50, 70
    FROM vertical, horizontal, image
  )
INSERT INTO pixels (image_id, x, y, r, g, b) SELECT 'polka', x, y, r, g, b FROM _pixels;
