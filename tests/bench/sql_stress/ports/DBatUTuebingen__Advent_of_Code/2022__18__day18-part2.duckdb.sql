SET integer_division = true;
DROP TABLE IF EXISTS fill;
CREATE TABLE fill(x INT, y INT, z INT);

DROP TABLE IF EXISTS fill_all;
CREATE TABLE fill_all(x INT, y INT, z INT);

DROP TABLE IF EXISTS object_filled;
CREATE TABLE object_filled(x INT, y INT, z INT);

DROP TABLE IF EXISTS input;
CREATE TABLE input(x INT, y INT, z INT);

INSERT INTO input
SELECT x :: INT, y :: INT, z :: INT
FROM read_csv_auto('2022/18/input.txt') AS c(x,y,z);

WITH RECURSIVE bounding_box(xmin, xmax, ymin, ymax, zmin, zmax) AS
(
  -- Calculate bounding box of droplet. Add one space of air around.
  SELECT min(x)-1, max(x)+1, min(y)-1, max(y)+1, min(z)-1, max(z)+1
  FROM   input
)
-- This is just a glorified generate_series:
-- SELECT x, y, z
-- FROM   bounding_box AS bb, LATERAL
--        generate_series(xmin, xmax) AS _(x), LATERAL
--        generate_series(ymin, ymax) AS __(y), LATERAL
--        generate_series(zmin, zmax) AS ___(z)
-- Fill the entire bounding box
, bfs(x, y, z) AS
(
  SELECT xmin, ymin, zmin
  FROM   bounding_box

    UNION

  SELECT CASE WHEN b.x + 1 <= xmax THEN b.x+1 ELSE xmin END,
         CASE WHEN b.x + 1 <= xmax THEN b.y   ELSE (CASE WHEN b.y + 1 <= ymax THEN b.y + 1 ELSE ymin END) END,
         CASE WHEN b.x + 1 <= xmax THEN b.z   ELSE (CASE WHEN b.y + 1 <= ymax THEN b.z ELSE (CASE WHEN b.z + 1 <= zmax THEN b.z + 1 ELSE zmax END) END) END
  FROM   bfs AS b, bounding_box AS bb
  WHERE  b.z <= zmax
)
INSERT INTO fill_all
SELECT * FROM bfs;

-- Subtract the droplet from the filled bounding box to create a cavity.
INSERT INTO fill
  SELECT * FROM fill_all
  EXCEPT
  SELECT * FROM input;


WITH RECURSIVE
bounding_box(xmin, xmax, ymin, ymax, zmin, zmax) AS
(
  SELECT min(x), max(x), min(y), max(y), min(z), max(z)
  FROM   fill
),
-- Perform a BFS-search to find all connected blocks on the outside.
bfs2(x, y, z) AS
(
  SELECT min(x), min(y), min(z)
  FROM   fill

    UNION

  SELECT x, y, z
  FROM  (SELECT f.x, f.y, f.z
         FROM   fill AS f, bfs2 AS b
         WHERE  (b.x = f.x - 1 AND b.y = f.y     AND b.z = f.z    )
         OR     (b.x = f.x + 1 AND b.y = f.y     AND b.z = f.z    )
         OR     (b.x = f.x     AND b.y = f.y + 1 AND b.z = f.z    )
         OR     (b.x = f.x     AND b.y = f.y - 1 AND b.z = f.z    )
         OR     (b.x = f.x     AND b.y = f.y     AND b.z = f.z + 1)
         OR     (b.x = f.x     AND b.y = f.y     AND b.z = f.z - 1)
        ) AS _(x, y, z), bounding_box AS bb
  WHERE x >= bb.xmin AND x <= bb.xmax
  AND   y >= bb.ymin AND y <= bb.ymax
  AND   z >= bb.zmin AND z <= bb.zmax
  AND   NOT EXISTS (SELECT 1 FROM input WHERE x = _.x AND y = _.y AND z = _.z)
)
-- Calculate a solid version of the droplet.
INSERT INTO object_filled
SELECT * FROM fill_all
  EXCEPT
SELECT *
FROM   bfs2 AS b;

-- The same as in part one, but performed on the filled droplet.
SELECT (SELECT COUNT(*)*6 FROM object_filled) - COUNT(*)
FROM   object_filled AS i1, object_filled AS i2
WHERE  (ABS(i1.x - i2.x) = 1 AND i1.y = i2.y AND i1.z = i2.z)
OR     (ABS(i1.y - i2.y) = 1 AND i1.x = i2.x AND i1.z = i2.z)
OR     (ABS(i1.z - i2.z) = 1 AND i1.x = i2.x AND i1.y = i2.y);
