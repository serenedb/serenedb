INSTALL spatial;
LOAD spatial;
WITH _points AS (
    SELECT column0 as x, column1 as y, ROW_NUMBER() OVER() as rowid
    FROM read_csv_auto('09/example.csv', header = false, delim=',')
),
points AS(
    SELECT *
    FROM _points
    UNION ALL
    SELECT *
    FROM _points
    WHERE rowid=1
),
rectangles AS (
     SELECT
        a.rowid AS src,
        b.rowid AS dst,
        ST_Envelope(ST_MakeLine(ST_POINT(a.x, a.y), ST_POINT(b.x, b.y))) as rectangle,
        (abs(a.x - b.x) + 1) * (abs(a.y - b.y) + 1) as size
    FROM points a
    JOIN points b ON a.rowid < b.rowid
),
geo AS (
    SELECT ST_MakeValid(ST_MakeLine(list(ST_POINT(x, y)))) as line_hull
    FROM points
),
tilemap AS (
    SELECT ST_MakePolygon(line_hull) as tiles
    FROM geo
),
part1 AS (
        SELECT MAX(size) as area
        FROM rectangles
),
part2 AS (
    SELECT MAX(size) as area
    FROM tilemap, rectangles
    WHERE ST_Contains(tiles, rectangle)
)
SELECT area, 'part1' as part FROM part1
UNION ALL
SELECT area, 'part2' as part FROM part2
