SET integer_division = true;

WITH input(x,y,z) AS (
SELECT x :: INT, y :: INT, z :: INT
FROM read_csv_auto('2022/18/input.txt') AS c(x,y,z)
)
SELECT (SELECT COUNT(*)*6 FROM input) - COUNT(*)
FROM   input AS i1, input AS i2
WHERE  (ABS(i1.x - i2.x) = 1 AND i1.y = i2.y AND i1.z = i2.z)
OR     (ABS(i1.y - i2.y) = 1 AND i1.x = i2.x AND i1.z = i2.z)
OR     (ABS(i1.z - i2.z) = 1 AND i1.x = i2.x AND i1.y = i2.y);
