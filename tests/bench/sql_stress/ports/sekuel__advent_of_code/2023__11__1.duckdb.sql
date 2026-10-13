WITH input AS (
    SELECT
        unnest(split(column0, NULL)) AS galaxy,
        row_number() OVER () - 1 AS y,
        generate_subscripts(split(column0, NULL), 1) - 1 AS x,
        unnest(split(column0, NULL)) = '#' AS is_galaxy
    FROM read_csv_auto('./2023/11/input.txt', sep = '', header=false)
), empty_y AS (
    SELECT
        y,
        COUNT(*) FILTER (is_galaxy) = 0 is_empty_y,
    FROM input
    GROUP BY 1
), empty_x AS (
    SELECT
        x,
        COUNT(*) FILTER (is_galaxy) = 0 is_empty_x,
    FROM input
    GROUP BY 1
), shifted_y AS (
    SELECT
        y,
        COUNT(y) FILTER(is_empty_y) OVER (ORDER BY y) AS new_y
    FROM empty_y
), shifted_x AS (
    SELECT
        x,
        COUNT(x) FILTER(is_empty_x) OVER (ORDER BY x) AS new_x
    FROM empty_x
), shifted_galaxies AS (
    SELECT
        y + shifted_y.new_y AS y,
        x + shifted_x.new_x AS x
    FROM input
    JOIN shifted_y USING (y)
    JOIN shifted_x USING (x)
    WHERE is_galaxy
), older_galaxies AS (
    SELECT
        y + shifted_y.new_y * 999999 AS y,
        x + shifted_x.new_x * 999999 AS x
    FROM input
    JOIN shifted_y USING (y)
    JOIN shifted_x USING (x)
    WHERE is_galaxy
)
SELECT 'Part I' AS part, sum(abs(glx1.y - glx2.y) + abs(glx1.x - glx2.x)) // 2 AS answer
FROM shifted_galaxies glx1, shifted_galaxies glx2
UNION ALL
SELECT 'Part II' AS part, sum(abs(glx1.y - glx2.y) + abs(glx1.x - glx2.x)) // 2 AS answer
FROM older_galaxies glx1, older_galaxies glx2;