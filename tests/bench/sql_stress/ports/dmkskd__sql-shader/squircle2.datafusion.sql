-- Squircle 2 by @XorDev - Converted to DataFusion SQL
-- https://www.shadertoy.com/view/WfySDD
-- @run: resolution=640x480, zoom=1

WITH
pixels AS (
    SELECT
        x,
        y,
        -- Center coordinates and scale to fit vertically
        (CAST(x AS DOUBLE) * 2.0 - 160::DOUBLE) / 120::DOUBLE AS p_x,
        (CAST(y AS DOUBLE) * 2.0 - 120::DOUBLE) / 120::DOUBLE AS p_y
    FROM
        ((SELECT unnest(range(120::INT)) AS y)
         CROSS JOIN (SELECT unnest(range(160::INT)) AS x)) AS p
),
rotated AS (
    SELECT
        x, y, p_x, p_y,
        -- Shortened time variable with rotation
        CAST(1.0 AS DOUBLE) / 4.0 - sin(CAST(1.0 AS DOUBLE)) / 4.0 AS angle
    FROM pixels
),
transformed AS (
    SELECT
        x, y,
        -- Rotate coordinates
        p_x * cos(angle) + p_y * cos(angle + 11.0) AS c_x,
        p_x * cos(angle + 33.0) + p_y * cos(angle) AS c_y
    FROM rotated
),
distance_field AS (
    SELECT
        x, y, c_x, c_y,
        -- Distance field: sqrt of sum of squares
        cos(sqrt(pow(c_x, 4.0) + pow(c_y, 4.0)) / 0.1 - CAST(1.0 AS DOUBLE)) AS l,
        -- Distance for color wave
        sqrt(c_x * c_x + c_y * c_y) AS dist
    FROM transformed
),
color_waves AS (
    SELECT
        x, y, l,
        -- Color wave components
        sin(3.0 * dist + 2.0 * c_y + CAST(1.0 AS DOUBLE) + 6.0) AS w_r,
        sin(3.0 * dist + 2.0 * c_y + CAST(1.0 AS DOUBLE) + 1.0) AS w_g,
        sin(3.0 * dist + 2.0 * c_y + CAST(1.0 AS DOUBLE) + 2.0) AS w_b
    FROM distance_field
),
colors AS (
    SELECT
        x, y,
        -- Tonemap each color channel
        -- DataFusion doesn't have GREATEST/LEAST, use CASE WHEN
        CASE 
            WHEN l / 0.2 > -l THEN tanh((0.6 + 0.5 * w_r) / (l / 0.2))
            ELSE tanh((0.6 + 0.5 * w_r) / (-l))
        END AS r,
        CASE 
            WHEN l / 0.2 > -l THEN tanh((0.6 + 0.5 * w_g) / (l / 0.2))
            ELSE tanh((0.6 + 0.5 * w_g) / (-l))
        END AS g,
        CASE 
            WHEN l / 0.2 > -l THEN tanh((0.6 + 0.5 * w_b) / (l / 0.2))
            ELSE tanh((0.6 + 0.5 * w_b) / (-l))
        END AS b
    FROM color_waves
)
SELECT
    CAST(r AS DOUBLE) AS r,
    CAST(g AS DOUBLE) AS g,
    CAST(b AS DOUBLE) AS b
FROM
    colors
ORDER BY
    y ASC,
    x ASC
