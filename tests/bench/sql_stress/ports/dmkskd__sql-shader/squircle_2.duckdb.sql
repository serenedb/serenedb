-- "Squircle 2" by @XorDev - Converted to DuckDB-WASM SQL
-- https://www.shadertoy.com/view/WfySDD
-- A lightweight shader with rotating squircle distance field and colorful waves
-- @run: resolution=640x480, zoom=1

WITH
uniforms AS (
  SELECT '{"iResolution.x": 160, "iResolution.y": 120, "iTime": 1.0, "iMouse.x": 80, "iMouse.y": 60}'::JSON AS iUniforms
),
u AS (
  SELECT
    CAST(json_extract(iUniforms, 'iResolution.x') AS BIGINT) AS width,
    CAST(json_extract(iUniforms, 'iResolution.y') AS BIGINT) AS height,
    CAST(json_extract(iUniforms, 'iTime') AS DOUBLE) AS iTime
  FROM uniforms
),
pixels AS (
      SELECT 
        y::BIGINT AS y, 
        x::BIGINT AS x,
        u.iTime,
        -- Center coordinates and scale to fit vertically
        (x::DOUBLE * 2.0 - u.width) / u.height AS p_x,
        (y::DOUBLE * 2.0 - u.height) / u.height AS p_y,
        -- Rotation angle
        u.iTime / 4.0 - sin(u.iTime) / 4.0 AS angle
    FROM 
        (SELECT unnest(generate_series(0, (SELECT height - 1 FROM u))) AS y) ty,
        (SELECT unnest(generate_series(0, (SELECT width - 1 FROM u))) AS x) tx,
        u
),
rotated AS (
    SELECT
        y, x, iTime,
        -- Rotated coordinates using DuckDB's optimized trig functions
        p_x * cos(angle) + p_y * cos(angle + 11.0) AS c_x,
        p_x * cos(angle + 33.0) + p_y * cos(angle) AS c_y
    FROM pixels
),
colors AS (
    SELECT
        y, x, iTime, c_x, c_y,
        -- DuckDB-optimized distance field calculation
        cos(sqrt(pow(c_x, 4) + pow(c_y, 4)) / 0.1 - iTime) AS l
    FROM rotated
)
SELECT
    -- Enhanced color calculation using DuckDB's mathematical functions
    tanh((0.6 + 0.5 * sin(3.0 * sqrt(c_x*c_x + c_y*c_y) + 2.0 * c_y + iTime + 6.0)) / greatest(l / 0.2, -l)) AS r,
    tanh((0.6 + 0.5 * sin(3.0 * sqrt(c_x*c_x + c_y*c_y) + 2.0 * c_y + iTime + 1.0)) / greatest(l / 0.2, -l)) AS g,
    tanh((0.6 + 0.5 * sin(3.0 * sqrt(c_x*c_x + c_y*c_y) + 2.0 * c_y + iTime + 2.0)) / greatest(l / 0.2, -l)) AS b
FROM colors
ORDER BY y, x;