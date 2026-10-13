-- "Interactive Waves" - A pattern that reacts to mouse movement and time.
-- @run: resolution=640x480, zoom=1

WITH
uniforms AS (
  SELECT '{"iResolution.x": 160, "iResolution.y": 120, "iTime": 1.0, "iMouse.x": 80, "iMouse.y": 60}'::JSON AS iUniforms
),
parsed AS (
  SELECT
    CAST(json_extract(iUniforms, 'iResolution.x') AS BIGINT) AS width,
    CAST(json_extract(iUniforms, 'iResolution.y') AS BIGINT) AS height,
    CAST(json_extract(iUniforms, 'iTime') AS DOUBLE) AS iTime,
    CAST(json_extract(iUniforms, 'iMouse.x') AS DOUBLE) AS mx,
    CAST(json_extract(iUniforms, 'iMouse.y') AS DOUBLE) AS my
  FROM uniforms
),
pixels AS (
  SELECT i::DOUBLE AS x, j::DOUBLE AS y
  FROM generate_series(0, (SELECT width - 1 FROM parsed)) AS t(i)
  CROSS JOIN generate_series(0, (SELECT height - 1 FROM parsed)) AS t2(j)
),
colors AS (
  SELECT
    0.5 + 0.5 * sin(sqrt(pow(p.x - u.mx, 2) + pow(p.y - u.my, 2)) * 0.1 - u.iTime * 2.0) AS r,
    0.5 + 0.5 * sin(u.iTime * 1.5) AS g,
    0.5 + 0.5 * cos(sqrt(pow(p.x - u.mx, 2) + pow(p.y - u.my, 2)) * 0.1 - u.iTime * 2.0) AS b,
    x, y
  FROM pixels AS p, parsed AS u
)
SELECT r, g, b FROM colors ORDER BY y, x;