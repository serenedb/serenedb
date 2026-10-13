\set MODE RECURSIVE
\set invocations 32
\set iterations 32
-- PLpg/SQL Ray Tracer
--
-- Derived from obfuscated MySQL version by Nick (holtsetio@gmail.com),
-- see https://files.scene.org/view/parties/2019/deadline19/wild/sqlray.zip

-----------------------------------------------------------------------
-- Conversion of int arrays (int[]) into PostgreSQL byte arrays (bytea)

-- Reverse byte order in byte array (→ little endian)
DROP FUNCTION IF EXISTS little_endian(bytea);
CREATE FUNCTION little_endian(bs bytea) RETURNS bytea AS
$$
  SELECT string_agg(substring(bs, i, 1), NULL ORDER BY i DESC)
  FROM   generate_series(1, octet_length(bs)) AS i;
$$
LANGUAGE SQL IMMUTABLE STRICT;

-- Interpret int array xs as array of bytes/words/double-words (w = 1/2/4)
DROP FUNCTION IF EXISTS bytes(int, int[]);
CREATE FUNCTION bytes(w int, xs int[]) RETURNS bytea AS
$$
  SELECT string_agg(little_endian(decode(lpad(to_hex(x), 2 * w, '0'), 'hex')), NULL ORDER BY i)
  FROM   unnest(xs) WITH ORDINALITY AS _(x,i);
$$
LANGUAGE SQL IMMUTABLE STRICT;

-----------------------------------------------------------------------
-- Generate BMP bitmap image file format (w/ 24 bit RGB pixels)

-- Array pixel is expected to contain h * (3 * w) bytes of r/g/b data
DROP FUNCTION IF EXISTS bmp(int, int, int[]);
CREATE FUNCTION bmp(w int, h int, pixel int[]) RETURNS bytea AS
$$
  SELECT bytes(1, array[ascii('B'), ascii('M')])     -- 'BM'
      || bytes(4, array[26 + h * (3 * w + padding)]) -- length of entire BMP file: 26 bytes header + h * (3 * w padding)
      || bytes(4, array[0, 26, 12])
      --                │   │   └──────────────────  -- header length in bytes (12)
      --                │   └──────────────────────  -- offset of actual pixel data in bytes (26)
      --                └──────────────────────────  -- reserved
      --                                             -- BMP header:
      || bytes(2, array[w, h, 1, 3 * 8])
      --                │  │  │    └──────────────── -- - 24 bits per pixel (r/g/b bytes)
      --                │  │  └───────────────────── -- - # of color planes (1)
      --                │  └──────────────────────── -- - height of bitmap (in pixels)
      --                └─────────────────────────── -- - width of bitmap (in pixels)
      -- pixel data (bytes), add padding to each line
      || (SELECT string_agg(bytes(1, pixel[i:i + 3 * w - 1] || array_fill(0, array[padding])), NULL ORDER BY i)
          FROM   generate_series(1, h * (3 * w), 3 * w) AS i)
  FROM  (VALUES ((4 - (3 * w) % 4) % 4)) AS _(padding); -- pad lines to next multiple of 4 bytes
$$
LANGUAGE SQL IMMUTABLE STRICT;

-- Encode byte array bs in the textual base64 format
DROP FUNCTION IF EXISTS base64(bytea);
CREATE FUNCTION base64(bs bytea) RETURNS text AS
$$
  SELECT translate(encode(bs, 'base64'), E'\n', '');
$$
LANGUAGE SQL IMMUTABLE STRICT;

-----------------------------------------------------------------------
-- Scene

-- A point in 3D space
DROP TYPE IF EXISTS vec3 CASCADE;
CREATE TYPE vec3 AS (x real, y real, z real);

-- RGB triple
DROP TYPE IF EXISTS rgb CASCADE;
CREATE TYPE rgb AS (r real, g real, b real);

-- Material ([m]att, [r]eflective)
DROP TYPE IF EXISTS material CASCADE;
CREATE TYPE material AS ENUM ('m', 'r', 'l', 'n');

DROP TABLE IF EXISTS triangles;
CREATE TABLE triangles (
  id    int GENERATED ALWAYS AS IDENTITY,
  p1    vec3,
  p2    vec3,
  p3    vec3,
  mat   material,
  color rgb        -- color of matt (material = 'm') triangles, otherwise NULL
);

DROP TABLE IF EXISTS spheres;
CREATE TABLE spheres (
  id     int GENERATED ALWAYS AS IDENTITY,
  center vec3,
  radius real,
  mat    material,
  color  rgb        -- color of matt (material = 'm') sphere, otherwise NULL
);

INSERT INTO triangles(p1, p2, p3, mat, color) VALUES
  (( -10.0,  -1.0,-10.0), ( 10.0,  -1.0,-10.0), ( 0.0,-1.0,  5.0), 'm', (0.6 ,0.6,0.6 )),
  (( -10.0,   1.0,-10.0), ( 10.0,   1.0,-10.0), ( 0.0, 1.0,  5.0), 'm', (0.6 ,0.6,0.6 )),
  ((  -1.0, -10.0,-10.0), ( -1.0,  10.0,-10.0), (-1.0, 0.0,  5.0), 'm', (0.85,0.5,0.0 )),
  ((   1.0, -10.0,-10.0), (  1.0,  10.0,-10.0), ( 1.0, 0.0,  5.0), 'm', (0.2 ,0.6,0.75)),
  (( -10.0, -10.0,  1.0), ( 10.0, -10.0,  1.0), ( 0.0, 5.0,  1.0), 'm', (0.6 ,0.6,0.6 )),
  ((-100.0,-100.0,-10.0), (100.0,-100.0,-10.0), ( 0.0,50.0,-10.0), 'm', (0.6 ,0.6,0.6 ));

INSERT INTO spheres(center, radius, mat, color) VALUES
  ((-0.5,-0.6,  0.3), 0.4,  'r', NULL         ),
  (( 0.4,-0.6, -0.6), 0.4,  'r', NULL         ),
  (( 1.0, 0.0,  0.5), 0.25, 'm', (0.2,0.8,0.2)),
  (( 0.0, 0.95, 0.0), 0.35, 'l', NULL         );  -- light source (must be last!)

-----------------------------------------------------------------------

