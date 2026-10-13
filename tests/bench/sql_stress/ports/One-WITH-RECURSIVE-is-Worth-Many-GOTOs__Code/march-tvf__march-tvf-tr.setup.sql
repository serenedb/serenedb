\set MODE RECURSIVE
\set invocations 128
\set iterations 128
-- The marching squares algorithm.
--
-- Iterative PL/SQL UDF that wanders the 2D map
-- to detect and track the border of a 2D shape. Returns a table describing a
-- closed path around the shape.
--

-- Marching Squares to trace an isoline (contour line) on a height map
--
-- See https://en.wikipedia.org/wiki/Marching_squares.

-- A vector/point in 2D Cartesian space
DROP TYPE IF EXISTS vec2 CASCADE;
CREATE TYPE vec2 AS (
  x int,
  y int
);

-- # of marches to perform (~ # of invocations)
\set N :invocations
DROP TABLE IF EXISTS map CASCADE;
CREATE TABLE map(xy vec2 PRIMARY KEY, alt int);

-- Load manually crafted maps or ...
-- \i map.sql
-- \i map-small.sql

-- ... compute map based on ellipse in area of given width/height (~ # of iterations)
\set X :iterations
\set Y :iterations
\set STRIPE 8

INSERT INTO map(xy, alt)
  SELECT (x + :STRIPE, y) :: vec2 AS xy,
         -- ellipse with center (:X/2, :Y/2) and radii :X/2-1 and :Y/2-1
         CASE WHEN ((x - :X / 2.0)^2/ (:X / 2.0 - 1)^2) + ((y - :Y / 2.0)^2 / (:Y / 2.0 - 1)^2) <= 1
              THEN 1000
              ELSE 0
         END AS alt
  FROM   generate_series(-:STRIPE, :X) AS x,
         generate_series(       0, :Y) AS y;

-- Tabular encoding of the essence of the marching square algorithm:
-- direct the march based on a 2×2 vicinity of pixels
--
DROP TABLE IF EXISTS directions CASCADE;
CREATE TABLE directions (
  ll       bool,   -- pixel set in the lower left?
  lr       bool,   --                  lower right
  ul       bool,   --                  upper left
  ur       bool,   --                  upper right
  dir      vec2,   -- direction of march
  "track?" bool,   -- are we tracking the shape yet?
  PRIMARY KEY (ll, lr, ul, ur));

INSERT INTO directions(ll, lr, ul, ur, dir, "track?") VALUES
  (false,false,false,false, ( 1, 0) :: vec2, false), -- | | ︎: →
  (false,false,false,true , ( 1, 0) :: vec2, true ), -- |▝| : →
  (false,false,true ,false, ( 0, 1) :: vec2, true ), -- |▘| : ↑
  (false,false,true ,true , ( 1, 0) :: vec2, true ), -- |▀| : →
  (false,true ,false,false, ( 0,-1) :: vec2, true ), -- |▗| : ↓
  (false,true ,false,true , ( 0,-1) :: vec2, true ), -- |▐| : ↓
  (false,true ,true ,false, ( 0, 1) :: vec2, true ), -- |▚| : ↑
  (false,true ,true ,true , ( 0,-1) :: vec2, true ), -- |▜| : ↓
  (true ,false,false,false, (-1, 0) :: vec2, true ), -- |▖| : ←
  (true ,false,false,true , (-1, 0) :: vec2, true ), -- |▞| : ←
  (true ,false,true ,false, ( 0, 1) :: vec2, true ), -- |▌| : ↑
  (true ,false,true ,true , ( 1, 0) :: vec2, true ), -- |▛| : →
  (true ,true ,false,false, (-1, 0) :: vec2, true ), -- |▄| : ←
  (true ,true ,false,true , (-1, 0) :: vec2, true ), -- |▟| : ←
  (true ,true ,true ,false, ( 0, 1) :: vec2, true ), -- |▛| : →
  (true ,true ,true ,true , NULL           , true ); -- |█| : x


-- Generate a thresholded black/white 2D map of pixels
--
DROP TABLE IF EXISTS pixels CASCADE;
CREATE TABLE pixels (
  xy  vec2 PRIMARY KEY,
  alt bool);

INSERT INTO pixels(xy, alt)
  -- Threshold height map based on given iso value (here: > 700)
  SELECT m.xy, m.alt > 700 AS alt
  FROM   map AS m;


-- Generate a 2D map of squares that each aggregate 2×2 adjacent pixel
--
DROP TABLE IF EXISTS squares CASCADE;
CREATE TABLE squares (
  xy vec2 PRIMARY KEY,
  ll bool,
  lr bool,
  ul bool,
  ur bool);

INSERT INTO squares(xy, ll, lr, ul, ur)
  -- Establish 2×2 squares on the pixel-fied map,
  -- (x,y) designates lower-left corner: ul  ur
  --                                       ⬜︎
  --                                     ll  lr
  SELECT p0.xy AS xy,
         p0.alt AS ll, p1.alt AS lr, p2.alt AS ul, p3.alt AS ur
  FROM   pixels p0, pixels p1, pixels p2, pixels p3
  WHERE  p1.xy = ((p0.xy).x + 1, (p0.xy).y + 0) :: vec2
  AND    p2.xy = ((p0.xy).x + 0, (p0.xy).y + 1) :: vec2
  AND    p3.xy = ((p0.xy).x + 1, (p0.xy).y + 1) :: vec2;

analyze squares; analyze directions; analyze map;

