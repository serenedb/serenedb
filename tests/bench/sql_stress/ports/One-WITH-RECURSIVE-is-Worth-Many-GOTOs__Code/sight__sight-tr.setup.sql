\set MODE RECURSIVE
\set invocations 8
\set iterations 8
-- Given a set of obstacle polygons in 2D, compute the lighted polygon
-- from a specified light source point in the scene
--
-- Based on https://ncase.light/sight-and-light/

-- # of visibility computations (~ invocations)
\set N :invocations
\set OBSTACLES :iterations
\set WIDTH  (100 * :iterations)
\set HEIGHT (100 * :iterations)

-- Arithmetic on points
-- Custom operations on PostgreSQL's types point and box
--
-- • equality, comparisons
-- • B+tree ops for types point and box
-- • scalar multiplication & division (from left and right)
-- • AVG(point), SUM(point)


-- =(point,point) -> boolean
DROP OPERATOR IF EXISTS =(point, point) CASCADE;
CREATE OPERATOR =(leftarg = point, rightarg = point, procedure = point_eq, commutator = =);

-- <(point,point) -> boolean
DROP OPERATOR IF EXISTS <(point, point) CASCADE;
DROP FUNCTION IF EXISTS point_lt(point, point);
CREATE FUNCTION point_lt(p1 point, p2 point) RETURNS boolean AS
$$
  SELECT p1[0] < p2[0] OR p1[0] = p2[0] AND p1[1] < p2[1];
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR <(leftarg = point, rightarg = point, procedure = point_lt, commutator = >);

-- >(point,point) -> boolean
DROP OPERATOR IF EXISTS >(point, point) CASCADE;
DROP FUNCTION IF EXISTS point_gt(point, point);
CREATE FUNCTION point_gt(p1 point, p2 point) RETURNS boolean AS
$$
  SELECT p1[0] > p2[0] OR p1[0] = p2[0] AND p1[1] > p2[1];
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR >(leftarg = point, rightarg = point, procedure = point_gt, commutator = <);

-- <=(point,point) -> boolean
DROP OPERATOR IF EXISTS <=(point, point) CASCADE;
DROP FUNCTION IF EXISTS point_le(point, point);
CREATE FUNCTION point_le(p1 point, p2 point) RETURNS boolean AS
$$
  SELECT p1 < p2 OR p1 = p2;
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR <=(leftarg = point, rightarg = point, procedure = point_le, commutator = >=);

-- >=(point,point) -> boolean
DROP OPERATOR IF EXISTS >=(point, point) CASCADE;
DROP FUNCTION IF EXISTS point_ge(point, point);
CREATE FUNCTION point_ge(p1 point, p2 point) RETURNS boolean AS
$$
  SELECT p1 > p2 OR p1 = p2;
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR >=(leftarg = point, rightarg = point, procedure = point_ge, commutator = <=);

-- comparison for points
DROP FUNCTION IF EXISTS point_cmp(point, point);
CREATE FUNCTION point_cmp(p1 point, p2 point) RETURNS int AS
$$
  SELECT CASE WHEN p1 = p2 THEN 0
              WHEN p1 < p2 THEN -1
              ELSE  1
         END;
$$
LANGUAGE SQL IMMUTABLE;

-- hash function for points
DROP FUNCTION IF EXISTS point_hash(point);
CREATE FUNCTION point_hash(p point) RETURNS int AS
$$
  SELECT CASE WHEN p[0] >= p[1]
              THEN (p[0]^2 + p[0] + p[1]) :: int
              ELSE (p[0] + p[1]^2) :: int
         END;
$$ LANGUAGE SQL IMMUTABLE;

-- points in B+-trees
DROP OPERATOR CLASS IF EXISTS point_ops USING btree;
CREATE OPERATOR CLASS point_ops DEFAULT FOR TYPE point USING btree AS
  operator 1 <,
  operator 2 <=,
  operator 3 =,
  operator 4 >=,
  operator 5 >,
  function 1 point_cmp(point, point);

-- point in hash tables (WITH RECURSIVE ... UNION ...)
DROP OPERATOR CLASS IF EXISTS point_hash_ops USING hash;
CREATE OPERATOR CLASS point_hash_ops DEFAULT FOR TYPE point USING hash AS
  operator 1 =,
  function 1 point_hash(point);

-- comparison for boxes
DROP FUNCTION IF EXISTS box_cmp(box, box) CASCADE;
CREATE FUNCTION box_cmp(b1 box, b2 box) RETURNS int AS
$$
  SELECT CASE WHEN b1 = b2 THEN 0
              WHEN b1 < b2 THEN -1
              ELSE  1
         END;
$$
LANGUAGE SQL IMMUTABLE;

-- boxes in B+-trees
DROP OPERATOR CLASS IF EXISTS box_ops USING btree;
CREATE OPERATOR CLASS box_ops DEFAULT FOR TYPE box USING btree AS
  operator 1 <,
  operator 2 <=,
  operator 3 =,
  operator 4 >=,
  operator 5 >,
  function 1 box_cmp(box, box);

-----------------------------------------------------------------------

-- *(numeric,point) -> point (scalar multiplication from the left)
DROP OPERATOR IF EXISTS *(numeric,point) CASCADE;
DROP FUNCTION IF EXISTS smult(numeric, point);
CREATE FUNCTION smult(s numeric, p point) RETURNS point AS
$$
  SELECT point(s * p[0], s * p[1]);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR *(leftarg = numeric, rightarg = point, procedure = smult, commutator = *);

-- *(float,point) -> point (scalar multiplication from the left)
DROP OPERATOR IF EXISTS *(float,point) CASCADE;
DROP FUNCTION IF EXISTS smult(float, point);
CREATE FUNCTION smult(s float, p point) RETURNS point AS
$$
  SELECT point(s * p[0], s * p[1]);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR *(leftarg = float, rightarg = point, procedure = smult, commutator = *);

-- *(point,numeric) -> point (scalar multiplication from the right)
DROP OPERATOR IF EXISTS *(point,numeric) CASCADE;
DROP FUNCTION IF EXISTS smult(point, numeric);
CREATE FUNCTION smult(p point, s numeric) RETURNS point AS
$$
  SELECT point(s * p[0], s * p[1]);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR *(leftarg = point, rightarg = numeric, procedure = smult, commutator = *);

-- *(point,float) -> point (scalar multiplication from the right)
DROP OPERATOR IF EXISTS *(point,float) CASCADE;
DROP FUNCTION IF EXISTS smult(point, float);
CREATE FUNCTION smult(p point, s float) RETURNS point AS
$$
  SELECT point(s * p[0], s * p[1]);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR *(leftarg = point, rightarg = float, procedure = smult, commutator = *);

-- /(point,numeric) -> point (scalar division from the right)
DROP OPERATOR IF EXISTS /(point,numeric) CASCADE;
DROP FUNCTION IF EXISTS sdiv(point, numeric);
CREATE FUNCTION sdiv(p point, s numeric) RETURNS point AS
$$
  SELECT point(p[0] / s, p[1] / s);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR /(leftarg = point, rightarg = numeric, procedure = sdiv);

-- /(point,float) -> point (scalar division from the right)
DROP OPERATOR IF EXISTS /(point,float) CASCADE;
DROP FUNCTION IF EXISTS sdiv(point, float);
CREATE FUNCTION sdiv(p point, s float) RETURNS point AS
$$
  SELECT point(p[0] / s, p[1] / s);
$$
LANGUAGE SQL IMMUTABLE;
CREATE OPERATOR /(leftarg = point, rightarg = float, procedure = sdiv);


-----------------------------------------------------------------------

-- AVG(group(point)) -> point
DROP AGGREGATE IF EXISTS AVG(point) CASCADE;

-- AVG (point) state
DROP TYPE IF EXISTS point_avg_type CASCADE;
CREATE TYPE point_avg_type AS (
    x double precision,
    y double precision,
    n integer
);

-- AVG(point) state transition
DROP FUNCTION IF EXISTS point_avg_accum(point_avg_type, point);
CREATE FUNCTION point_avg_accum(current point_avg_type, next point) RETURNS point_avg_type  AS
$$
  SELECT current.x + next[0], current.y + next[1], current.n + 1;
$$
LANGUAGE SQL IMMUTABLE;

-- AVG(point) finalizer
DROP FUNCTION IF EXISTS point_avg(point_avg_type);
CREATE FUNCTION point_avg(result point_avg_type) RETURNS point AS
$$
  SELECT point(result.x/result.n, result.y/result.n);
$$
LANGUAGE SQL IMMUTABLE;

-- AVG(point)
CREATE AGGREGATE AVG(point) (
  sfunc     = point_avg_accum,
  stype     = point_avg_type,
  finalfunc = point_avg,
  initcond  = '(0,0,0)'
);


-----------------------------------------------------------------------

-- SUM(group(point)) -> point
DROP AGGREGATE IF EXISTS SUM(point) CASCADE;
CREATE AGGREGATE SUM(point) (
  sfunc       = point_add,
  combinefunc = point_add,
  stype       = point,
  initcond    = '(0,0)'
);

SELECT setseed(0.42);

-- Auxiliary functions: convert between polygon and arrays of points
-- (PostgreSQL really should have these built in).
DROP FUNCTION  IF EXISTS points(polygon);
CREATE FUNCTION points(p polygon) RETURNS point[] AS
$$
  SELECT array_agg(point('(' || pt.p || ')') ORDER BY pt.pos)
  FROM   unnest(regexp_split_to_array(rtrim(ltrim(p :: text, '('), ')'), '\),\(')) WITH ORDINALITY AS pt(p,pos);
$$
LANGUAGE SQL STABLE STRICT;

DROP FUNCTION IF EXISTS polygon(point[]);
CREATE FUNCTION polygon(pts point[]) RETURNS polygon AS
$$
  SELECT polygon('(' || replace(rtrim(ltrim(pts :: text, '{'), '}'), '"', '') || ')');
$$
LANGUAGE SQL STABLE STRICT;

-- New type: ray with origin and direction
DROP TYPE IF EXISTS ray CASCADE;
CREATE TYPE ray AS (
  orig point,  -- origin of ray
  dir  point   -- direction of ray
);

-- Ray pointing from p1 towards p2
DROP FUNCTION IF EXISTS ray(point,point);
CREATE FUNCTION ray(p1 point, p2 point) RETURNS ray AS
$$
  SELECT (p1, p2 - p1) :: ray;
$$
LANGUAGE SQL STABLE STRICT;

-- Intersection of ray r with line seglightnt s (NULL if no intersection)
DROP FUNCTION IF EXISTS intersection(ray, lseg);
CREATE FUNCTION intersection(r ray, s lseg) RETURNS point AS
$$
  SELECT CASE
    WHEN lseg(r.orig, r.orig + r.dir) ?|| s THEN NULL
    ELSE (SELECT CASE WHEN t1 > 0 AND t2 BETWEEN 0 AND 1 THEN r.orig + t1 * r.dir
                      ELSE NULL
                 END
          FROM   (VALUES (r.orig[0], r.orig[1], r.dir[0], r.dir[1],
                          (s[0])[0], (s[0])[1], (s[1] - s[0])[0], (s[1] - s[0])[1])) AS _(r_px, r_py, r_dx, r_dy, s_px, s_py, s_dx, s_dy),
                 LATERAL (VALUES ((r_dx*(s_py-r_py) + r_dy*(r_px-s_px))/(s_dx*r_dy - s_dy*r_dx))) AS __(t2),
                 LATERAL (VALUES ((s_px+s_dx*t2-r_px)/r_dx)) AS ___(t1))
  END;
$$
LANGUAGE SQL STABLE STRICT;
DROP OPERATOR IF EXISTS #(ray, lseg);
CREATE OPERATOR #(FUNCTION = intersection, LEFTARG = ray, RIGHTARG = lseg);

-----------------------------------------------------------------------

-- Scene (obstacle polygons)
DROP TABLE IF EXISTS scene;
CREATE TABLE scene (
  id    int GENERATED ALWAYS AS IDENTITY,
  color text,    -- SVG fill color of polygon
  poly  polygon
);


-- Generate random scene
INSERT INTO scene(color, poly) VALUES
  ('none', polygon(box(point(0,0), point(:WIDTH, :HEIGHT))));  -- scene border containing all obstacles

INSERT INTO scene(color, poly)
  SELECT 'lightgrey' AS color,
         polygon(box(point(GREATEST(x - 10, 0),
                           GREATEST(y - 10, 0)),
                     point(LEAST(x + 10 + random() * 100, :WIDTH),
                           LEAST(y + 10 + random() * 100, :HEIGHT)))) AS poly
  FROM   generate_series(1, :OBSTACLES) AS i,
         LATERAL (VALUES (random() * :WIDTH - i + i, random() * :HEIGHT - i + i)) AS _(x,y);

-----------------------------------------------------------------------

