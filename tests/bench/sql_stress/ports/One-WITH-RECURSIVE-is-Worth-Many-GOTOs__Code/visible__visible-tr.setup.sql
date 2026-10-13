\set MODE RECURSIVE
\set invocations 64
\set iterations 64
-- PL/SQL-based visibility of a point there from point here in a
-- 3D landscape modelled by a Bezier surface.  Based on a MAX angle
-- scan along a line from here to there.

-- Define aggregates, operations, comparisons on PostgreSQL points
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

-- # of visibility tests (~ invocations)
\set N :invocations
\set RESOLUTION :iterations
\set X 10
\set Y 10
DROP TABLE IF EXISTS controlp;
CREATE TABLE controlp (
  x int,
  y int,
  z double precision,
  PRIMARY KEY (x,y)
);

-- All coordinates (x,y) not explicitly defined here
-- are assumed to be at height 0

-- Generate control points for a wavy surface using a 2D sin/cos parametric curve
INSERT INTO controlp(x,y,z)
  SELECT x, y,
         round(2 * cos(2 * (pi()/(:X - 1)) * x) * sin((pi()/(:Y - 1)) * y)) AS z
  FROM   generate_series(0, :X - 1) AS x,
         generate_series(0, :Y - 1) AS y;

-- INSERT INTO controlp(x,y,z) VALUES
--   (1, 1, 2),
--   (1, 2, 2),
--   (2, 1, 2),
--   (2, 2, 2);

/*
Can use the following Mathematica snippet to visualize the 3D landscape:

  pts = {{{0, 0, 1}, {0, 1, 0}, {0, 2, 0}, {0, 3, 0}},
         {{1, 0, 0}, {1, 1, 2}, {1, 2, 2}, {1, 3, 0}},
         {{2, 0, 0}, {2, 1, 2}, {2, 2, 2}, {2, 3, 0}},
         {{3, 0, 0}, {3, 1, 0}, {3, 2, 0}, {3, 3, 0}}}

  f = BezierFunction[pts]

  Show[Graphics3D[{PointSize[Medium], Red, Map[Point, pts]}],
       Graphics3D[{Gray, Line[pts], Line[Transpose[pts]]}],
       ParametricPlot3D[f[u, v], {u, 0, 1}, {v, 0, 1}, Mesh -> None]]
*/

