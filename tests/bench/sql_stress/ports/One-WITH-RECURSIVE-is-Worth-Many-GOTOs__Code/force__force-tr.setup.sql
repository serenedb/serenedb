\set MODE RECURSIVE
\set invocations 1024
\set iterations 1024
-- PL/SQL-based Barnes-Hut N-Body Simulation
-- (for a static set of bodies, separated by walls)
--
-- Uses SQL (WITH RECURSIVE) to compute the Barnes-Hut quad-tree
-- in table barneshut(node,bbox,parent,mass,center)
-- in which each quadrant is decorated with is total mass (mass)
-- and center point of mass (center). Points on opposite sides
-- of a wall do not affect each other.
--
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


-- Overall containing area for all bodies
\set area box(point(0,0), point(1000,1000))


-- Number of bodies
\set N :iterations
SELECT setseed(0.42);

DROP TABLE IF EXISTS bodies CASCADE;
CREATE TABLE bodies (pos point, mass float);

INSERT INTO bodies(pos, mass)
  SELECT point(width(:area) * random(),
               width(:area) * random()) AS pos,
         GREATEST(1.0, 10 * random()) AS mass
  FROM   generate_series(1, :N);

-- Number of walls
\set W :N / 100

-- Populate area with W random walls
SELECT setseed(0.42);

DROP TABLE IF EXISTS walls CASCADE;
CREATE TABLE walls (wall line);

INSERT INTO walls(wall)
  SELECT line(point(width(:area) * random(), width(:area) * random()),
              point(width(:area) * random(), width(:area) * random()))
  FROM   generate_series(1, :W);



-- Table barneshut(node, bbox, parent, mass, center) stores
-- the quad-tree.  Leaf nodes represent the bodies themselves.
--
-- node                     id of internal quad-tree node (NULL for bodies ≡ leaves)
-- bbox (box)               bounding box enclosed by this node (leaf: encloses body only)
-- parent (int)             parent node
-- mass (float)             aggregate mass of subtree below node (leaf: mass of body)
-- center (point)           weigthed center of mass of subtree below node (leaf: body position)

DROP TABLE IF EXISTS barneshut;
CREATE TABLE barneshut AS
  -- Build complete quad-tree for :area
  WITH RECURSIVE
    -- ➊ Construct inner nodes of quad-tree (top-down, starting from the root)
    skeleton(node, bbox, parent) AS (
      SELECT 0 AS node, :area AS bbox, NULL :: int AS parent
        UNION ALL
      SELECT n.*
      FROM   skeleton AS s,
             LATERAL (VALUES (center(s.bbox), width(s.bbox) / 2)) AS _(c,w),
             LATERAL (VALUES (s.node * 4 + 1, box(c, c + point( w, w)), s.node),
                             (s.node * 4 + 2, box(c, c + point(-w, w)), s.node),
                             (s.node * 4 + 3, box(c, c + point( w,-w)), s.node),
                             (s.node * 4 + 4, box(c, c + point(-w,-w)), s.node)) AS n(node,bbox,parent)
      -- create quad-tree node only if it indeed hosts more than one body
      WHERE (SELECT COUNT(*) FROM bodies AS b WHERE b.pos <@ n.bbox) >= 2
    ),
    -- ➋ Add bodies as quad-tree leaves (hanging off the inner nodes covering minimal area)
    quadtree(node, bbox, parent, mass) AS (
      SELECT s.node, s.bbox, s.parent, NULL :: float AS mass
      FROM   skeleton AS s
        UNION ALL                                           -- ⚠️ not recursive
      SELECT NULL AS node, box(b.pos, b.pos) AS bbox,
             (SELECT s.node
              FROM   skeleton AS s
              WHERE  b.pos <@ s.bbox
              -- if two bounding boxes overlap, place body b in node s with smaller ID
              ORDER BY area(s.bbox), s.node
              LIMIT 1) AS parent,
             b.mass AS mass
      FROM   bodies AS b
    ),
    -- ➌ Annotate all quad-tree nodes with their total mass and centre of mass (bottom-up)
    barneshut(node, bbox, parent, mass, center) AS (
      SELECT q.node, q.bbox, q.parent,
             q.mass,
             center(q.bbox) AS center
      FROM   quadtree AS q
      WHERE  q.node IS NULL      -- ≡ is q a leaf?
        UNION ALL
      SELECT DISTINCT ON (q.node) q.node, q.bbox, q.parent,
             SUM(b.mass) OVER (PARTITION BY q.node) AS mass,  -- ─────────────────────────────────── ≡ mass
             SUM(b.mass * b.center) OVER (PARTITION BY q.node) / SUM(b.mass) OVER (PARTITION BY q.node) AS center -- ⧆
      FROM   quadtree AS q, barneshut AS b
      WHERE  q.node = b.parent
    )
    SELECT b.node, b.bbox, b.parent,
           SUM(b.mass) AS mass,
           SUM(b.mass * b.center) / SUM(b.mass) AS center -- ⧆
    FROM   barneshut AS b
    GROUP BY b.node, b.bbox, b.parent, center(b.bbox);
    --                                      🠵
    --              additional grouping criterion to differentiate
    --              between leaf bounding boxes below the same parent
    --              (PostgreSQL considers any two boxes of width 0
    --               to be equal, no matter what their center point is)

-----------------------------------------------------------------------

DROP INDEX IF EXISTS barneshut_parent;
CREATE INDEX barneshut_parent ON barneshut USING btree (parent);
ANALYZE barneshut;

