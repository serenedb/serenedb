-----------------------------------------------------------------------
-- Translation into a UDF using trampolined style

DROP TYPE IF EXISTS K;
CREATE TYPE K As (nodes int, s int, e int, s1 int, s2 int, ref int);

DROP FUNCTION IF EXISTS floyd(int, int, int);


CREATE FUNCTION floyd(nodes int, s int, e int) RETURNS int AS
$$
WITH RECURSIVE tramp(fn, nodes, s, e, x, k) AS (
  SELECT 'floyd', nodes, s, e, 0, ARRAY[] :: K[]
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'apply', 0, t.s, t.e,
                  (SELECT edge.weight
                  FROM   edges AS edge
                  WHERE  (edge.here,edge.there) = (t.s,t.e)),
                  t.k
          WHERE t.fn = 'floyd' AND t.nodes = 0
            UNION ALL
          SELECT 'floyd', t.nodes-1, t.s, t.e, t.x,
                 array_prepend((t.nodes, t.s, t.e, 0, 0, 1) :: K, t.k)
          WHERE t.fn = 'floyd' AND t.nodes <> 0
            UNION ALL
          SELECT 'finish', t.nodes, t.s, t.e, t.x, t.k
          WHERE t.fn = 'apply' AND CARDINALITY(t.k) = 0
            UNION ALL
          SELECT 'floyd', t.k[1].nodes - 1, t.k[1].s, t.k[1].nodes, t.x,
                 array_prepend((t.k[1].nodes, t.k[1].s, t.k[1].e, t.x, 0, 2) :: K, t.k[2:])
          WHERE t.fn = 'apply' AND t.k[1].ref = 1
            UNION ALL
          SELECT 'floyd', t.k[1].nodes - 1, t.k[1].nodes, t.k[1].e, t.x,
                 array_prepend((t.k[1].nodes, t.k[1].s, t.k[1].e, t.k[1].s1, t.x, 3) :: K, t.k[2:])
          WHERE t.fn = 'apply' AND t.k[1].ref = 2
            UNION ALL
          SELECT 'apply', t.nodes, t.s, t.e, LEAST(t.k[1].s1, t.k[1].s2 + t.x), t.k[2:]
          WHERE t.fn = 'apply' AND t.k[1].ref = 3
          ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'finish';
$$ LANGUAGE SQL;

-----------------------------------------------------------------------
-- Find the length of the shortest path from here to there

SELECT setseed(0.42);

SELECT here,
       there,
       floyd((SELECT COUNT(*) FROM nodes) :: int, here :: int, there :: int)
FROM (SELECT MAX(node) FROM nodes) AS _(max),
     LATERAL generate_series(1, max) as __(here),
     LATERAL generate_series(1, max) as ___(there)
LIMIT :iterations;
