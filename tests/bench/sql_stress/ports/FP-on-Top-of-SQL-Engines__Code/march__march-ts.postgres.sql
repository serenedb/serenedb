-----------------------------------------------------------------------
-- Translation into a UDF using trampolined style

DROP TYPE IF EXISTS kontMarch CASCADE;
CREATE TYPE kontMarch AS (current int[], "track?" boolean);

DROP FUNCTION IF EXISTS marchTS(int[], int[], boolean);

CREATE FUNCTION marchTS(current int[], goal int[], "track?" boolean) RETURNS int[][] AS
$$
WITH RECURSIVE tramp(fn, current, goal, "track?", x, k) AS (
  SELECT 'march', current, goal, "track?", ARRAY[] :: int[][], ARRAY[] :: kontMarch[]
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'apply', t.current, t.goal, t."track?", ARRAY[] :: int[][], t.k
          WHERE t.fn = 'march' AND t."track?" AND t.current = t.goal
            UNION ALL
          SELECT 'march', ARRAY[t.current[1] + d.dir[0], t.current[2] + d.dir[1]] :: int[],
                 t.goal, d."track?", t.x, (t.current, t."track?") :: kontMarch || t.k
          FROM squares AS s, directions AS d
          WHERE t.fn = 'march' AND NOT(t."track?" AND t.current = t.goal) AND
                t.current = s.xy AND (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur) AND d."track?"
            UNION ALL
          SELECT 'march', ARRAY[t.current[1] + d.dir[0], t.current[2] + d.dir[1]] :: int[],
                 ARRAY[t.current[1] + d.dir[0], t.current[2] + d.dir[1]] :: int[], d."track?",
                 t.x, (t.current, t."track?") :: kontMarch || t.k
          FROM squares AS s, directions AS d
          WHERE t.fn = 'march' AND
                NOT(t."track?" AND t.current = t.goal) AND t.current = s.xy AND
                (s.ll,s.lr,s.ul,s.ur) = (d.ll,d.lr,d.ul,d.ur) AND NOT(d."track?")
            UNION ALL
          SELECT 'finish', t.current, t.goal, t."track?", t.x, t.k
          WHERE t.fn = 'apply' AND CARDINALITY(t.k) = 0
            UNION ALL
          SELECT 'apply', t.k[1].current, t.goal, t.k[1]."track?",
                  ARRAY[t.k[1].current] || t.x, t.k[2:]
          WHERE t.fn = 'apply' AND CARDINALITY(t.k) <> 0 AND t.k[1]."track?"
            UNION ALL
          SELECT 'apply', t.k[1].current, t.goal, t.k[1]."track?",
                  ARRAY[] :: int[][] || t.x, t.k[2:]
          WHERE t.fn = 'apply' AND CARDINALITY(t.k) <> 0 AND NOT(t.k[1]."track?")
          ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'finish';
$$
LANGUAGE SQL STRICT;

-- Trace the shape's border in the 2D map, starting from (x,y)
--

SELECT setseed(0.42);

SELECT point(x, y) AS start, marchTS(array[x, y], array[x, y], false) AS border
FROM (SELECT s.i FROM generate_series(0, 57) AS s(i) ORDER BY random()) AS _(x),
     (SELECT s.i FROM generate_series(0, 67) AS s(i) ORDER BY random()) AS __(y)
LIMIT :iterations;
