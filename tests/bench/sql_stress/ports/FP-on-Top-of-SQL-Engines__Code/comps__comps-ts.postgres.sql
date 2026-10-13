-- Connected components in a directed acyclic graph (DAG) with an out-degree of two

-- Translation into a UDF using trampolined style

DROP TYPE IF EXISTS K;
CREATE TYPE K AS (node int, target int, c1 boolean, ref int);

DROP FUNCTION IF EXISTS connectedTS(int, int);

CREATE FUNCTION connectedTS(node int, target int) RETURNS boolean AS
$$
WITH RECURSIVE tramp(fn, node, target, x, k) AS (
  SELECT 'Connected', node, target, False, ARRAY[] :: K[]
    UNION ALL
  SELECT _.*
  FROM   tramp AS t, LATERAL
          (SELECT 'Apply', t.node, t.target, True, t.k
          WHERE t.fn = 'Connected' AND t.node = t.target
            UNION ALL
          SELECT 'Apply', t.node, t.target, False, t.k
          WHERE t.fn = 'Connected' AND t.node <> t.target AND
                NOT EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = t.node)
            UNION ALL
          SELECT 'Connected', (SELECT n.next FROM nodes AS n WHERE (n.id, n.child) = (t.node, 'l')),
                 t.target, t.x, array_prepend((t.node, t.target, False, 1) :: K, t.k)
          WHERE t.fn = 'Connected' AND t.node <> t.target AND
                ((SELECT COUNT(*) FROM nodes AS n WHERE n.id = t.node) = 2)
            UNION ALL
          SELECT 'Connected', (SELECT n.next FROM nodes AS n WHERE n.id = t.node),
                  t.target, t.x, t.k
          WHERE t.fn = 'Connected' AND t.node <> t.target AND
                EXISTS (SELECT n.id FROM nodes AS n WHERE n.id = t.node) AND
                ((SELECT COUNT(*) FROM nodes AS n WHERE n.id = t.node) <> 2)
            UNION ALL
          SELECT 'Finish', t.node, t.target, t.x, t.k
          WHERE t.fn = 'Apply' AND CARDINALITY(t.k) = 0
            UNION ALL
          SELECT 'Connected', (SELECT n.next FROM nodes AS n WHERE (n.id, n.child) = (t.k[1].node, 'r')),
                 t.k[1].target, t.x, array_prepend((t.k[1].node, t.k[1].target, t.x, 2) :: K, t.k[2:])
          WHERE t.fn = 'Apply' AND t.k[1].ref = 1
            UNION ALL
          SELECT 'Apply', t.node, t.target, t.k[1].c1 or t.x, t.k[2:]
          WHERE t.fn = 'Apply' AND t.k[1].ref = 2
          ) AS _
) SELECT t.x FROM tramp AS t WHERE t.fn = 'Finish';
$$ LANGUAGE SQL STABLE STRICT;

/*
-----------------------------------------------------------------------
-- Check whether node there can be reached from here
*/

SELECT here, there, connectedTS(here,there)
FROM generate_series(1, :iterations) as s(i),
     LATERAL (SELECT n.id FROM nodes AS n WHERE s.i = s.i ORDER BY random() LIMIT 1) as _(here),
     LATERAL (SELECT n.id FROM nodes AS n WHERE s.i = s.i ORDER BY random() LIMIT 1) as __(there);
