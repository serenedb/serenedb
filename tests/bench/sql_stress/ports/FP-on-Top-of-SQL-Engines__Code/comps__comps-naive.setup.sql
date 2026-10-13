\set iterations 3000
-- Connected components in a directed acyclic graph (DAG) with an out-degree of two

\set n 10

DROP TABLE IF EXISTS nodes CASCADE;
DROP TYPE IF EXISTS child CASCADE;

-- Uniquely marks the edge connecting a node n to its children.
--
CREATE TYPE child AS ENUM ('l', 'r');

-- Each node is defined by its id and has at most two children.
-- This is enforced by having id and child be a primary key.
--
--     n
--   ↙   ↘
--  l     r
--
CREATE TABLE nodes (
  id    int,
  child child,
  next  int,
  PRIMARY KEY(id, child)
);

SELECT setseed(0.42);

-- Randomly select an operator
DROP FUNCTION IF EXISTS oper();
CREATE FUNCTION oper() RETURNS char(1) AS
$$
  SELECT (array['+','*'])[(1 + random())::int];
$$ LANGUAGE SQL;

INSERT INTO nodes
WITH RECURSIVE
-- (1) construct inner operation nodes of expression tree
operations(n, node, op, arg1, arg2, lit) AS (
  SELECT :n AS n, 0 AS node, oper() AS op, 0 * 2 + 1 AS arg1, 0 * 2 + 2 AS arg2, NULL::numeric AS lit

    UNION ALL

  SELECT e.n - 1 AS n, n.node, n.op, n.arg1, n.arg2, n.lit
  FROM   operations AS e,
         LATERAL (VALUES (e.node * 2 + 1, oper(), (e.node * 2 + 1) * 2 + 1, (e.node * 2 + 1) * 2 + 2, NULL::numeric),
                         (e.node * 2 + 2, oper(), (e.node * 2 + 2) * 2 + 1, (e.node * 2 + 2) * 2 + 2, NULL::numeric)) AS n(node,op,arg1,arg2,lit)
  WHERE  e.n > 0
),
-- (2) at the leaf level add literal expressions
expr(node, op, arg1, arg2, lit) AS (
  SELECT  e.node, e.op, e.arg1, e.arg2, e.lit
  FROM    operations AS e
  WHERE   e.n > 0

    UNION

  SELECT  e.node, 'ℓ' AS op, NULL AS arg1, NULL AS arg2,
          100 * random() AS lit
  FROM    operations AS e
  WHERE   e.n = 0
),
-- (3) introduce distortion and sharing into the expression, we end up with a DAG
shuffle(node, op, arg1, arg2, lit) AS (
  SELECT e.node, e.op,
         CASE WHEN random() < 0.1 THEN (SELECT e1.node FROM expr AS e1 WHERE e1.node > e.node ORDER BY random() LIMIT 1)
                                  ELSE e.arg1
         END AS arg1,
         CASE WHEN random() < 0.1 THEN (SELECT e1.node FROM expr AS e1 WHERE e1.node > e.node ORDER BY random() LIMIT 1)
                                  ELSE e.arg2
         END AS arg2,
         e.lit
  FROM   expr AS e
  WHERE  e.op <> 'ℓ'

    UNION

  SELECT e.*
  FROM   expr AS e
  WHERE  e.op = 'ℓ'
),
maintree AS (
  SELECT *
  FROM   shuffle AS s
  WHERE  s.node = 0
    UNION
  SELECT s.*
  FROM   maintree AS m,
         LATERAL (SELECT *
                  FROM   shuffle AS s
                  WHERE  m.arg1 = s.node
                    UNION ALL
                  SELECT *
                  FROM   shuffle AS s
                  WHERE  m.arg2 = s.node) AS s
)
SELECT s.node AS id, 'l' :: child, s.arg1
FROM   maintree AS s
WHERE  NOT s.arg1 IS NULL
  UNION ALL
SELECT s.node AS id, 'r' :: child, s.arg2
FROM   maintree AS s
WHERE  NOT s.arg2 IS NULL;

analyze nodes;