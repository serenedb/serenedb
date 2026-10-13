\set iterations 15
-- Floyd-Warshall's algorithm to find the length of the shortest path
-- in a weighted, directed graph
--
-- The naive formulation below performs an exponential number of
-- recursive calls (O(3ⁿ) for a graph of n nodes).  Once the
-- UDF is compiled into a CTE and call sharing is applied, we observe
-- the expected O(n³) complexity of the Floyd-Warshall algorithm.
-- Additionally, over time, memoization leads to the materialization of
-- the graph's node distance matrix, which drastically cuts down
-- evaluation time.

-----------------------------------------------------------------------
-- Recursive SQL UDF in functional style that implements
-- Floyd-Warshall's algorithm to find the length of the
-- shortest path between nodes s and e (returns NULL if there
-- is no such path):
--
SELECT setseed(0.42);

-- Graph size (# of nodes)
\set G 10
DROP TABLE IF EXISTS nodes CASCADE;
CREATE TABLE nodes (
  node  serial PRIMARY KEY,
  label char(1)
);

-- Generate G random nodes, label ∊ {'A'...'Z'}
INSERT INTO nodes(label)
  SELECT chr(ascii('A') + (random() * 25) :: int)
  FROM   generate_series(1, :G);

-- Edge set (weighted, directed)
DROP TABLE IF EXISTS edges CASCADE;
CREATE TABLE edges (
  here   int REFERENCES nodes,
  there  int REFERENCES nodes,
  weight int,
  PRIMARY KEY (here, there));

-- Generate random edges
INSERT INTO edges(here, there, weight)
  SELECT here.node AS here, there.node AS there,
         (random() * 10) :: int AS weight
  FROM   nodes AS here, nodes AS there
  WHERE  abs(ascii(here.label) - ascii(there.label)) <= 3
  AND    random() < 0.5;

analyze nodes;
