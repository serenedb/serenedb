-- Community Detection using Label Propagation (CDLP)
--
-- See the LDBC Graphalytics Benckmark
-- https://arxiv.org/pdf/2011.15028


DROP TABLE IF EXISTS edges CASCADE;
DROP TABLE IF EXISTS nodes CASCADE;

CREATE TABLE nodes (
  id int PRIMARY KEY
);

CREATE TABLE edges (
   here int,
   there   int,
   FOREIGN KEY (here)  REFERENCES nodes,
   FOREIGN KEY (there) REFERENCES nodes
);

-- Sample directed graph from Figure D.2 (a)



-- Social Network Graph from https://docs.falkordb.com/algorithms/cdlp.html
--  (1)→(5)→(6)  (11)
--   ↓    |    ↓
--  (2)   |   (7)→(8)
--   ↓    ↓    ↓    ↓
--  (3)→(4)  (9)→(10)

INSERT INTO nodes(id)
  FROM generate_series(1,11);

INSERT INTO edges(here, there) VALUES
   (1,2), (1,5),
   (2,3),
   (3,4),
   (5,4), (5,6),
   (6,7),
   (7,8), (7,9),
   (8,10),
   (9,10);

-- add inverted edges (we're working with undirected graphs here!)
INSERT INTO edges
  SELECT there, here
  FROM   edges;

FROM nodes;
FROM edges;

-----------------------------------------------------------------------
-- Community Detection using Label Propagation (CDLP)
--
--
-- ⚠️ Only DuckDB v2.0 supports USING KEY with UNION semantics
--     (stop once the node-to-community assigment does not change)
WITH RECURSIVE cdlp(id, L) USING KEY (id, min(L)) AS (
  SELECT n.id, n.id AS L
  FROM   nodes AS n
    UNION
  SELECT v.id, mode(u.L ORDER BY u.L) AS freq
  FROM   recurring.cdlp AS v, edges AS e, recurring.cdlp AS u
  WHERE  (e.here, e.there) = (v.id, u.id)
  GROUP BY v.id
)
SELECT c.L, list(c.id) AS nodes
FROM   cdlp AS c
GROUP BY c.L;
