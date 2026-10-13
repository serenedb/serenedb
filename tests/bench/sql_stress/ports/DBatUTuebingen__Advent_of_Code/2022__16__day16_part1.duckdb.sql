SET integer_division = true;
DROP TABLE IF EXISTS input;
CREATE TABLE input(valve TEXT, flow INT, tunnel TEXT);
INSERT INTO INPUT
SELECT regexp_extract(valve, 'Valve (..)', 1) :: text AS valve,                    -- extract valve name
       regexp_extract(valve, 'flow rate=(.*)', 1) :: int AS flow, -- extract valve flow rate
       unnest(string_to_array(string_split_regex(tunnel, 'valves\s|valve\s')[2], ', ')) :: text AS tunnel
FROM read_csv_auto('2022/16/input.txt', header=false) AS c(valve, tunnel);

DROP TABLE IF EXISTS distances;
CREATE TABLE distances(src TEXT, dst TEXT, distance INT);

-- read input
WITH RECURSIVE hops(src, dst, distance, visited) AS
(
  -- BFS, start from every node in the graph
  SELECT DISTINCT ON(valve) valve, valve, 0, ARRAY[valve] :: TEXT[]
  FROM   input

    UNION ALL

  -- Find all paths
  SELECT h.dst, i.tunnel, h.distance + 1, ARRAY_APPEND(visited, i.tunnel)
  FROM   hops AS h, input AS i
  WHERE  h.src = i.valve
  -- AND    h.src <> i.valve
  AND    NOT ARRAY_CONTAINS(visited, i.tunnel)
)
INSERT INTO distances
(
  -- Find the shortest distance from every node, to every other node
  -- Ignore nodes with flow = 0, but keep node 'AA'
  SELECT DISTINCT ON (src, dst) src, dst, MIN(distance) + 1
  FROM hops, input AS i1, input AS i2
  WHERE i1.valve = hops.src
  AND   i2.valve = hops.dst
  AND   (i1.flow <> 0 OR i1.valve = 'AA')
  AND   i2.flow <> 0
  AND   hops.src <> hops.dst
  GROUP BY src, dst
);

WITH RECURSIVE flow(time, valve, released, visited) AS
(
  SELECT 30, 'AA', 0, ARRAY[] :: TEXT[]
    UNION ALL
  SELECT time - d.distance, d.dst, released + (time - d.distance) * d.flow, ARRAY_APPEND(visited, d.dst)
  FROM   flow AS f, (SELECT DISTINCT d.src, d.dst, d.distance, i.flow
                     FROM   distances AS d, input AS i
                     WHERE  d.dst = i.valve) AS d
  WHERE  f.valve = d.src
  AND    NOT ARRAY_CONTAINS(visited, d.dst)
  AND    time - d.distance >= 0
)
SELECT *
FROM   flow
ORDER BY released DESC
LIMIT 1;
