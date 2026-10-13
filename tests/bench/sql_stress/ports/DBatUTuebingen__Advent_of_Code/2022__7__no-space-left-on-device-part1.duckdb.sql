SET integer_division = true;
-- AoC 2022, Day 7 (Part 1)

DROP TYPE IF EXISTS file;
CREATE TYPE file AS struct(name text, dir boolean, size int);

CREATE TEMP TABLE paths AS
  WITH RECURSIVE
  input(num,command) AS (
    SELECT  ROW_NUMBER() OVER () AS num, string_split_regex(c.line, ' +') AS command
    FROM    read_csv_auto('2022/7/input.txt', SEP='', header=false) AS c(line)
  ),
  -- simulate a shell that runs command "num" in current working directory "cwd",
  -- encountering file/directory "path"
  shell(num, cwd, path) AS (
    SELECT 0                                                  AS num,
           []   :: file[]                                     AS cwd,
           NULL :: struct(name text, dir boolean, size int)[] AS path  -- casting to file[] yields *** (bug in DuckDB?)
    FROM   input AS i
      UNION
    SELECT i.num,
           CASE WHEN i.command      = ['$','cd','..'] THEN array_pop_back(s.cwd)
                WHEN i.command[1:2] = ['$','cd']      THEN s.cwd || [{name:i.command[3]||s.num,dir:true,size:NULL}]
                ELSE s.cwd
           END AS cwd,
           CASE WHEN i.command[1] NOT IN ('$','dir')  THEN s.cwd || [{name:i.command[2]||s.num,dir:false,size:i.command[1] :: int}]
                WHEN i.command[1] = 'dir'             THEN s.cwd || [{name:i.command[2]||s.num,dir:true,size:NULL}]
                ELSE NULL                             -- ignore command ['$','ls']
           END AS path
    FROM   shell AS s, input AS i
    WHERE  i.num = s.num + 1
  )
  SELECT s.path
  FROM   shell AS s
  WHERE  s.path IS NOT NULL;

-- TODO: From here on, operate on the collected path column (of absolute paths)
--       directly, DO NOT split into edges (can save CTEs nodes, edges, tree?)
--       Find all files/dirs below a dir by looking for path prefixes,
--       no further recursive CTE required.

-- all nodes (files/dirs) on a path
WITH RECURSIVE
nodes(path, node) AS (
  SELECT ROW_NUMBER() OVER () AS path, unnest(p.path) AS node
  FROM   paths AS p
),
-- pairs of adjacent nodes on paths
edges(node, parent) AS (
  SELECT nth_value(n.node, 2) OVER downwards AS node,
         nth_value(n.node, 1) OVER downwards AS parent,
  FROM   nodes AS n
  WINDOW downwards AS (PARTITION BY n.path ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
),
-- file system tree
tree(node, parent) AS (
  SELECT DISTINCT e.node, e.parent
  FROM   edges AS e
  WHERE  e.node IS NOT NULL
),
-- recursively walk up the file system hierarchy, annotate dirs/files with their sizes
ascent(node, size) AS (
  SELECT t.node AS node, t.node.size AS size
  FROM   tree AS t
  WHERE  NOT t.node.dir
    UNION
  SELECT t.parent AS node, a.size
  FROM   tree AS t, ascent AS a
  WHERE  t.node = a.node
  AND    t.parent IS NOT NULL
),
-- aggregate sizes (of directories)
sizes(node, size) AS (
  SELECT a.node, SUM(a.size) AS size
  FROM   ascent AS a
  GROUP BY a.node
)
SELECT SUM(s.size) AS size
FROM   sizes AS s
WHERE  s.node.dir AND s.size <= 100000;

-----------------------------------------------------------------------

-- ***
-- Error: near line 34: Binder Error:
--   No function matches the given name and argument types
--   'struct_extract(file, VARCHAR)'.
--   You might need to add explicit type casts.
--   Candidate functions:
--   struct_extract(STRUCT, VARCHAR) -> ANY
