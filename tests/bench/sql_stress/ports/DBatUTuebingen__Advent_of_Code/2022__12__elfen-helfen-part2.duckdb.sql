SET integer_division = true;
-- macro to treat 'S' as 'a' and 'E' as 'z'
-- do NOT use union here if the macro is used in the CTE
CREATE MACRO ascii2(height) AS
CASE WHEN height = 'S' THEN ascii('a')
     WHEN height = 'E' THEN ascii('z')
     ELSE ascii(height) END;

-- read input
WITH RECURSIVE input(row, map) AS (
     SELECT ROW_NUMBER() OVER () AS row, string_to_array(s, NULL) AS map
     FROM read_csv_auto('2022/12/input.txt', header=false) AS c(s)
),
-- parse input into desired format
heightmap(row, col, height) AS (
     SELECT i.row AS row,
            generate_subscripts(i.map, 1) AS a,
            i.map[a] AS height
     FROM input AS i
),
-- Denis' optimierte Breitensuche
path_finding(row, col, height, steps, visited) AS (
  SELECT h.row, h.col, h.height, 0, ARRAY[ARRAY[h.row, h.col]]
  FROM heightmap AS h
  WHERE height='E'

    UNION ALL

  (WITH vis AS (SELECT DISTINCT unnest(visited) AS p FROM path_finding)
  SELECT DISTINCT ON (h.row, h.col, h.height, p.steps) h.row, h.col, h.height, p.steps + 1, array_append(p.visited, ARRAY[h.row, h.col])
  FROM (VALUES (-1, 0), (1, 0), (0,1), (0,-1)) AS d(row, col)
  JOIN path_finding AS p ON true
  JOIN heightmap AS h ON p.row + d.row = h.row  AND p.col + d.col = h.col
  WHERE  h.height IS NOT NULL AND NOT array_contains(p.visited, ARRAY[h.row, h.col])
  AND    ascii2(p.height) <= ascii2(h.height) + 1
  AND    p.height <> 'a'
  AND    ARRAY[h.row,h.col] NOT IN (SELECT * FROM vis)
  )
)
SELECT * FROM path_finding WHERE height = 'a'
LIMIT 1;
