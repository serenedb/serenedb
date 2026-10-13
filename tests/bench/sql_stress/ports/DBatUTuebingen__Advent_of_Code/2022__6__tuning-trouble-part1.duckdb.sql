SET integer_division = true;
-- AoC 2022, Day 6 (Part 1)

WITH
input(char) AS (
  SELECT unnest(string_split_regex(c.chars, '')) AS char
  FROM   read_csv_auto('2022/6/input.txt', header=false) AS c(chars)
),
buffer(pos, packet) AS (
  SELECT ROW_NUMBER() OVER () AS pos,
         list(i.char) OVER (ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS packet
  FROM   input AS i
),
chars(pos, char) AS (
  SELECT b.pos, unnest(b.packet) AS char
  FROM   buffer AS b
)
SELECT   c.pos AS marker
FROM     chars AS c
GROUP BY c.pos
HAVING COUNT(DISTINCT c.char) = 4
ORDER BY c.pos
LIMIT 1;


-- Alternative formulation using len(list_distinct(...))
-- [DuckDB does not implement COUNT(DISTINCT ...) as window function]

-- AoC 2022, Day 6 (Part 1)
WITH
input(char) AS (
  SELECT unnest(string_split_regex(c.chars, '')) AS char
  FROM   read_csv_auto('2022/6/input.txt', header=false) AS c(chars)
)
SELECT   ROW_NUMBER() OVER () AS marker
FROM     input AS i
QUALIFY  len(list_distinct(list(i.char) OVER (ROWS BETWEEN 3 PRECEDING AND CURRENT ROW))) = 4
ORDER BY marker
LIMIT 1;
