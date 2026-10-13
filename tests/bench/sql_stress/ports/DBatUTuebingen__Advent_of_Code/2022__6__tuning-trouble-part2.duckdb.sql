SET integer_division = true;
-- AoC 2022, Day 6 (Part 2)
--
-- Just like Part 1, but extend packet (= window size) from 4 to 14 characters
WITH
input(char) AS (
  SELECT unnest(string_split_regex(c.chars, '')) AS char
  FROM   read_csv_auto('2022/6/input.txt', header=false) AS c(chars)
),
buffer(pos, packet) AS (
  SELECT ROW_NUMBER() OVER () AS pos,
         list(i.char) OVER (ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS packet
  FROM   input AS i
),
chars(pos, char) AS (
  SELECT b.pos, unnest(b.packet) AS char
  FROM   buffer AS b
)
SELECT   c.pos AS marker
FROM     chars AS c
GROUP BY c.pos
HAVING COUNT(DISTINCT c.char) = 14
ORDER BY c.pos
LIMIT 1;
