SET integer_division = true;
-- AoC 2022, Day 3 (Part 2)

WITH
items(row, g, l, item) AS (
  SELECT ROW_NUMBER() OVER () - 1 AS row,
         row / 3 AS g, row % 3 AS l, unnest(string_split(c.items, NULL)) AS item
  FROM   read_csv_auto('2022/3/input.txt', header=false) AS c(items)
),
common(g, item) AS (
  SELECT DISTINCT i1.g, i1.item
  FROM   items AS i1 JOIN items AS i2 USING (g,item) JOIN items AS i3 USING (g,item)
  WHERE  [i1.l, i2.l, i3.l] = [0, 1, 2]
)
--         a..z → 1..26, A..Z → 27..52
SELECT SUM(ord(c.item) - ord('A') + 27 - 58 * (c.item >= 'a') :: int) AS priority
FROM   common AS c;

