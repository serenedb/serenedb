SET integer_division = true;
-- AoC 2022, Day 3 (Part 1)

WITH
input(row, len, l, r) AS (
  SELECT ROW_NUMBER() OVER () AS row,
         length(c.s) AS len, left(c.s, len/2) AS l, right(c.s, len/2) AS r
  FROM   read_csv_auto('2022/3/input.txt', header=false) AS c(s)
),
align(row, l, r) AS (
  SELECT DISTINCT s.row, unnest(string_split(s.l, NULL)) AS l, s.r
  FROM   (SELECT  i.row, i.l, unnest(string_split(i.r, NULL)) AS r
          FROM    input AS i) AS s
)
--         a..z → 1..26, A..Z → 27..52
SELECT SUM(ord(a.l) - ord('A') + 27 - 58 * (a.l >= 'a') :: int) AS priority
FROM   align AS a
WHERE  a.l = a.r;
