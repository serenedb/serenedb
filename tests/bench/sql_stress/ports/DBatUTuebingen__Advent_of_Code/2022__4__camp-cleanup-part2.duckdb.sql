SET integer_division = true;
-- AoC 2022, Day 4 (Part 2)

WITH
input(sec) AS (
  SELECT string_split_regex(c.s, '[,-]') :: int[] AS sec
  FROM   read_csv_auto('2022/4/input.txt', SEP='', header=false) AS c(s)
)
SELECT COUNT(*) AS pairs
FROM   input AS i
WHERE  i.sec[3] <= i.sec[1] AND i.sec[1] <= i.sec[4]
OR     i.sec[1] <= i.sec[3] AND i.sec[3] <= i.sec[2];


-- Optional:
-- fold both parts of Day 4 into a single query using COUNT(*) FILTER(...)

WITH
input(sec) AS (
  SELECT string_split_regex(c.s, '[,-]') :: int[] AS sec
  FROM   read_csv_auto('2022/4/input.txt', SEP='', header=false) AS c(s)
)
SELECT COUNT(*) FILTER (   i.sec[1] <= i.sec[3] AND i.sec[4] <= i.sec[2]
                        OR i.sec[3] <= i.sec[1] AND i.sec[2] <= i.sec[4]) AS part1,
       COUNT(*) FILTER (   i.sec[3] <= i.sec[1] AND i.sec[1] <= i.sec[4]
                        OR i.sec[1] <= i.sec[3] AND i.sec[3] <= i.sec[2]) AS part2
FROM   input AS i;
