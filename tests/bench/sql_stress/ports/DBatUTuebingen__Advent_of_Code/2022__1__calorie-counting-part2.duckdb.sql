SET integer_division = true;
-- AoC 2022, Day 1 (Part 2)

WITH
input(calories, num) AS (
  SELECT c.calories, ROW_NUMBER() OVER () AS num
  FROM   read_csv_auto('2022/1/input.txt') AS c(calories)
),
reindeers(reindeer, calories) AS (
  SELECT SUM((i.calories IS NULL) :: int) OVER (ORDER BY num) AS reindeer, i.calories
  FROM   input AS i
)
SELECT SUM(c.calories) AS top3
FROM   (SELECT   SUM(r.calories) AS calories
        FROM     reindeers AS r
        GROUP BY r.reindeer
        ORDER BY calories DESC
        LIMIT 3) AS c;
