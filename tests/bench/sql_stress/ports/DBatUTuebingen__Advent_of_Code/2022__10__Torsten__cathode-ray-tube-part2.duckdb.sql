SET integer_division = true;
-- AoC 2022, Day 10 (Part 2)

WITH
input(ip, _, addx, v) AS (
  SELECT ROW_NUMBER() OVER () AS ip,
         string_split(c.line, ' ') AS instr,
         instr[1] = 'addx' AS addx, COALESCE(instr[2] :: int, 0) AS v
  FROM   read_csv_auto('2022/10/Torsten/input.txt', SEP='', header=false) AS c(line)
),
noop(ip, addx, v) AS (
  -- register x starts with value 1
  SELECT 0 AS ip, NULL AS addx, 1 AS v
    UNION ALL
  SELECT i.ip, i.addx, i.v
  FROM   input AS i
    UNION ALL
  -- add dummy instruction to model addx's exec time
  SELECT i.ip - 0.5 AS ip, false AS addx, 0 AS v
  FROM   input AS i
  WHERE  i.addx
),
run(cycle, x) AS (
  SELECT ROW_NUMBER() OVER (ORDER BY n.ip) AS cycle,
         SUM(n.v) OVER (ORDER BY n.ip)     AS x
  FROM   noop AS n
),
crt(col, row, pixel) AS (
  SELECT (r.cycle - 1) % 40 AS col, (r.cycle - 1) / 40 AS row,
         r.x IN (col - 1, col, col + 1) AS pixel
  FROM   run AS r
  -- DuckDB respects table order: order rows by col
  -- to prepare string_agg below
  ORDER BY row, col
)
SELECT c.row, string_agg(CASE WHEN c.pixel THEN '#' ELSE '.' END, '') AS pixels
FROM   crt AS c
GROUP BY c.row;
