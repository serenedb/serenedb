SET integer_division = true;
-- AoC 2022, Day 21 

CREATE TABLE raw ( i int PRIMARY KEY, line text NOT NULL );

INSERT INTO raw
SELECT ROW_NUMBER() OVER () AS i, line 
FROM   read_csv_auto('2022/10/input.txt', header=false) AS _(line);

CREATE TABLE input ( 
  line int PRIMARY KEY, 
  inst int
);

INSERT INTO input 
SELECT r.i, string_to_array(r.line, ' ')[2]
FROM   raw AS r;

-- Part 1
CREATE TABLE eval (
  cycle int PRIMARY KEY,
  x     int NOT NULL,
  loc   int REFERENCES input(line)
);

INSERT INTO eval
SELECT 0, 1, NULL -- At cycle 0 (before the first cycle begins), set x to 1 
  UNION ALL       -- NOT recursive
SELECT SUM(COALESCE(i.inst * 0 + 2, 1)) OVER eval     AS cycle,
       SUM(COALESCE(i.inst, 0))         OVER eval + 1 AS x,
       i.line                                         AS loc
FROM   input AS i
WINDOW eval AS (ORDER BY i.line ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

SELECT SUM((40 * segment + 20) * x) AS "Day 10 (part one)"
FROM (
  SELECT (e.cycle + 20) / 40, 
         (SELECT _e.x 
          FROM   eval AS _e 
          WHERE  _e.cycle = MAX(e.cycle))
  FROM   eval AS e
  GROUP BY (e.cycle + 20) / 40
) AS _(segment, x)
WHERE 40 * segment + 20 <= 220;

-- Part 2
CREATE MACRO is_addx(inst) AS inst IS NOT NULL;

SELECT STRING_AGG(CASE WHEN pixel THEN '#' ELSE '.' END, '' ORDER BY x) AS "Day 10 (part two)"
FROM   (
  SELECT pos.x, pos.y, pos.x BETWEEN e.x-1 AND e.x+1
  FROM (
    SELECT e.cycle, 
           LAG(e.x) OVER (ORDER BY e.cycle), 
           is_addx(i.inst)
    FROM   eval AS e JOIN input AS i ON e.loc = i.line
  ) AS e(cycle, x, "addx?"), LATERAL (
    SELECT (e.cycle-1)%40, (e.cycle-1)/40
      UNION ALL 
    SELECT (e.cycle-2)%40, (e.cycle-2)/40
    WHERE  e."addx?"
  ) AS pos(x,y)
  WHERE  e.cycle > 0
) AS draw(x, y, pixel)
GROUP BY y
ORDER BY y;