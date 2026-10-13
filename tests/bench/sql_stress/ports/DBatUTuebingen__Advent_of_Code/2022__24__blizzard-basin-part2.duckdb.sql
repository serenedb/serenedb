SET integer_division = true;
-- AoC 2022, Day 24 (Part 2)

-- AoC input file
CREATE MACRO input() AS '2022/24/input.txt';

-- AoC 2022, Day 24

-- 2D vector
CREATE MACRO v2(x, y) AS
  {x: x, y: y};

-- like a % b (but works for a < 0)
CREATE MACRO modulo(a,b) AS
  ((a) % (b) + (b)) % (b);

DROP TABLE IF EXISTS blizzards;
CREATE TABLE blizzards AS
  WITH
  input(y, row, x) AS (
    SELECT ROW_NUMBER () OVER ()      AS y,
           string_split(c.line, NULL)   AS row,
           generate_subscripts(string_split(c.line, NULL),1) AS x
    FROM   read_csv_auto(input(), SEP='', header=false) AS c(line)
  )
  SELECT i.x - 2 AS x, i.y - 2 AS y, i.row[i.x] AS dir
  FROM   input AS i
  WHERE  i.row[i.x] <> '#' AND i.row[3] <> '#'; -- focus on valley strictly inside the # border

DROP TABLE IF EXISTS grid;
CREATE TABLE grid AS
  SELECT MAX(b.x) + 1          AS width,
         MAX(b.y) + 1          AS height,
         v2(0, -1)             AS start,
         v2(width - 1, height) AS goal,
         lcm(width, height)    AS repeat
  FROM   blizzards AS b;

-- start/goal location in the valley
CREATE MACRO start() AS (SELECT g.start FROM grid AS g);
CREATE MACRO goal()  AS (SELECT g.goal  FROM grid AS g);


-- at given minute, position @ x,y is open
DROP TABLE IF EXISTS open;
CREATE TABLE open AS
  WITH RECURSIVE
  open(minute, x ,y) AS (
    SELECT 0 AS minute, b.x, b.y
    FROM   blizzards AS b
    WHERE  b.dir = '.'

      UNION ALL

    SELECT  DISTINCT next.minute, b.x, b.y
    FROM    open AS o, grid AS g,
    LATERAL (SELECT o.minute + 1) AS next(minute),
            blizzards AS b, blizzards AS b1, blizzards AS b2, blizzards AS b3, blizzards AS b4
    WHERE   b1.x = b.x                                AND b1.y = modulo(b.y - next.minute, g.height) AND b1.dir <> 'v'
    AND     b2.x = b.x                                AND b2.y = modulo(b.y + next.minute, g.height) AND b2.dir <> '^'
    AND     b3.x = modulo(b.x - next.minute, g.width) AND b3.y = b.y                                 AND b3.dir <> '>'
    AND     b4.x = modulo(b.x + next.minute, g.width) AND b4.y = b.y                                 AND b4.dir <> '<'
    AND     next.minute < g.repeat
  )
  SELECT o.minute, v2(o.x, o.y) AS xy
  FROM   open AS o
    UNION
  -- grid locations start and goal are open anytime
  SELECT DISTINCT ON (minute) o.minute, g.start AS xy
  FROM   open AS o, grid AS g
    UNION
  SELECT DISTINCT ON (minute) o.minute, g.goal AS xy
  FROM   open AS o, grid AS g;

-- expedition through the blizzard basin,
-- starting at minute, traversing from here to there,
-- returns minute of arrival
CREATE MACRO expedition(minute, here, there) AS (
  WITH RECURSIVE
  expedition(minute, x, y, done) AS (
    SELECT minute, (here).x, (here).y, false AS done
    FROM   grid AS g

      UNION ALL

    SELECT  DISTINCT next.minute, go.x, go.y, bool_or(v2(go.x,go.y) = there) OVER () AS done
    FROM    expedition AS e, grid AS g,
    LATERAL (SELECT e.minute + 1) AS next(minute),
    LATERAL (VALUES (e.x    , e.y    ),                 -- stay
                    (e.x - 1, e.y    ),                 -- move left
                    (e.x + 1, e.y    ),                 -- move right
                    (e.x    , e.y - 1),                 -- move up
                    (e.x    , e.y + 1)) AS go(x, y),    -- move down
            open AS o
    WHERE   (next.minute % g.repeat, v2(go.x, go.y)) = o
    AND     NOT e.done
  )
  SELECT DISTINCT e.minute AS minutes
  FROM   expedition AS e
  WHERE  e.done
);

-- three consecutive expeditions:
--  from start to goal,
--  return to start
--  going to goal once more
SELECT expedition(
         expedition(
           expedition(0, start(), goal()),  -- 
           goal(), start()),                -- 
         start(), goal()) AS minutes;       -- 
