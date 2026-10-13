SET integer_division = true;
-- AoC 2022, Day 23 (Part 1)

-- AoC input file
CREATE MACRO input() AS '2022/23/input.txt';

-- # of rounds in which elves move
CREATE MACRO rounds() AS 10;

-- done once we have moved the required # of rounds
CREATE MACRO done(p) AS p.round >= rounds();

-- AoC 2022, Day 23 

-- convert array of 0/1s into integer
CREATE MACRO byte(bits) AS
  list_sum(list_apply(range(0,8), b -> bits[8-b] * 1 << b));

-- 2D vector
CREATE MACRO v2(x, y) AS
  {x: x, y: y};

-- 2D vector addition
CREATE MACRO addv2(v1,v2) AS
  v2(v1.x + v2.x, v1.y + v2.y);

-- valid directions (if there is no elf to the N, NW, and NE, then move N)
CREATE MACRO dirs() AS
  --            N  S  W  E  NW SE SE NE
  [{bits: byte([1, 0, 0, 0, 1, 0, 0, 1]), dir: v2( 0,-1)},  -- move N
   {bits: byte([0, 1, 0, 0, 0, 1, 1, 0]), dir: v2( 0, 1)},  -- move S
   {bits: byte([0, 0, 1, 0, 1, 0, 1, 0]), dir: v2(-1, 0)},  -- move W
   {bits: byte([0, 0, 0, 1, 0, 1, 0, 1]), dir: v2( 1, 0)}]; -- move E

-- translate relative Δx,Δy movement into a direction
CREATE MACRO dir(Δx,Δy) AS
  list_extract([byte([0,0,0,0,1,0,0,0]),  -- NW   (-1,-1) |
                byte([0,0,1,0,0,0,0,0]),  -- W    (-1, 0) | ascendingly ordered
                byte([0,0,0,0,0,0,1,0]),  -- SW   (-1, 1) v
                byte([1,0,0,0,0,0,0,0]),  -- N    ( 0,-1)
                byte([0,0,0,0,0,0,0,0]),  -- self ( 0, 0)
                byte([0,1,0,0,0,0,0,0]),  -- S    ( 0, 1)
                byte([0,0,0,0,0,0,0,1]),  -- NE   ( 1,-1)
                byte([0,0,0,1,0,0,0,0]),  -- E    ( 1, 0)
                byte([0,0,0,0,0,1,0,0])], -- SE   ( 1, 1)
      (Δx+1)*3 + (Δy+1) + 1);

-- inspect vicinity, propose first direction without elves (all vicinity bits = 0),
-- if all directions are blocked, do not move
CREATE MACRO propose(vicinity, directions) AS
  COALESCE(directions[list_indexof(list_apply(directions, d -> vicinity & d.bits), 0)].dir,
           v2(0,0));

-- rotate array xs left
CREATE MACRO rotate(xs) AS
  xs[2:] || [xs[1]];

CREATE TABLE rounds AS
  WITH RECURSIVE
  input(y, row, x) AS (
    SELECT ROW_NUMBER () OVER ()      AS y,
           string_split(c.line, NULL)   AS row,
           generate_subscripts(string_split(c.line, NULL),1) AS x
    FROM   read_csv_auto(input(), SEP='', header=false) AS c(line)
  ),
  scan(xy) AS (
    SELECT v2(i.x,i.y) AS xy
    FROM   input AS i
    WHERE  i.row[i.x] = '#'
  ),
  rounds(round, dirs, xy, "stable?") AS (
    SELECT 0 AS round, dirs() AS dirs, s.xy, false AS "stable?"
    FROM   scan AS s

      UNION ALL

    SELECT proposal.round + 1    AS round,
           rotate(proposal.dirs) AS dirs,
           CASE WHEN COUNT(*) OVER (PARTITION BY proposal.move) = 1
                THEN proposal.move
                ELSE proposal.xy
           END AS xy,
           bool_and(proposal.move = proposal.xy) OVER () AS "stable?"
    FROM   (SELECT  r1.*,
                    CASE WHEN vicinity
                         THEN addv2(r1.xy, propose(vicinity, r1.dirs))
                         ELSE r1.xy
                    END AS move
            FROM    rounds AS r1,
            LATERAL (SELECT bit_or(dir(Δx,Δy))
                     FROM   rounds AS r2,
                            generate_series(-1,1) AS  _(Δx),
                            generate_series(-1,1) AS __(Δy)
                     WHERE  addv2(r1.xy, v2(Δx,Δy)) = r2.xy) AS _(vicinity)
           ) AS proposal(round, dirs, xy, "stable?", move)
    WHERE NOT done(proposal)
  )
  TABLE rounds;

--                  width                             height               elf tiles
--     ┌─────────────────────────────┐   ┌─────────────────────────────┐   ┌──────┐
SELECT (MAX(r.xy.x) - MIN(r.xy.x) + 1) * (MAX(r.xy.y) - MIN(r.xy.y) + 1) - COUNT(*) AS empty_tiles
FROM   rounds AS r
WHERE  r.round = rounds();
