SET integer_division = true;
-- AoC 2022, Day 19 (Part 1)

-- duration of mining run
CREATE MACRO minutes() AS 24;

-- # of blueprints to consider
CREATE MACRO blueprints() AS 30;

-- quality level of blueprint (based on # of geodes mined)
CREATE MACRO AGG(m) AS SUM(m.blueprint * m.geodes);

-- perform mining for geodes
-- AoC 2022, Day 19 (code common to Parts 1 & 2)

-- a robot/resource vector
-- (indices: ore=1, clay=2, obsidian=3, geode=4)
CREATE TYPE vec4 AS int[4];

-- ∞ (infinite cost)
CREATE MACRO oo() AS 1000000;


CREATE TABLE input (
  id        int,      -- blueprint ID
  blueprint vec4[]    -- robot building cost
);

-- input.txt
INSERT INTO input(id, blueprint) VALUES
--   to build a robot of type ...
--
--         ore        clay     obsidian      geode
--      ┌───────┐  ┌───────┐  ┌────────┐  ┌────────┐
--
--   ... you need these resources:
--
--         geode
--    obsidian │
--      clay │ │
--     ore │ │ │
--       │ │ │ │
  ( 1, [[4,0,0,0], [4,0,0,0], [4,12,0,0], [4,0,19,0]]),
  ( 2, [[4,0,0,0], [4,0,0,0], [2,11,0,0], [2,0, 7,0]]),
  ( 3, [[3,0,0,0], [3,0,0,0], [2,13,0,0], [3,0,12,0]]),
  ( 4, [[2,0,0,0], [3,0,0,0], [3,18,0,0], [2,0,19,0]]),
  ( 5, [[2,0,0,0], [4,0,0,0], [3,19,0,0], [4,0,13,0]]),
  ( 6, [[4,0,0,0], [4,0,0,0], [3, 7,0,0], [4,0,11,0]]),
  ( 7, [[4,0,0,0], [4,0,0,0], [4,15,0,0], [4,0,17,0]]),
  ( 8, [[3,0,0,0], [4,0,0,0], [4,13,0,0], [3,0, 7,0]]),
  ( 9, [[4,0,0,0], [4,0,0,0], [2,12,0,0], [3,0,15,0]]),
  (10, [[4,0,0,0], [3,0,0,0], [4,18,0,0], [4,0,11,0]]),
  (11, [[4,0,0,0], [4,0,0,0], [4, 8,0,0], [2,0,15,0]]),
  (12, [[4,0,0,0], [3,0,0,0], [4, 8,0,0], [3,0, 7,0]]),
  (13, [[4,0,0,0], [3,0,0,0], [3,10,0,0], [2,0,10,0]]),
  (14, [[2,0,0,0], [3,0,0,0], [3,13,0,0], [2,0,20,0]]),
  (15, [[3,0,0,0], [4,0,0,0], [3,19,0,0], [3,0, 8,0]]),
  (16, [[3,0,0,0], [3,0,0,0], [2,16,0,0], [2,0,18,0]]),
  (17, [[4,0,0,0], [4,0,0,0], [2, 9,0,0], [3,0,19,0]]),
  (18, [[4,0,0,0], [4,0,0,0], [2,11,0,0], [4,0, 8,0]]),
  (19, [[3,0,0,0], [4,0,0,0], [3,12,0,0], [3,0,17,0]]),
  (20, [[3,0,0,0], [3,0,0,0], [2,14,0,0], [3,0,17,0]]),
  (21, [[4,0,0,0], [4,0,0,0], [2,15,0,0], [3,0,16,0]]),
  (22, [[4,0,0,0], [4,0,0,0], [2,16,0,0], [4,0,16,0]]),
  (23, [[3,0,0,0], [4,0,0,0], [4,19,0,0], [4,0,11,0]]),
  (24, [[4,0,0,0], [4,0,0,0], [4,18,0,0], [4,0, 9,0]]),
  (25, [[4,0,0,0], [3,0,0,0], [2,17,0,0], [3,0,16,0]]),
  (26, [[3,0,0,0], [4,0,0,0], [2,20,0,0], [4,0, 7,0]]),
  (27, [[2,0,0,0], [2,0,0,0], [2, 8,0,0], [2,0,14,0]]),
  (28, [[3,0,0,0], [4,0,0,0], [3,20,0,0], [3,0,14,0]]),
  (29, [[4,0,0,0], [3,0,0,0], [4,20,0,0], [4,0, 8,0]]),
  (30, [[3,0,0,0], [4,0,0,0], [4,18,0,0], [3,0,13,0]]);


-- add/subtract two robot/resource vectors
CREATE MACRO add(x,y) AS
  [x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4]] :: vec4;

CREATE MACRO sub(x,y) AS
  [x[1]-y[1], x[2]-y[2], x[3]-y[3], x[4]-y[4]] :: vec4;

-- scale a robot/resource vector
CREATE MACRO mul(x,n) AS
  [x[1]*n, x[2]*n, x[3]*n, x[4]*n] :: vec4;

-- maximum minerals of type m required to build a robot with blueprint b :: vec4[]
CREATE MACRO max_minerals(b,m) AS
  if(m = 4, oo(), GREATEST(b[1][m], b[2][m], b[3][m], b[4][m]));

-- we require minerals m :: vec4 and have robots r :: vec4:
-- how many minutes do we have to wait to mine for m?
CREATE MACRO wait(m,r) AS
  1 + COALESCE(GREATEST(if(m[1] > 0 AND r[1] > 0, ceiling(m[1] / r[1]), if(m[1] > 0, oo(), NULL)),
                        if(m[2] > 0 AND r[2] > 0, ceiling(m[2] / r[2]), if(m[2] > 0, oo(), NULL)),
                        if(m[3] > 0 AND r[3] > 0, ceiling(m[3] / r[3]), if(m[3] > 0, oo(), NULL)),
                        if(m[4] > 0 AND r[4] > 0, ceiling(m[4] / r[4]), if(m[4] > 0, oo(), NULL))) :: int,
               0);



WITH RECURSIVE
mining(mins, id, blueprint, resources, robots) AS (
  SELECT minutes() AS mins, i.id, i.blueprint, [0,0,0,0] AS resources, [1,0,0,0] AS robots
  FROM   input AS i
  WHERE  i.id <= blueprints()
    UNION ALL
  (WITH state(mins, id, blueprint, resources, robots, time, p0, p1, p2, p3, p4) AS MATERIALIZED (
     SELECT m.*,
            [wait(sub(m.blueprint[1], m.resources), m.robots),
             wait(sub(m.blueprint[2], m.resources), m.robots),
             wait(sub(m.blueprint[3], m.resources), m.robots),
             wait(sub(m.blueprint[4], m.resources), m.robots)]                              AS time,
            time[4] = 1                                                                     AS p0,
            time[1] BETWEEN 1 AND m.mins - 1 AND m.robots[1] < max_minerals(m.blueprint, 1) AS p1,
            time[2] BETWEEN 1 AND m.mins - 1 AND m.robots[2] < max_minerals(m.blueprint, 2) AS p2,
            time[3] BETWEEN 1 AND m.mins - 1 AND m.robots[3] < max_minerals(m.blueprint, 3) AS p3,
            time[4] BETWEEN 1 AND m.mins - 1 AND m.robots[4] < max_minerals(m.blueprint, 4) AS p4
     FROM   mining AS m
     WHERE  m.mins > 0
   )
    -- build one geode robot
    SELECT  s.mins - 1 AS mins, s.id, s.blueprint,
            add(sub(s.resources, s.blueprint[4]), s.robots) AS resources,
            add(s.robots, [0,0,0,1])                        AS robots
    FROM    state AS s
    WHERE   s.p0
      UNION ALL
    -- build one ore robot
    SELECT  s.mins - s.time[1] AS mins, s.id, s.blueprint,
            add(sub(s.resources, s.blueprint[1]), mul(s.robots, s.time[1])) AS resources,
            add(s.robots, [1,0,0,0])                                        AS robots
    FROM    state AS s
    WHERE   NOT s.p0 AND s.p1
      UNION ALL
    -- build one clay robot
    SELECT  s.mins - s.time[2] AS mins, s.id, s.blueprint,
            add(sub(s.resources, s.blueprint[2]), mul(s.robots, s.time[2])) AS resources,
            add(s.robots, [0,1,0,0])                                        AS robots
    FROM    state AS s
    WHERE   NOT s.p0 AND s.p2
      UNION ALL
    -- build one obsidian robot
    SELECT  s.mins - s.time[3] AS mins, s.id, s.blueprint,
            add(sub(s.resources, s.blueprint[3]), mul(s.robots, s.time[3])) AS resources,
            add(s.robots, [0,0,1,0])                                        AS robots
    FROM    state AS s
    WHERE   NOT s.p0 AND s.p3
      UNION ALL
    -- build one geode robot
    SELECT  s.mins - s.time[4] AS mins, s.id, s.blueprint,
            add(sub(s.resources, s.blueprint[4]), mul(s.robots, s.time[4])) AS resources,
            add(s.robots, [0,0,0,1])                                        AS robots
    FROM    state AS s
    WHERE   NOT s.p0 AND s.p4
      UNION ALL
    -- otherwise: time passes (one minute)
    SELECT  s.mins - 1 AS mins, s.id, s.blueprint,
            add(s.resources, s.robots) AS resources,
            s.robots
    FROM    state AS s
    WHERE   NOT (s.p0 OR s.p1 OR s.p2 OR s.p3 OR s.p4)
  )
)
SELECT AGG(m) AS geodes
FROM   (SELECT id AS blueprint, MAX(resources[4]) AS geodes
        FROM   mining
        GROUP BY id) AS m;

