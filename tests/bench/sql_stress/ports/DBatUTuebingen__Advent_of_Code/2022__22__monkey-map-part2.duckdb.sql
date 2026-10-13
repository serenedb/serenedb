SET integer_division = true;
PRAGMA temp_directory='tmp.tmp';

-- input source and size of the partial dice map, e.g. for the main input, one side of the dice is a 50x50 map
CREATE MACRO input() AS ('2022/22/input.txt');
CREATE MACRO tiles() AS (50);

CREATE TABLE dice(area integer, border char(1), neighbour integer, rotation integer);

INSERT INTO dice
VALUES
(1, '^', 5, 0),
(1, '>', 2, 0),
(1, 'v', 6, 0),
(1, '<', 4, 0),
(2, '^', 5, 90),
(2, '>', 3, 0),
(2, 'v', 6, 270),
(2, '<', 1, 0),
(3, '^', 5, 180),
(3, '>', 4, 0),
(3, 'v', 6, 180),
(3, '<', 2, 0),
(4, '^', 5, 270),
(4, '>', 1, 0),
(4, 'v', 6, 90),
(4, '<', 3, 0),
(5, '^', 3, 180),
(5, '>', 2, 270),
(5, 'v', 1, 0),
(5, '<', 4, 90),
(6, '^', 1, 0),
(6, '>', 2, 90),
(6, 'v', 3, 180),
(6, '<', 4, 270);

-- dir is one of '>', 'v', '<', '^'
-- change is on of 'L', 'R'
CREATE MACRO change_direction(dir, change) AS (
  SELECT '>'
  WHERE dir = '^' AND change = 'R' OR dir = 'v' AND change = 'L'
  UNION ALL
  SELECT 'v'
  WHERE dir = '>' AND change = 'R' OR dir = '<' AND change = 'L'
  UNION ALL
  SELECT '<'
  WHERE dir = 'v' AND change = 'R' OR dir = '^' AND change = 'L'
  UNION ALL
  SELECT '^'
  WHERE dir = '<' AND change = 'R' OR dir = '>' AND change = 'L'
);

CREATE MACRO facing(dir) AS (
  SELECT 0 WHERE dir = '>'
  UNION ALL
  SELECT 1 WHERE dir = 'v'
  UNION ALL
  SELECT 2 WHERE dir = '<'
  UNION ALL
  SELECT 3 WHERE dir = '^'
);

CREATE MACRO dice_map_part(map, pos_x, pos_y) AS (
  list_transform(map[pos_y:pos_y+tiles()-1], l -> l[pos_x:pos_x+tiles()-1])
);

CREATE MACRO directions(pos_x, pos_y, rotation, pos_x2, pos_y2) AS (
  WITH let0(x, y, rotation) AS (
    SELECT (pos_x2 - pos_x)/tiles() AS x, (pos_y2 - pos_y)/tiles() AS y, radians(-rotation) AS rotation
  ), let1(x,y) AS (
    SELECT l.x * round(cos(l.rotation)) - l.y * round(sin(l.rotation)) AS x, l.x * round(sin(l.rotation)) + l.y * round(cos(l.rotation)) AS y
    FROM let0 AS l
  )
  SELECT '^' FROM let1 l WHERE l.y = -1 AND l.x = 0
  UNION ALL
  SELECT '>' FROM let1 l WHERE l.x = 1  AND l.y = 0
  UNION ALL
  SELECT 'v' FROM let1 l WHERE l.y = 1  AND l.x = 0
  UNION ALL
  SELECT '<' FROM let1 l WHERE l.x = -1 AND l.y = 0
);

-- extract the 6 dice maps: area and rotation tell us where and how the map is placed on the dice
CREATE TABLE dice_map(area integer, map text[], rotation integer, x integer, y integer);
Insert INTO dice_map(
  WITH RECURSIVE input(lines, num) AS (
    -- read csv input
    SELECT i.lines, ROW_NUMBER() OVER () AS num
    FROM   read_csv_auto(input(), header=false) AS i(lines)
  ), init_map(m) AS (
    -- aggregate input into a list to represent the map, filter empty lines
    SELECT list(lines)
    FROM input
    WHERE num <> (SELECT MAX(num) FROM input)
    AND lines <> ''
  ), map_length(ml) AS (
    -- get horizontal length of the map
    SELECT list_max([length(s) for s in m]) :: integer
    FROM init_map
  ), monkey_map(m) AS (
    -- add padding to obtain a full map
    SELECT list_transform(m, e -> rpad(e, ml, ' '))
    FROM init_map, map_length
  ), check_map(pos_x, pos_y, mc) AS (
    -- extract the upper left corner coordinates and content of the 6 dice maps
    SELECT CAST(c2*tiles()+1 AS integer) AS pos_x, CAST(c1*tiles()+1 AS integer) AS pos_y, m[pos_y][pos_x] AS mc
    FROM generate_series(0,3) AS _(c1), generate_series(0,3) AS __(c2), monkey_map
    WHERE mc IS NOT NULL AND mc <> ' ' AND mc <> ''
  -- ) SELECT * FROM check_map
  ), calc_dice_map(i, area, pos_x, pos_y, map, rotation) AS (
    -- map these coordinates to the dice structure in the dice table
    SELECT 1 AS i, 1 AS area, cm.pos_x, cm.pos_y, dice_map_part(mm.m, cm.pos_x, cm.pos_y) AS map, 0 AS rotation
    FROM check_map AS cm, monkey_map AS mm
    WHERE cm.pos_y = 1 AND cm.pos_x = (SELECT MIN(c.pos_x) FROM check_map AS c WHERE c.pos_y = 1)

    UNION ALL

    ( SELECT i+1, * EXCLUDE(i) FROM calc_dice_map WHERE i < 6

      UNION ALL

      SELECT c.i+1,  d.neighbour AS area, cm.pos_x, cm.pos_y, dice_map_part(mm.m, cm.pos_x, cm.pos_y) AS map, mod(c.rotation + d.rotation, 360) AS rotation
      FROM dice d, calc_dice_map c, check_map cm, monkey_map mm
      WHERE directions(c.pos_x, c.pos_y, c.rotation, cm.pos_x, cm.pos_y) = d.border
      AND d.area = c.area
      AND (cm.pos_x, cm.pos_y) NOT IN (SELECT (cdm.pos_x, cdm.pos_y) FROM calc_dice_map cdm)

    )
  )
  SELECT area, map, rotation, pos_x, pos_y
  FROM calc_dice_map
  WHERE i = 6
);

CREATE MACRO rotate_coordinate(pos_x, pos_y, rotation) AS (
  SELECT struct_pack(pos_x := pos_x, pos_y := pos_y, dir_facing := 0)                     WHERE rotation = 0
  UNION ALL
  SELECT struct_pack(pos_x := tiles()+1-pos_y, pos_y := pos_x, dir_facing := 1)           WHERE rotation = 90
  UNION ALL
  SELECT struct_pack(pos_x := tiles()+1-pos_x, pos_y := tiles()+1-pos_y, dir_facing := 2) WHERE rotation = 180
  UNION ALL
  SELECT struct_pack(pos_x := pos_y, pos_y := tiles()+1-pos_x, dir_facing := 3)           WHERE rotation = 270
);

CREATE MACRO rotate_dir(dir, rotation) AS (
  SELECT '>' WHERE dir = '>' AND rotation = 0 OR dir='^' AND rotation = 90 OR dir = '<' AND rotation = 180 OR dir='v' AND rotation = 270
  UNION ALL
  SELECT 'v' WHERE dir = '>' AND rotation = 90 OR dir='^' AND rotation = 180 OR dir = '<' AND rotation = 270 OR dir='v' AND rotation = 0
  UNION ALL
  SELECT '<' WHERE dir = '>' AND rotation = 180 OR dir='^' AND rotation = 270 OR dir = '<' AND rotation = 0 OR dir='v' AND rotation = 90
  UNION ALL
  SELECT '^' WHERE dir = '>' AND rotation = 270 OR dir='^' AND rotation = 0 OR dir = '<' AND rotation = 90 OR dir='v' AND rotation = 180
);

CREATE MACRO step(p_x, p_y, area_label, rotation1, dir) AS (
  WITH new_pos(dice_change, x, y) AS (
    SELECT p_x+1 > tiles() as dice_change, CASE WHEN dice_change THEN 1 ELSE p_x+1 END, p_y       WHERE dir = '>'
    UNION ALL
    SELECT p_y+1 > tiles() AS dice_change, p_x, CASE WHEN dice_change THEN 1 ELSE p_y+1 END       WHERE dir = 'v'
    UNION ALL
    SELECT p_x-1 = 0 AS dice_change,       CASE WHEN dice_change THEN tiles() ELSE p_x-1 END, p_y WHERE dir = '<'
    UNION ALL
    SELECT p_y-1 = 0 AS dice_change,       p_x, CASE WHEN dice_change THEN tiles() ELSE p_y-1 END WHERE dir = '^'
  ), the_map(area, x, y, map, full_rot, new_rot) AS (
    SELECT d.neighbour, n.x, n.y, dm.map, mod(dm.rotation - d.rotation - rotation1 +720, 360), mod(d.rotation+rotation1, 360)
    FROM new_pos n, dice d, dice_map dm
    WHERE n.dice_change AND d.area = area_label AND d.border = rotate_dir(dir, mod(360-rotation1, 360)) AND dm.area = d.neighbour -- FIXME this dir might have to be altered by rotation

    UNION ALL

    SELECT area_label, n.x, n.y, dm.map, mod(360+dm.rotation - rotation1, 360), rotation1
    FROM new_pos n, dice_map AS dm
    WHERE NOT n.dice_change AND dm.area = area_label
  ), get_map_rotated(x, y, area, new_rot, field) AS (
    -- this dice area
    SELECT tm.x, tm.y, tm.area, tm.new_rot, tm.map[tm.y][tm.x]                     FROM the_map AS tm WHERE tm.full_rot = 0
    UNION ALL
    SELECT tm.x, tm.y, tm.area, tm.new_rot, tm.map[tm.x][tiles()+1-tm.y]           FROM the_map AS tm WHERE tm.full_rot = 90
    UNION ALL
    SELECT tm.x, tm.y, tm.area, tm.new_rot, tm.map[tiles()+1-tm.y][tiles()+1-tm.x] FROM the_map AS tm WHERE tm.full_rot = 180
    UNION ALL
    SELECT tm.x, tm.y, tm.area, tm.new_rot, tm.map[tiles()+1-tm.x][tm.y]           FROM the_map AS tm WHERE tm.full_rot = 270
  )
  SELECT struct_pack(pos_x := g.x, pos_y := g.y, area := g.area, rotation := g.new_rot, wall := false)
  FROM get_map_rotated AS g
  WHERE g.field = '.'
  UNION ALL
  SELECT struct_pack(pos_x := p_x, pos_y := p_y, area := area_label, rotation := rotation1, wall := true)
  FROM get_map_rotated AS g
  WHERE g.field = '#'
);

-- mode is one of true = step, false = change direction
-- area is 1..6: one side of the dice
WITH RECURSIVE input(lines, num) AS (
  -- read csv input
  SELECT i.lines, ROW_NUMBER() OVER () AS num
  FROM   read_csv_auto(input(), header=false) AS i(lines)
  ), map_directions(d) AS (
  SELECT list_filter(string_split_regex(i.lines, '[0-9]+'), e -> e='R' OR e='L')
  FROM input i
  WHERE i.num = (SELECT max(i.num) FROM input i)
  ), map_steps(s) AS (
  SELECT [e :: integer for e in string_split_regex(i.lines, '[R|L]')] -- '
  FROM input i
  WHERE i.num = (SELECT MAX(i.num) FROM input i)
  ),
  run(it, finished, mode, pos_x, pos_y, area, rotation, dir, wall, steps, directions) AS (
  (
    SELECT 1 AS it, false AS finished, true AS mode,
           1 AS pos_x, 1 AS pos_y, 1 AS area, 0 AS rotation,
           '>' AS dir, false AS wall,
           s AS steps, d AS directions
    FROM map_steps, map_directions
  )

  UNION ALL

  (
    -- step
    (WITH let3(st) AS (
      SELECT step(r.pos_x, r.pos_y, r.area, r.rotation, r.dir) AS st, r.*
      FROM run AS r
      WHERE NOT r.finished AND r.mode AND NOT r.wall AND len(r.steps) > 0 AND r.steps[1] > 0
    )
    SELECT r.* EXCLUDE(st) REPLACE(r.it + 1 AS it,
                                   st['pos_x'] AS pos_x,
                                   st['pos_y'] AS pos_y,
                                   st['area'] AS area,
                                   st['rotation'] AS rotation,
                                   st['wall'] AS wall,
                                   array_push_front(array_pop_front(r.steps), r.steps[1]-1) AS steps)
    FROM let3 AS r
    )

    UNION ALL

    -- go to direction change
    SELECT r.* REPLACE(false AS mode, false AS wall, array_pop_front(r.steps) AS steps)
    FROM run r
    WHERE NOT r.finished AND ((r.mode AND r.steps[1] = 0) OR r.wall) AND len(r.steps) + len(r.directions) > 0

    UNION ALL

    -- change direction and go to step
    SELECT r.* REPLACE(true AS mode, change_direction(r.dir, r.directions[1]) AS dir, array_pop_front(r.directions) AS directions)
    FROM run r
    WHERE NOT r.finished AND NOT r.mode AND len(r.steps) + len(r.directions) > 0

    UNION ALL

    -- finish
    SELECT r.* REPLACE (true AS finished)
    FROM run r
    WHERE NOT r.finished AND len(r.steps) + len(r.directions) = 0
  )
), run_result(pos_x, pos_y, facing, rotation, area) AS (
  SELECT r.pos_x, r.pos_y, facing(r.dir), r.rotation, r.area
  FROM run r
  WHERE r.finished
), get_coordinates(pos_x, pos_y, facing, x, y, rotation) AS (
  SELECT r.pos_x, r.pos_y, r.facing, dm.x, dm.y, mod(dm.rotation - r.rotation + 360, 360) AS rotation
  FROM run_result r, dice_map dm
  WHERE r.area = dm.area
), rotate_res(pos_x, pos_y, facing, x, y) AS (
  SELECT rr.pos_x           AS pos_x, rr.pos_y           AS pos_y, mod(rr.facing + 0, 4) AS facing, rr.x, rr.y FROM get_coordinates AS rr WHERE rr.rotation = 0
  UNION ALL
  SELECT tiles()+1-rr.pos_y AS pos_x, rr.pos_x           AS pos_y, mod(rr.facing + 1, 4) AS facing, rr.x, rr.y FROM get_coordinates AS rr WHERE rr.rotation = 90
  UNION ALL
  SELECT tiles()+1-rr.pos_x AS pos_x, tiles()+1-rr.pos_y AS pos_y, mod(rr.facing + 2, 4) AS facing, rr.x, rr.y FROM get_coordinates AS rr WHERE rr.rotation = 180
  UNION ALL
  SELECT rr.pos_y           AS pos_x, tiles()+1-rr.pos_x AS pos_y, mod(rr.facing + 3, 4) AS facing, rr.x, rr.y FROM get_coordinates AS rr WHERE rr.rotation = 270
)
SELECT (rr.pos_y + rr.y-1) * 1000 + (rr.pos_x + rr.x-1) * 4 + rr.facing AS result
FROM rotate_res AS rr;
