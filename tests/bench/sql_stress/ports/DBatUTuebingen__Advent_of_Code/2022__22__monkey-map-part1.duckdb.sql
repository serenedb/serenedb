SET integer_division = true;
-- dir is one of '>', 'v', '<', '^'
-- change is on of 'L', 'R'
CREATE MACRO get_col(map, col) AS (
SELECT replace(list_string_agg([m[col] for m in map]), ',', '')
);

CREATE MACRO change_direction(dir, change) AS
(SELECT '>'
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

CREATE MACRO step(pos_x, pos_y, dir, map) AS
(SELECT struct_pack(pos_x := pos_x + 1, pos_y := pos_y, wall := false)
 WHERE dir = '>' AND map[pos_y][pos_x+1] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := position('.' in map[pos_y]), pos_y := pos_y, wall := false)
 WHERE dir = '>' AND (map[pos_y][pos_x+1] = ' ' OR length(map[pos_y]) < pos_x+1) AND ltrim(map[pos_y])[1] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := pos_x, pos_y := pos_y + 1, wall := false)
 WHERE dir = 'v' AND map[pos_y+1][pos_x] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := pos_x, pos_y := position('.' in get_col(map, pos_x)), wall := false)
 WHERE dir = 'v' AND (map[pos_y+1][pos_x] = ' ' OR length(map) < pos_y+1) AND ltrim(get_col(map, pos_x))[1] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := pos_x - 1, pos_y := pos_y, wall := false)
 WHERE dir = '<' AND map[pos_y][pos_x-1] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := length(rtrim(map[pos_y])), pos_y := pos_y, wall := false)
 WHERE dir = '<' AND (map[pos_y][pos_x-1] = ' ' OR  pos_x-1 = 0) AND rtrim(map[pos_y])[-1] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := pos_x, pos_y := pos_y-1, wall := false)
 WHERE dir = '^' AND map[pos_y-1][pos_x] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := pos_x, pos_y := length(rtrim(get_col(map, pos_x))), wall := false)
 WHERE dir = '^' AND (map[pos_y-1][pos_x] = ' ' OR  pos_y-1 = 0) AND rtrim(get_col(map, pos_x))[-1] = '.'
 UNION ALL
 SELECT struct_pack(pos_x := pos_x, pos_y := pos_y, wall := true) -- d´----todo: dieser fall falsch, weil überlauf sagt wand obwohl er weiter laufen könnte.
 WHERE dir = '>' AND (map[pos_y][pos_x+1] = '#' OR ((map[pos_y][pos_x+1] = ' ' OR length(map[pos_y]) < pos_x+1) AND ltrim(map[pos_y])[1] = '#'))
    OR dir = 'v' AND (map[pos_y+1][pos_x] = '#' OR ((map[pos_y+1][pos_x] = ' ' OR length(map) < pos_y+1) AND ltrim(get_col(map, pos_x))[1] = '#'))
    OR dir = '<' AND (map[pos_y][pos_x-1] = '#' OR ((map[pos_y][pos_x-1] = ' ' OR  pos_x-1 = 0) AND rtrim(map[pos_y])[-1] = '#'))
    OR dir = '^' AND (map[pos_y-1][pos_x] = '#' OR ((map[pos_y-1][pos_x] = ' ' OR  pos_y-1 = 0) AND rtrim(get_col(map, pos_x))[-1] = '#'))
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

WITH RECURSIVE input(lines, num) AS (
  SELECT i.lines, ROW_NUMBER() OVER () AS num
  FROM   read_csv_auto('2022/22/input.txt', header=false) AS i(lines)
), init_map(m) AS (
  SELECT list(lines)
  FROM input
  WHERE num <> (SELECT MAX(num) FROM input)
  AND lines <> ''
), map_length(ml) AS (
  SELECT list_max([length(s) for s in m]) :: integer
  FROM init_map
), monkey_map(m) AS (
  SELECT list_transform(m, e -> rpad(e, ml, ' '))
  FROM init_map, map_length
), map_steps(s) AS (
    SELECT [e :: integer for e in string_split_regex(i.lines, '[R|L]')]
    FROM input i
    WHERE i.num = (SELECT MAX(i.num) FROM input i)
), map_directions(d) AS (
  SELECT list_filter(string_split_regex(i.lines, '[0-9]+'), e -> e='R' OR e='L')
  FROM input i
  WHERE i.num = (SELECT max(i.num) FROM input i)
),
-- mode is one of true = step, false = change direction
run (finished, mode, pos_x, pos_y, dir, wall, map, steps, directions) AS (
  SELECT false AS finished,
         true AS mode,
         position('.' in m[1]) AS pos_x,
         1 AS pos_y,
         '>' AS dir,
         false AS wall,
         m AS map,
         s AS steps,
         d AS directions
  FROM monkey_map, map_steps, map_directions

  -- SELECT false, true, 69, 117, '^', false, m, [1, 41], ['R'] FROM monkey_map

  UNION ALL

  (
  -- step
  WITH let0(st) AS (
    SELECT step(r.pos_x, r.pos_y, r.dir, r.map) AS st, r.*
    FROM run r
    WHERE NOT r.finished AND r.mode AND NOT r.wall AND len(r.steps) > 0 AND r.steps[1] > 0
  )
  SELECT r.* EXCLUDE(st) REPLACE(st['pos_x'] AS pos_x,
                                 st['pos_y'] AS pos_y,
                                 st['wall'] AS wall,
                                 array_push_front(array_pop_front(r.steps), r.steps[1]-1) AS steps)
  FROM let0 AS r

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
)
SELECT r.pos_y * 1000 + r.pos_x * 4 + facing(r.dir) AS result
FROM run r
WHERE r.finished;

-- ┌────────┐
-- │ result │
-- │ int64  │
-- ├────────┤
-- │  47462 │
-- └────────┘