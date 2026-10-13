SET integer_division = true;
-- AoC 2022, Day 14 (Part 2)

-- (1) Rock parsing
CREATE TEMPORARY TABLE rocks AS
  WITH RECURSIVE
  input(path, point) AS (
    SELECT ROW_NUMBER() OVER () AS row,
           unnest(list_apply(string_split(c.line, ' -> '),
                             p -> string_split(p, ',') :: int[]))
    FROM   read_csv_auto('2022/14/input.txt', SEP='', header=false) AS c(line)
  ),
  paths(path, pos, x, y) AS (
    SELECT i.path, ROW_NUMBER() OVER (PARTITION BY i.path) AS pos,
           i.point[1] AS x, i.point[2] AS y
    FROM   input AS i
  ),
  segments("from", "to", Δ) AS (
    SELECT {x:p1.x, y:p1.y} AS from, {x:p2.x, y:p2.y} AS to,
           {x:sign(p2.x - p1.x), y:sign(p2.y - p1.y)} AS Δ
    FROM   paths AS p1, paths AS p2
    WHERE  p1.path = p2.path AND p2.pos = p1.pos + 1
  ),
  rocks("from", "to", Δ) AS (
    TABLE segments
      UNION ALL
    SELECT {x:r.from.x + r.Δ.x, y:r.from.y + r.Δ.y} AS from, r.to, r.Δ
    FROM   rocks AS r
    WHERE  r.from <> r.to
  )
  SELECT DISTINCT r.from.x, r.from.y
  FROM   rocks AS r;


-- (2) Sand simulation, sand is dropped from (x,y) = (500,0)
WITH RECURSIVE
sand(grain) AS (
  SELECT {rest:0, x:500, y:0, path:[]::struct(x int, y int)[], rocks:list({x:r.x, y:r.y})} AS grain
  FROM   rocks AS r

    UNION ALL

  SELECT CASE list_apply([{x:s.grain.x-1, y:s.grain.y+1},
                          {x:s.grain.x  , y:s.grain.y+1},
                          {x:s.grain.x+1, y:s.grain.y+1}],
                         r -> CASE WHEN list_contains(s.grain.rocks, r) THEN '#' ELSE '.' END)
           -- sand movement rules:
           --  o  →  .  |   o  →  .  |  o  →  .  |  o  →  .  |  o  →  .  |  o  →  .  |  o  →  o   (rest)
           -- ...    .o. |  ..#    .o# | .##    o## | #..    #o. | #.#    #o# | ##.    ##o | ###    ###
           WHEN ['.','.','.'] THEN
                         -- grains comes to rest in any case if its y coordinate reaches MAX(y) + 2
                         CASE WHEN s.grain.y+1 < (SELECT MAX(y) + 2 FROM rocks)
                              THEN {rest:s.grain.rest  , x:s.grain.x        , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
                              ELSE {rest:s.grain.rest+1, x:s.grain.path[1].x, y:s.grain.path[1].y, path:array_pop_front(s.grain.path),                              rocks:list_prepend({x:s.grain.x, y:s.grain.y},
                                                                                                                                                                                       s.grain.rocks)}
                         END
           WHEN ['.','.','#'] THEN {rest:s.grain.rest  , x:s.grain.x        , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
           WHEN ['.','#','.'] THEN {rest:s.grain.rest  , x:s.grain.x-1      , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
           WHEN ['.','#','#'] THEN {rest:s.grain.rest  , x:s.grain.x-1      , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
           WHEN ['#','.','.'] THEN {rest:s.grain.rest  , x:s.grain.x        , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
           WHEN ['#','.','#'] THEN {rest:s.grain.rest  , x:s.grain.x        , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
           WHEN ['#','#','.'] THEN {rest:s.grain.rest  , x:s.grain.x+1      , y:s.grain.y+1      , path:array_push_front(s.grain.path, {x:s.grain.x, y:s.grain.y}), rocks:s.grain.rocks}
           WHEN ['#','#','#'] THEN
                         -- finish run once even the grain entry at (500,0) is blocked
                         CASE WHEN (s.grain.x, s.grain.y) <> (500,0)
                              THEN {rest:s.grain.rest+1, x:s.grain.path[1].x, y:s.grain.path[1].y, path:array_pop_front(s.grain.path),                              rocks:list_prepend({x:s.grain.x, y:s.grain.y},
                                                                                                                                                                                       s.grain.rocks)}
                              ELSE {rest:s.grain.rest+1, x:NULL             , y:NULL             , path:s.grain.path,                                               rocks:s.grain.rocks}
                         END
         END AS grain
  FROM   sand AS s
  WHERE  s.grain.y IS NOT NULL
)
SELECT MAX(s.grain.rest) AS rested
FROM   sand AS s;
