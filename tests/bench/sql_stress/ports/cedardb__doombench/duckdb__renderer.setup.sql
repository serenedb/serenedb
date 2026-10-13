-- config params concerning the gameplay
DROP TABLE IF EXISTS config;
CREATE TABLE config(
  player_move_speed NUMERIC DEFAULT 0.3,
  player_turn_speed NUMERIC DEFAULT 0.2,
  ammo_max INT DEFAULT 10,
  ammo_refill_interval_seconds INT DEFAULT 2,
  shot_cooldown_seconds FLOAT DEFAULT 0.5);

insert into config (player_move_speed, player_turn_speed, ammo_max, ammo_refill_interval_seconds, shot_cooldown_seconds) values (0.3, 0.2, 10, 2, 0.5);

-- The game map, filled by data/map.sql.
-- The map is a grid of tiles, each tile is either a wall '#', a floor '.' or a respawn point 'R'.
DROP TABLE IF EXISTS map;
CREATE TABLE map(x INT, y INT, tile CHAR, PRIMARY KEY (x, y));

-- Player inputs. To be updated by the game client. May either be a direction ('w', 'a', 's', 'd') or an action (currently only 'x' for shooting).
DROP TABLE IF EXISTS inputs;
CREATE TABLE inputs(
  player_id INT PRIMARY KEY,
  action CHAR, -- 'w', 'a', 's', 'd', 'x' for shooting
  timestamp TIMESTAMP DEFAULT NOW()
);

-- Settings for the renderer
DROP TABLE IF EXISTS settings;
CREATE TABLE settings(fov DOUBLE PRECISION, step DOUBLE PRECISION, max_steps INT, view_w INT, view_h INT);
INSERT INTO settings VALUES (PI()/3, 0.1, 100, 128, 64);

-- A catalog of all sprites used in the game
CREATE TABLE IF NOT EXISTS sprites (
  id INTEGER PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  w INT NOT NULL,
  h INT NOT NULL
);


-- Sprite pixels: (0,0) is top-left.
CREATE TABLE IF NOT EXISTS sprite_pixels (
  sprite_id INT,
  sx INT NOT NULL,
  sy INT NOT NULL,
  ch TEXT,  -- single char; NULL or ' ' for transparent
  PRIMARY KEY (sprite_id, sx, sy)
);

-- Generic MOBs (players, bullets, monsters, items, ...)
CREATE TABLE IF NOT EXISTS mobs (
  id serial PRIMARY KEY,
  kind TEXT NOT NULL,                   -- 'player', 'bullet', 'monster', ...
  owner INT,
  x DOUBLE PRECISION NOT NULL,
  y DOUBLE PRECISION NOT NULL,
  dir DOUBLE PRECISION DEFAULT 0,
  world_w DOUBLE PRECISION default 1, -- width in world tiles
  world_h DOUBLE PRECISION default 1, -- height in world tiles
  name TEXT,
  sprite_id INT,
  minimap_icon TEXT
);

-- (Human) players
CREATE TABLE IF NOT EXISTS players (
  id INT,
  score INT DEFAULT 0,
  hp INT DEFAULT 100,
  ammo INT DEFAULT 10,
  last_ammo_refill int default EXTRACT(EPOCH FROM (now()))::int,
  last_shot_time FLOAT DEFAULT 0,
  spawn_x FLOAT,
  spawn_y FLOAT,
  spawn_dir FLOAT
);

-- A helper view to find collisions between bullets and players
-- We create this here once so we don't have to repeat ourselves in the gameloop
CREATE OR REPLACE VIEW collisions AS
  SELECT m.id AS bullet_id,
    m.owner as bullet_owner,
    p.id AS player_id,
    p_m.x AS player_x,
    p_m.y AS player_y
  FROM mobs m, players p, mobs p_m
  WHERE CAST(m.x AS INT) = CAST(p_m.x AS INT)
  AND CAST(m.y AS INT) = CAST(p_m.y AS INT)
  AND m.kind = 'bullet'
  AND p_m.id = p.id
  AND m.owner != p.id; -- Ensure the bullet is not from the player being hit
insert into sprites(id, name, w, h)   VALUES (0, 'marine_outline_16x20', 16, 20);
insert into sprites(id, name, w, h)   VALUES (1, 'marine_outline_12x15', 12, 15);
insert into sprites(id, name, w, h)   VALUES (2, 'marine_outline_8x10', 8, 10);

with art AS (
  SELECT
    0 AS sprite_id,
    t.line,
    t.sy::int - 1 AS sy -- 0-based y
  FROM unnest(ARRAY[
    '.....@@@@@@.....',
    '....##@@@@##....',
    '....#@@##@@#....',
    '....#@#..#@#....',
    '....#@@@@@@#....',
    '...#@@@@@@@@#...',
    '...#@@@&&@@@#...',
    '..##@@&##&@@##..',
    '..#@@@####@@@#..',
    '..#==@@@@@@==#..',
    '..#@@@####@@@#..',
    '..#@@@#..#@@@#..',
    '..#@@#....#@@#..',
    '..#@@#....#@@#..',
    '..#@@#....#@@#..',
    '..#@@#....#@@#..',
    '...##......##...',
    '...##......##...',
    '....#......#....',
    '....##....##....'
  ]) WITH ORDINALITY AS t(line, sy)
),
expanded AS (
  SELECT
    a.sprite_id,
    x % 16 as sx,
    x / 16 as sy,
    SUBSTRING(a.line FROM (x % 16)+1 FOR 1) AS raw_ch
  FROM generate_series(0, 16 * 20) AS x(x), art a
  WHERE a.sy = x / 16
)
INSERT INTO sprite_pixels (sprite_id, sx, sy, ch)
SELECT
  sprite_id, sx, sy,
  CASE WHEN raw_ch = '.' THEN NULL ELSE raw_ch END AS ch
FROM expanded;



with art AS (
  SELECT
    1 AS sprite_id,
    t.line,
    t.sy::int - 1 AS sy -- 0-based y
  FROM unnest(ARRAY[
    '...@@@@@@...',
    '..##@@@@##..',
    '..#@@##@@#..',
    '..#@#..#@#..',
    '..#@@@@@@#..',
    '.#@@@@@@@@#.',
    '.#@@@&&@@@#.',
    '.#@@&##&@@#.',
    '.#@@####@@#.',
    '.#==@@@@==#.',
    '.#@@####@@#.',
    '.#@@#..#@@#.',
    '.#@@....@@#.',
    '..##....##..',
    '...#....#...'
  ]) WITH ORDINALITY AS t(line, sy)
),
expanded AS (
  SELECT
    a.sprite_id,
    x % 12 as sx,
    x / 12 as sy,
    SUBSTRING(a.line FROM (x % 12)+1 FOR 1) AS raw_ch
  FROM generate_series(0, 12 * 15) AS x(x), art a
  WHERE a.sy = x / 12
)
INSERT INTO sprite_pixels (sprite_id, sx, sy, ch)
SELECT
  sprite_id, sx, sy,
  CASE WHEN raw_ch = '.' THEN NULL ELSE raw_ch END AS ch
FROM expanded;


with art AS (
  SELECT
    2 AS sprite_id,
    t.line,
    t.sy::int - 1 AS sy -- 0-based y
  FROM unnest(ARRAY[
    '..@@@@..',
    '.#@@@@#.',
    '.#@##@#.',
    '.#@..@#.',
    '.#@@@@#.',
    '.#@&&@#.',
    '.#@##@#.',
    '.#==@#..',
    '..##..##',
    '...#..#.'
  ]) WITH ORDINALITY AS t(line, sy)
),
expanded AS (
  SELECT
    a.sprite_id,
    x % 8 as sx,
    x / 8 as sy,
    SUBSTRING(a.line FROM (x % 8)+1 FOR 1) AS raw_ch
  FROM generate_series(0, 8 * 10) AS x(x), art a
  WHERE a.sy = x / 8
)
INSERT INTO sprite_pixels (sprite_id, sx, sy, ch)
SELECT
  sprite_id, sx, sy,
  CASE WHEN raw_ch = '.' THEN NULL ELSE raw_ch END AS ch
FROM expanded;
insert into sprites(id, name, w, h)   VALUES (3, 'shot_slug_away_12x12', 12, 12);
insert into sprites(id, name, w, h)   VALUES (4, 'shot_slug_away_6x6', 6, 6);

with art AS (
  SELECT
    3 AS sprite_id,
    t.line,
    t.sy::int - 1 AS sy -- 0-based y
  FROM unnest(ARRAY[
    '.....++.....',
    '....+@@+....',
    '...+@@@@+...',
    '..+@@@@@@+..',
    '..+@@@@@@+..',
    '.+@@@@@@@@+.',
    '.+@@@@@@@@+.',
    '..+@@@@@@+..',
    '..+@@@@@@+..',
    '...+@@@@+...',
    '....+@@+....',
    '.....++.....'
  ]) WITH ORDINALITY AS t(line, sy)
),
expanded AS (
  SELECT
    a.sprite_id,
    x % 12 as sx,
    x / 12 as sy,
    SUBSTRING(a.line FROM (x % 12)+1 FOR 1) AS raw_ch
  FROM generate_series(0, 12 * 12) AS x(x), art a
  WHERE a.sy = x / 12
)
INSERT INTO sprite_pixels (sprite_id, sx, sy, ch)
SELECT
  sprite_id, sx, sy,
  CASE WHEN raw_ch = '.' THEN NULL ELSE raw_ch END AS ch
FROM expanded;



with art AS (
  SELECT
    4 AS sprite_id,
    t.line,
    t.sy::int - 1 AS sy -- 0-based y
  FROM unnest(ARRAY[
    '..+...',
    '.+@@..',
    '+@@@+.',
    '+@@@+.',
    '.+@@..',
    '..+...'
  ]) WITH ORDINALITY AS t(line, sy)
),
expanded AS (
  SELECT
    a.sprite_id,
    x % 6 as sx,
    x / 6 as sy,
    SUBSTRING(a.line FROM (x % 6)+1 FOR 1) AS raw_ch
  FROM generate_series(0, 6 * 6) AS x(x), art a
  WHERE a.sy = x / 6
)
INSERT INTO sprite_pixels (sprite_id, sx, sy, ch)
SELECT
  sprite_id, sx, sy,
  CASE WHEN raw_ch = '.' THEN NULL ELSE raw_ch END AS ch
FROM expanded;
-- A big map with multiple rooms and corridors
TRUNCATE TABLE map;

WITH params AS (
  SELECT 64::int AS w, 32::int AS h
),

-- Full grid (defaults to walls)
grid AS (
  SELECT x, y
  FROM params p,
       generate_series(0, p.w - 1) AS x,
       generate_series(0, p.h - 1) AS y
),

-- Rectangles to carve as FLOOR (rooms + corridors)
-- Format: (x1,y1,x2,y2), all inclusive; corridors are just skinny rects (>= 2 wide)
areas AS (
  SELECT * FROM (VALUES
    -- ROOMS (big rectangles)
    (  2,  2, 14, 10),   -- Room A
    ( 20,  2, 34,  9),   -- Room B
    ( 40,  3, 60, 12),   -- Room C
    (  4, 14, 20, 28),   -- Room D
    ( 26, 16, 38, 28),   -- Room E
    ( 44, 16, 60, 28),   -- Room F

    -- CORRIDORS (>= 2 tiles wide)
    ( 15,  6, 20,  7),   -- A ↔ B (horizontal, 2 high)
    ( 35,  6, 40,  7),   -- B ↔ C (horizontal, 2 high)
    ( 10, 11, 11, 14),   -- A ↔ D (vertical, 2 wide: x=10..11)
    ( 20, 21, 26, 22),   -- D ↔ E (horizontal, 2 high)
    ( 38, 22, 44, 23),   -- E ↔ F (horizontal, 2 high)
    ( 28,  9, 29, 16),   -- B ↔ E (vertical, 2 wide)
    ( 52, 12, 53, 16)    -- C ↔ F (vertical, 2 wide)
  ) AS t(x1,y1,x2,y2)
),

-- Normalize any reversed coords
normalized AS (
  SELECT LEAST(x1,x2) AS x1, LEAST(y1,y2) AS y1,
         GREATEST(x1,x2) AS x2, GREATEST(y1,y2) AS y2
  FROM areas
),

-- Find rooms that are at least 6 by 6 (i.e., rooms that are suitable for spawn points).
room_boxes AS (
  SELECT *
  FROM normalized
  WHERE (x2 - x1 + 1) >= 6 AND (y2 - y1 + 1) >= 6
),

-- One respawn at the center of each room
respawns AS (
  SELECT floor((x1 + x2) / 2.0)::int AS x,
         floor((y1 + y2) / 2.0)::int AS y
  FROM room_boxes
),

-- Expand rectangles into floor cells 
floors AS (
  SELECT x, y
  FROM normalized a
  JOIN LATERAL generate_series(a.x1, a.x2) AS x ON TRUE
  JOIN LATERAL generate_series(a.y1, a.y2) AS y ON TRUE
),

-- Compose final map: floors '.' else walls '#'
map_rows AS (
  SELECT g.x, g.y,
         CASE 
          WHEN EXISTS (SELECT 1 FROM respawns r WHERE r.x = g.x AND r.y = g.y)
             THEN 'R'::char
          WHEN EXISTS (SELECT 1 FROM floors f WHERE f.x = g.x AND f.y = g.y)
              THEN '.'::char 
          ELSE '#'::char END AS tile
  FROM grid g
)

INSERT INTO map(x, y, tile)
SELECT x, y, tile FROM map_rows;
-- Act 1 scene setup:
--   Players 1 & 2 duel in Room A — facing each other, bullet mid-flight between them.
--   Players 3 & 4 face nearby walls (Room D south, Room F east) for strong wall rendering.

INSERT INTO mobs(kind, x, y, dir, name, sprite_id, minimap_icon) VALUES
  ('player',  4.0,  6.0,  0.117,  'Player1', 0, '1'),  -- Room A west, aimed at Player 2 (atan2(1,8.5))
  ('player', 12.5,  7.0,  3.258,  'Player2', 0, '2'),  -- Room A east, aimed back at Player 1 (π+0.117)
  ('player', 12.0, 24.5,  1.5708, 'Player3', 0, '3'),  -- Room D, facing south wall (~4.5 tiles away)
  ('player', 57.5, 22.0,  0.0,    'Player4', 0, '4'),  -- Room F, facing east wall (~3.5 tiles away)
  ('bullet',  8.0,  6.0,  0.0,    'Bullet',  4, '*');  -- Room A, mid-flight east from P1 toward P2

INSERT INTO players(id, spawn_x, spawn_y, spawn_dir) VALUES
  (1,  4.0,  6.0,  0.117),
  (2, 12.5,  7.0,  3.258),
  (3, 12.0, 24.5,  1.5708),
  (4, 57.5, 22.0,  0.0);
