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
  id SERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  w INT NOT NULL,
  h INT NOT NULL
);


-- Sprite pixels: (0,0) is top-left.
CREATE TABLE IF NOT EXISTS sprite_pixels (
  sprite_id INT REFERENCES sprites(id),
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
  sprite_id INT REFERENCES sprites(id),
  minimap_icon TEXT
);

-- (Human) players
CREATE TABLE IF NOT EXISTS players (
  id INT REFERENCES mobs(id),
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
