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
-- We start by casting a ray for each column of the player's view.
-- We get away with only using columns (and not also rows)
-- because the game is 2.5D: There is nothing happening at half height,
-- except for sprites which are rendered separately anyway.
CREATE OR REPLACE VIEW rays AS
WITH cols AS (
  -- For each column ...
    SELECT pc.col FROM settings s, generate_series(0, s.view_w) as pc(col)
  )
SELECT p.id as player_id, -- ... and each player ...
  m.x AS player_x,
  m.y AS player_y,
  c.col,
  -- ... calculate the angle of the ray relative to the player's direction
  (m.dir - s.fov/2.0 + s.fov * (c.col*1.0 / (s.view_w - 1))) AS angle
FROM cols c
CROSS JOIN settings s
CROSS JOIN players p
JOIN mobs m ON p.id = m.id;

-- Next, we calculate for all players, which tiles are visible
-- from their current position and orientation using the rays we've
-- calculated before.
CREATE OR REPLACE VIEW visible_tiles AS
WITH RECURSIVE raytrace AS (
  -- The heart of our algorithm.
    SELECT
      -- Starting at the player's eye ...
      r.player_id,
      r.col,
      1 as step_count,
      r.player_x + COS(r.angle)*s.step as fx,
      r.player_y + SIN(r.angle)*s.step as fy,
      r.angle,
      0::double precision as dist
    FROM rays r
    CROSS JOIN settings s

    UNION ALL

    SELECT
      -- ... we recursively march along the rays, 1 "step" at a time ...
      rt.player_id as player_id,
      rt.col as col,
      rt.step_count + 1 as step_count,
      rt.fx + COS(rt.angle)*s.step as fx,
      rt.fy + SIN(rt.angle)*s.step as fy,
      rt.angle,
      step_count * s.step * COS(rt.angle - m.dir) as dist
    FROM settings s
    CROSS JOIN raytrace rt
    JOIN players p ON p.id = rt.player_id
    JOIN mobs m ON m.id = p.id
    WHERE rt.step_count < s.max_steps -- ... stopping after our max render distance
      AND NOT EXISTS ( -- or if we hit a wall
        SELECT 1
        FROM map m
        WHERE m.x = CAST(rt.fx AS INT)
          AND m.y = CAST(rt.fy AS INT)
          AND m.tile = '#') -- Wall
    )
  SELECT DISTINCT
   -- We then determine per player:
   --  a) which tiles we hit
   --  b) how far away these tiles are
   --  c) the column of the screen each tile should correspond to
    rt.player_id,
    m.tile,
    CAST(rt.fx AS INT) AS tile_x,
    CAST(rt.fy AS INT) AS tile_y,
    col,
    min(dist) as dist -- We might hit the same tile multiple times, so we take the closest hit
  FROM raytrace rt
  JOIN map m ON m.x = CAST(rt.fx AS INT) AND m.y = CAST(rt.fy AS INT)
  GROUP BY tile_x, tile_y, m.tile, col, player_id;

-- Render the 3D viewport for each player. Ignore mobs for now.
CREATE OR REPLACE VIEW render_3d_frame AS
WITH
  -- Generate the columns and rows of the viewport, i.e. all the pixels we have to render
  cols AS (
    SELECT pc.col
    FROM settings s
    CROSS JOIN generate_series(0, s.view_w) as pc(col)
  ),
  rows_gen AS ( -- "rows" is already a keyword in SQL :(
    SELECT pr.row FROM settings s
    CROSS JOIN generate_series(0, s.view_h) as pr(row)
  ),
  visible_walls AS (
    -- Which walls are visible for each player, i.e. not occluded by another wall?
    SELECT
      player_id,
      vt.col,
      min(dist) as dist -- i.e., the first wall we hit
    FROM map m
    JOIN visible_tiles vt ON vt.tile_x = m.x AND vt.tile_y = m.y
    WHERE m.tile = '#'
    GROUP BY player_id, vt.col
  ),
  distances AS (
    -- Annotate each hit wall with the distance and angle relative to the player
    select r.player_id,
      r.col,
      r.angle,
      coalesce(v.dist, s.max_steps * s.step) as dist
    FROM settings s
    CROSS JOIN rays r
    LEFT JOIN visible_walls v ON r.col = v.col AND r.player_id = v.player_id
  ),
  heights AS (
    -- Even though all walls have the same height, their height on the screen depends on the distance.
    -- Calculate the height of all visible walls for all players.
    SELECT
      p.id as player_id,
      d.col,
      CASE WHEN d.dist <= 0
        THEN s.view_h
        ELSE GREATEST(0, LEAST(s.view_h, CAST(s.view_h / (d.dist * COS(d.angle - m.dir)) AS INT)))
      END AS height
    FROM settings s
    CROSS JOIN distances d
    JOIN players p ON p.id = d.player_id
    JOIN mobs m ON m.id = p.id
  ),
  pixels AS (
    -- We can finally render all the pixels!
    SELECT
      h.player_id,
      c.col AS x,
      rg.row AS y,
      CASE WHEN rg.row < (s.view_h - h.height) / 2 THEN ' '  -- Sky
        WHEN rg.row >= (s.view_h + h.height) / 2 THEN '.' -- Floor
        WHEN d.dist < s.max_steps * s.step / 4 THEN '█' -- Close wall
        WHEN d.dist < s.max_steps * s.step * 2 / 4 THEN '▓' -- Mid wall
        WHEN d.dist < s.max_steps * s.step * 3 / 4 THEN '▒' -- Far wall
        ELSE '░' END AS ch -- Very far wall
      FROM cols c
      JOIN heights h ON c.col = h.col
      JOIN distances d ON h.player_id = d.player_id AND h.col = d.col
      CROSS JOIN rows_gen rg
      CROSS JOIN settings s)
SELECT player_id, x, y, ch FROM pixels ORDER BY y, x;

-- Now the real fun begins: The full 3D frame including all entities
CREATE OR REPLACE VIEW game_view as
WITH
-- Gather settings and player info we need multiple times, once
config as (
  SELECT
    p.id as player_id,
    m.x as player_x,
    m.y as player_y,
    m.dir as player_dir,
    s.view_w / (2 * tan(s.fov / 2)) as projection_factor
  FROM settings s
  CROSS JOIN players p
  JOIN mobs m ON p.id = m.id
),
-- Compute relative coordinates, depth, screen_x, etc.
projected_mobs as (
  SELECT
    c.player_id,
    m.*,
    (m.x - c.player_x) * cos(-c.player_dir) - (m.y - c.player_y) * sin(-c.player_dir) as depth,
    (m.x - c.player_x) * sin(-c.player_dir) + (m.y - c.player_y) * cos(-c.player_dir) as horiz
  FROM mobs m
  CROSS JOIN config c
),
-- Project all visible mobs onto screen, filtering behind-camera entities
screen_mobs as (
  SELECT
    pm.*,
    ROUND(s.view_w / 2 + (pm.horiz / pm.depth) * c.projection_factor) AS screen_x_center,
    ROUND(s.view_h / 2) AS screen_y_center -- Center vertically
  FROM projected_mobs pm
  CROSS JOIN settings s
  CROSS JOIN config c
  WHERE pm.depth > 0.1
),

-- Wall distances per column
column_distances AS (
  SELECT player_id, col, MAX(dist) AS dist
  FROM visible_tiles
  GROUP BY player_id, col
),

-- We have multiple LODs for some sprites, so we need to select the right one
-- TODO: It would be better to have a generic LOD lookup table instead of
-- TODO: hardcoding this here, but this will do for now.
bullet_lods AS (
  SELECT
    (SELECT id FROM sprites WHERE name = 'shot_slug_away_12x12') AS near_id,
    (SELECT id FROM sprites WHERE name = 'shot_slug_away_6x6')  AS far_id
),
marine_lods AS (
  SELECT
    (SELECT id FROM sprites WHERE name = 'marine_outline_16x20') AS near_id,
    (SELECT id FROM sprites WHERE name = 'marine_outline_12x15') AS mid_id,
    (SELECT id FROM sprites WHERE name = 'marine_outline_8x10')  AS far_id
),
screen_mobs_lod AS (
  SELECT
    sm.*,
    CASE
      WHEN sm.kind = 'bullet' AND sm.depth > 4 THEN blod.far_id
      WHEN sm.kind = 'player' AND sm.depth > 6 THEN ml.far_id
      WHEN sm.kind = 'player' AND sm.depth > 3 THEN ml.mid_id
      ELSE sm.sprite_id
    END AS effective_sprite_id
  FROM screen_mobs sm
  CROSS JOIN bullet_lods blod
  CROSS JOIN marine_lods ml
),

-- Project sprite pixels for each visible MOB
expanded_sprite_pixels AS (
  SELECT
    sm.player_id,
    sm.id AS mob_id,
    sm.depth,
    sp.sx, sp.sy, sp.ch,
    spr.w, spr.h,
    sm.screen_x_center,
    sm.screen_y_center,
    (c.projection_factor * sm.world_w / sm.depth) / spr.w AS scale_x,
    (c.projection_factor * sm.world_h / sm.depth) / spr.h AS scale_y
  FROM config c, screen_mobs_lod sm
  JOIN sprites spr ON spr.id = sm.effective_sprite_id
  JOIN sprite_pixels sp ON sp.sprite_id = spr.id
  WHERE sp.ch IS NOT NULL AND sp.ch <> ' ' -- Only non-transparent pixels
),
-- Convert sprite local pixel coords to screen coords
sprite_screen_pixels AS (
  WITH base AS (
    SELECT
      esp.player_id,
      esp.mob_id,
      esp.depth,
      esp.ch,
      esp.sx, esp.sy,
      esp.scale_x,
      esp.scale_y,
      esp.w, esp.h,
      (esp.screen_x_center - ROUND((esp.w/2.0) * esp.scale_x))::int AS ax,
      (esp.screen_y_center - ROUND((esp.h/2.0) * esp.scale_y))::int AS ay
    FROM expanded_sprite_pixels esp
  ),
  spans AS (
    SELECT
      b.*,
      -- horizontal span for this texel
      (b.ax + FLOOR(b.sx * b.scale_x))::int AS x0_raw,
      (b.ax + FLOOR((b.sx + 1) * b.scale_x) - 1)::int AS x1_raw,
      -- vertical span for this texel
      (b.ay + FLOOR(b.sy * b.scale_y))::int AS y0_raw,
      (b.ay + FLOOR((b.sy + 1) * b.scale_y) - 1)::int AS y1_raw
    FROM base b
    WHERE b.ch IS NOT NULL
  ),
  clamped AS (
    SELECT
      s.player_id, s.mob_id, s.depth, s.ch, se.view_w, se.view_h,
      GREATEST(s.x0_raw, 0)                          AS x0,
      LEAST(GREATEST(s.x1_raw, s.x0_raw), se.view_w-1) AS x1,  -- ensure x1 >= x0 and on-screen
      GREATEST(s.y0_raw, 0)                          AS y0,
      LEAST(GREATEST(s.y1_raw, s.y0_raw), se.view_h-1) AS y1   -- ensure y1 >= y0 and on-screen
    FROM spans s, settings se
  )
  SELECT
    c.player_id, c.mob_id, c.depth, c.ch,
    px AS px, py AS py
  FROM clamped c
  JOIN LATERAL generate_series(c.x0, c.x1) AS px ON TRUE
  JOIN LATERAL generate_series(c.y0, c.y1) AS py ON TRUE
),

-- Keep only sprite pixels that are in front of the wall
visible_sprite_pixels AS (
  SELECT
    ssp.*,
    cd.dist AS wall_depth
  FROM sprite_screen_pixels ssp
  LEFT JOIN column_distances cd
    ON cd.player_id = ssp.player_id
   AND cd.col = ssp.px
   WHERE depth < COALESCE(cd.dist, 1e9)
),

-- Keep the closest MOB pixel per screen (x,y)
closest_sprite_pixel AS (
  SELECT DISTINCT ON (player_id, px, py)
    player_id, px, py, ch, depth
  FROM visible_sprite_pixels
  ORDER BY player_id, px, py, depth ASC
),
-- Overlay the MOB sprite pixels on top of the base frame
patched_framebuffer AS (
  SELECT
    bf.player_id,
    bf.y,
    bf.x,
    COALESCE(csp.ch, bf.ch) AS ch
  FROM render_3d_frame bf -- "base frame"
  LEFT JOIN closest_sprite_pixel csp
    ON csp.player_id = bf.player_id AND csp.px = bf.x AND csp.py = bf.y
),
-- Reconstruct each row from characters
final_frame AS (
  SELECT
    player_id,
    y,
    string_agg(ch, '' ORDER BY x) as row
  FROM patched_framebuffer
  GROUP BY player_id, y
)
-- Now we have our final game view!
select * from final_frame ORDER BY y;


--- Let's render our minimap

CREATE OR REPLACE VIEW minimap AS
-- Find the bounding box, we don't want to allow just rectangular maps
with dimensions AS (
  SELECT max(x) as max_x, max(y) as max_y FROM map
),
-- Render the bounding box
grid AS (
  SELECT x, y
  FROM dimensions d
  CROSS JOIN generate_series(0, d.max_x) as x
  CROSS JOIN generate_series(0, d.max_y) as y
),
-- Get all tiles visible per player
tiles_to_display AS (
  SELECT player_id, tile_x, tile_y, min(tile) as tile
  FROM visible_tiles
  GROUP BY player_id, tile_x, tile_y
),
-- MOB overlay, only where tile is visible
mobs_overlay AS (
  SELECT
    t.player_id,
    FLOOR(m.x)::int AS x,
    FLOOR(m.y)::int AS y,
    COALESCE(m.minimap_icon, '?') AS ch -- Fallback: '?' for mobs without icon
  FROM mobs m
  JOIN tiles_to_display t
    ON FLOOR(m.x)::int = t.tile_x AND FLOOR(m.y)::int = t.tile_y
),
-- Combine overlays in draw order.
-- Explicit CASE avoids CHAR(1)/TEXT type ambiguity that causes Postgres to produce
-- '' instead of ' ' for unseen floor tiles when using nested COALESCE(CASE...).
combined AS (
  SELECT pl.id AS player_id, g.x, g.y,
    CASE
      WHEN mo.ch IS NOT NULL              THEN mo.ch
      WHEN t.tile IS NOT NULL             THEN CASE WHEN t.tile = 'R' THEN '.' ELSE t.tile::text END
      WHEN base.tile::text = '#'          THEN '#'
      ELSE ' '
    END AS ch
  FROM players pl
  CROSS JOIN grid g
  LEFT JOIN map base ON g.x = base.x AND g.y = base.y
  LEFT JOIN tiles_to_display t ON g.x = t.tile_x AND g.y = t.tile_y AND pl.id = t.player_id
  LEFT JOIN mobs_overlay mo ON g.x = mo.x AND g.y = mo.y AND pl.id = mo.player_id
),
-- Reconstruct lines
lines AS (
  SELECT
    player_id,
    y,
    string_agg(ch, '' ORDER BY x) as row
  FROM combined
  GROUP BY player_id, y
)
SELECT *
FROM lines;

-- Finally, let's combine everything into a nice screen for each player, using fancy borders.
CREATE OR REPLACE VIEW screen AS
WITH
-- Raw minimap rows
mm AS (
  SELECT player_id, y, row
  FROM minimap
),
-- Per-player minimap dims
mm_dims AS (
  SELECT
         MAX(LENGTH(row))::int AS mm_w,
         MAX(y)           AS y_max
  FROM mm
),
-- Frame the 3D game view with borders
gameview_lines AS (
  SELECT g.player_id,
         g.y,
         '║' || rpad(g.row, view_w, ' ') || '║' AS view_col
  FROM game_view g
  CROSS JOIN settings s
),
gameview_frame AS (
  SELECT p.id as player_id, -1 AS y, '╔' || repeat('═', view_w) || '╗' AS view_col
  FROM players p
  CROSS JOIN settings s

  UNION ALL

  SELECT *
  FROM gameview_lines

  UNION ALL

  SELECT p.id as player_id, s.view_w + 1 AS y, '╚' || repeat('═', view_w) || '╝' AS view_col
  FROM players p
  CROSS JOIN settings s
),
-- Frame the minimap with borders
minimap_lines AS (
  SELECT m.player_id,
         m.y,
         '║' || rpad(m.row, md.mm_w, ' ') || '║' AS mm_col
  FROM mm m
  CROSS JOIN mm_dims md
),
minimap_frame AS (
  SELECT p.id as player_id, -1 AS y, '╔' || repeat('═', md.mm_w) || '╗' AS mm_col
  FROM mm_dims md
  CROSS JOIN players p

  UNION ALL

  SELECT *
  FROM minimap_lines

  UNION ALL

  SELECT p.id as player_id, md.y_max + 1 AS y, '╚' || repeat('═', md.mm_w) || '╝' AS mm_col
  FROM mm_dims md
  CROSS JOIN players p
),
-- Build the HUD: name, score, HP bar, bullets
player_lines AS (
  SELECT
    row_number() OVER (ORDER BY p.id) - 1 AS y,
    (
      p.id || ': ' || rpad(m.name, 10, ' ') || ' (' || m.minimap_icon || ') '
      || 'score: ' || p.score || '   '
      || 'HP: [' ||
         repeat('█', GREATEST(0, LEAST(20, ROUND(20 * GREATEST(0, LEAST(p.hp,100))::numeric / 100)::int))) ||
         repeat(' ', GREATEST(0, 20 - ROUND(20 * GREATEST(0, LEAST(p.hp,100))::numeric / 100)::int)) ||
         '] ' || GREATEST(0, p.hp) || '   '
      || 'AMMO: ' ||
        repeat('•', COALESCE(p.ammo,0))
    ) AS player_row
  FROM players p
  JOIN mobs m ON p.id = m.id
),

-- Shift HUD rows to start below the minimap bottom border
player_lines_shifted AS (
  SELECT pl.player_row,
         pl.y + md.y_max + 2 AS y
  FROM player_lines pl
  CROSS JOIN mm_dims md
),

-- Number of HUD rows (max y of HUD, after shift)
hud_dims AS (
  SELECT MAX(y) AS hud_max_y
  FROM player_lines_shifted
),

-- Build a per-viewer row index covering frames and the HUD height
bounds AS (
  SELECT
    p.id as player_id,
    GREATEST(
      s.view_h + 1,  -- account for bottom border in gameview
      md.y_max + 1,  -- account for bottom border in minimap
      hd.hud_max_y   -- HUD rows are 0..hud_max_y
    ) AS y_top
  FROM players p
  CROSS JOIN settings s
  CROSS JOIN mm_dims md
  CROSS JOIN hud_dims hd
),
row_index AS (
  SELECT b.player_id, gs.y
  FROM bounds b
  JOIN LATERAL generate_series(-1, b.y_top) AS gs(y) ON TRUE
),

-- Attach the framed columns to the row index (per viewer)
frames_by_row AS (
  SELECT
    ri.player_id,
    ri.y,
    gv.view_col,
    mf.mm_col
  FROM row_index ri
  LEFT JOIN gameview_frame gv
    ON gv.player_id = ri.player_id AND gv.y = ri.y
  LEFT JOIN minimap_frame mf
    ON mf.player_id = ri.player_id AND mf.y = ri.y
)

-- Final: each viewer sees their own frames + the full global HUD
SELECT
  f.player_id,
  f.y,
  COALESCE(f.view_col, repeat(' ', s.view_w + 2)) || '   ' ||
  COALESCE(f.mm_col, COALESCE(pls.player_row, '')) AS full_row
FROM frames_by_row f
LEFT JOIN player_lines_shifted pls ON pls.y = f.y, mm_dims md, settings s
ORDER BY f.player_id, f.y;
