CREATE TABLE schema_meta (
    version BIGINT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE settings (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    screen_width BIGINT NOT NULL CHECK (screen_width BETWEEN 40 AND 160),
    screen_height BIGINT NOT NULL CHECK (screen_height BETWEEN 30 AND 120 AND screen_height % 2 = 0),
    horizontal_fov DOUBLE PRECISION NOT NULL,
    max_depth DOUBLE PRECISION NOT NULL,
    max_ray_steps BIGINT NOT NULL,
    move_speed DOUBLE PRECISION NOT NULL,
    turn_speed DOUBLE PRECISION NOT NULL,
    player_radius DOUBLE PRECISION NOT NULL,
    max_dt_us BIGINT NOT NULL,
    input_lease_us BIGINT NOT NULL,
    input_full_us BIGINT NOT NULL,
    edge_rearm_us BIGINT NOT NULL
);

CREATE TABLE game_clock (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    sample_us BIGINT NOT NULL DEFAULT 0,
    now_us BIGINT NOT NULL DEFAULT 0,
    dt_us BIGINT NOT NULL DEFAULT 0,
    tick BIGINT NOT NULL DEFAULT 0,
    seed BIGINT NOT NULL DEFAULT 1337
);

-- Raw terminal geometry is transport input, just like raw key bytes and the
-- monotonic clock sample. SQL decides how that geometry maps to a framebuffer.
CREATE TABLE terminal_surface (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    columns BIGINT NOT NULL CHECK (columns BETWEEN 1 AND 1000),
    rows BIGINT NOT NULL CHECK (rows BETWEEN 1 AND 1000)
);

CREATE TABLE game_status (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    state TEXT NOT NULL CHECK (state IN ('running', 'paused', 'dead', 'won')),
    message TEXT NOT NULL DEFAULT '',
    score BIGINT NOT NULL DEFAULT 0,
    level_id BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE input_events (
    id BIGSERIAL PRIMARY KEY,
    captured_us BIGINT NOT NULL,
    raw_code BLOB NOT NULL,
    consumed_tick BIGINT
);

CREATE TABLE key_bindings (
    raw_code BLOB PRIMARY KEY,
    action TEXT NOT NULL
);

CREATE TABLE action_latches (
    action TEXT PRIMARY KEY,
    armed BIGINT NOT NULL DEFAULT 1 CHECK (armed IN (0, 1)),
    last_event_us BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE materials (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    base_r BIGINT NOT NULL CHECK (base_r BETWEEN 0 AND 255),
    base_g BIGINT NOT NULL CHECK (base_g BETWEEN 0 AND 255),
    base_b BIGINT NOT NULL CHECK (base_b BETWEEN 0 AND 255),
    accent_r BIGINT NOT NULL CHECK (accent_r BETWEEN 0 AND 255),
    accent_g BIGINT NOT NULL CHECK (accent_g BETWEEN 0 AND 255),
    accent_b BIGINT NOT NULL CHECK (accent_b BETWEEN 0 AND 255),
    emissive DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE map_cells (
    x BIGINT NOT NULL,
    y BIGINT NOT NULL,
    material_id BIGINT NOT NULL REFERENCES materials(id),
    solid BIGINT NOT NULL CHECK (solid IN (0, 1)),
    is_exit BIGINT NOT NULL DEFAULT 0 CHECK (is_exit IN (0, 1)),
    PRIMARY KEY (x, y)
);

CREATE TABLE doors (
    id BIGINT PRIMARY KEY,
    x BIGINT NOT NULL,
    y BIGINT NOT NULL,
    open_fraction DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (open_fraction BETWEEN 0 AND 1),
    locked BIGINT NOT NULL DEFAULT 1 CHECK (locked IN (0, 1)),
    UNIQUE (x, y),
    FOREIGN KEY (x, y) REFERENCES map_cells(x, y)
);

CREATE TABLE player_spawn (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    angle DOUBLE PRECISION NOT NULL,
    health BIGINT NOT NULL,
    ammo BIGINT NOT NULL
);

CREATE TABLE player (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    angle DOUBLE PRECISION NOT NULL,
    health BIGINT NOT NULL,
    max_health BIGINT NOT NULL,
    ammo BIGINT NOT NULL,
    has_token BIGINT NOT NULL DEFAULT 0 CHECK (has_token IN (0, 1)),
    damage_flash DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE weapon_state (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    damage BIGINT NOT NULL,
    range DOUBLE PRECISION NOT NULL,
    cooldown_us BIGINT NOT NULL,
    ready_at_us BIGINT NOT NULL DEFAULT 0,
    muzzle_until_us BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE sprites (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    width BIGINT NOT NULL,
    height BIGINT NOT NULL
);

CREATE TABLE sprite_pixels (
    sprite_id BIGINT NOT NULL REFERENCES sprites(id),
    x BIGINT NOT NULL,
    y BIGINT NOT NULL,
    r BIGINT NOT NULL CHECK (r BETWEEN 0 AND 255),
    g BIGINT NOT NULL CHECK (g BETWEEN 0 AND 255),
    b BIGINT NOT NULL CHECK (b BETWEEN 0 AND 255),
    a BIGINT NOT NULL DEFAULT 255 CHECK (a BETWEEN 0 AND 255),
    PRIMARY KEY (sprite_id, x, y)
);

CREATE TABLE entity_types (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('enemy', 'pickup')),
    sprite_id BIGINT NOT NULL REFERENCES sprites(id),
    sprite_scale DOUBLE PRECISION NOT NULL,
    radius DOUBLE PRECISION NOT NULL,
    max_health BIGINT NOT NULL,
    speed DOUBLE PRECISION NOT NULL DEFAULT 0,
    contact_damage BIGINT NOT NULL DEFAULT 0,
    attack_range DOUBLE PRECISION NOT NULL DEFAULT 0,
    attack_cooldown_us BIGINT NOT NULL DEFAULT 0,
    pickup_kind TEXT,
    pickup_value BIGINT NOT NULL DEFAULT 0,
    score_value BIGINT NOT NULL DEFAULT 0,
    is_boss BIGINT NOT NULL DEFAULT 0 CHECK (is_boss IN (0, 1))
);

CREATE TABLE entity_spawns (
    id BIGINT PRIMARY KEY,
    type_id BIGINT NOT NULL REFERENCES entity_types(id),
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    angle DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE entities (
    id BIGINT PRIMARY KEY,
    type_id BIGINT NOT NULL REFERENCES entity_types(id),
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    angle DOUBLE PRECISION NOT NULL DEFAULT 0,
    health BIGINT NOT NULL,
    state TEXT NOT NULL DEFAULT 'idle',
    next_attack_us BIGINT NOT NULL DEFAULT 0,
    active BIGINT NOT NULL DEFAULT 1 CHECK (active IN (0, 1))
);

CREATE TABLE event_log (
    id BIGSERIAL PRIMARY KEY,
    tick BIGINT NOT NULL,
    kind TEXT NOT NULL,
    source_id BIGINT,
    target_id BIGINT,
    value BIGINT,
    detail TEXT NOT NULL DEFAULT ''
);

-- Persistent SQL-owned scratch relations. They are cleared and rebuilt inside
-- each tick so the host never needs to understand intermediate game state.
CREATE TABLE tick_actions (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    forward DOUBLE PRECISION NOT NULL,
    backward DOUBLE PRECISION NOT NULL,
    strafe_left DOUBLE PRECISION NOT NULL,
    strafe_right DOUBLE PRECISION NOT NULL,
    turn_left DOUBLE PRECISION NOT NULL,
    turn_right DOUBLE PRECISION NOT NULL,
    fire_pressed BIGINT NOT NULL,
    interact_pressed BIGINT NOT NULL,
    restart_pressed BIGINT NOT NULL,
    pause_pressed BIGINT NOT NULL
);

CREATE TABLE tick_motion (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    dx DOUBLE PRECISION NOT NULL,
    dy DOUBLE PRECISION NOT NULL
);

CREATE TABLE tick_shot (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    fired BIGINT NOT NULL,
    target_id BIGINT
);

CREATE TABLE tick_enemy_plan (
    entity_id BIGINT PRIMARY KEY,
    next_x DOUBLE PRECISION NOT NULL,
    next_y DOUBLE PRECISION NOT NULL
);

CREATE TABLE numbers (n BIGINT PRIMARY KEY);

WITH RECURSIVE seq(n) AS (
    VALUES (0)
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 511
)
INSERT INTO numbers(n) SELECT n FROM seq;

CREATE VIEW recent_actions AS
WITH latest AS (
    SELECT
        kb.action,
        max(ie.captured_us) AS latest_us,
        max(CASE WHEN ie.consumed_tick IS NULL THEN 1 ELSE 0 END) AS pressed,
        count(*) AS event_count
    FROM input_events AS ie
    JOIN key_bindings AS kb ON kb.raw_code = ie.raw_code
    JOIN game_clock AS gc ON gc.id = 1
    WHERE ie.captured_us <= gc.sample_us
    GROUP BY kb.action
)
SELECT
    latest.action,
    CASE
        WHEN gc.now_us - latest.latest_us <= s.input_full_us THEN 1.0
        ELSE greatest(0.0, least(1.0,
            (s.input_lease_us - (gc.now_us - latest.latest_us)) * 1.0
            / greatest(1, s.input_lease_us - s.input_full_us)
        ))
    END AS held,
    latest.pressed,
    latest.event_count,
    latest.latest_us
FROM latest
JOIN game_clock AS gc ON gc.id = 1
JOIN settings AS s ON s.id = 1
WHERE latest.latest_us >= gc.now_us - s.input_lease_us;

CREATE INDEX idx_input_events_recent ON input_events(captured_us, consumed_tick);
CREATE INDEX idx_entities_active ON entities(active, type_id);
INSERT INTO settings(
    id, screen_width, screen_height, horizontal_fov, max_depth,
    max_ray_steps, move_speed, turn_speed, player_radius,
    max_dt_us, input_lease_us, input_full_us, edge_rearm_us
) VALUES (
    1, 80, 60, 1.1519173063162575, 18.0,
    48, 2.25, 1.95, 0.22,
    80000, 350000, 100000, 600000
);

INSERT INTO game_clock(id, sample_us, now_us, dt_us, tick, seed)
VALUES (1, 0, 0, 0, 0, 1337);

INSERT INTO terminal_surface(id, columns, rows)
VALUES (1, 80, 33);

INSERT INTO game_status(id, state, message, score, level_id)
VALUES (1, 'running', 'FIND THE INDEX TOKEN', 0, 1);

INSERT INTO materials(id, name, base_r, base_g, base_b, accent_r, accent_g, accent_b, emissive) VALUES
    (1, 'catalog stone', 72, 78, 92, 29, 34, 45, 0.00),
    (2, 'warning bulkhead', 142, 35, 32, 62, 9, 13, 0.08),
    (3, 'index gate', 184, 111, 24, 55, 24, 8, 0.12),
    (4, 'commit portal', 24, 162, 174, 6, 52, 61, 0.35);

CREATE TEMP TABLE _layout_rows(y BIGINT PRIMARY KEY, row_text TEXT NOT NULL);
INSERT INTO _layout_rows(y, row_text) VALUES
    (0,  '########################'),
    (1,  '#..........#...........#'),
    (2,  '#..##......#......##...#'),
    (3,  '#..........#...........#'),
    (4,  '#.....##...#..##.......#'),
    (5,  '#..........#...........#'),
    (6,  '#..###.....#.....###...#'),
    (7,  '#..........#...........#'),
    (8,  '#..........D...........#'),
    (9,  '#..........#...........#'),
    (10, '#...##.....#..##.......#'),
    (11, '#..........#...........#'),
    (12, '#..###.....#.....###...#'),
    (13, '#..........#...........#'),
    (14, '#......##..#...##......#'),
    (15, '#..........#...........#'),
    (16, '#..........#.........X.#'),
    (17, '########################');

INSERT INTO map_cells(x, y, material_id, solid, is_exit)
SELECT
    n.n,
    r.y,
    CASE substr(r.row_text, n.n + 1, 1)
        WHEN 'D' THEN 3
        WHEN 'X' THEN 4
        WHEN '#' THEN CASE WHEN (n.n + r.y) % 7 = 0 THEN 2 ELSE 1 END
        ELSE 1
    END,
    CASE WHEN substr(r.row_text, n.n + 1, 1) = '#' THEN 1 ELSE 0 END,
    CASE WHEN substr(r.row_text, n.n + 1, 1) = 'X' THEN 1 ELSE 0 END
FROM _layout_rows AS r
JOIN numbers AS n ON n.n < length(r.row_text);

DROP TABLE _layout_rows;

INSERT INTO doors(id, x, y, open_fraction, locked)
VALUES (1, 11, 8, 0.0, 1);

INSERT INTO player_spawn(id, x, y, angle, health, ammo)
VALUES (1, 2.5, 3.5, 0.0, 100, 32);

INSERT INTO player(id, x, y, angle, health, max_health, ammo, has_token, damage_flash)
VALUES (1, 2.5, 3.5, 0.0, 100, 100, 32, 0, 0.0);

INSERT INTO weapon_state(id, damage, range, cooldown_us, ready_at_us, muzzle_until_us)
VALUES (1, 24, 14.0, 260000, 0, 0);

-- The host records raw byte sequences. Meaning lives only in this table.
INSERT INTO key_bindings(raw_code, action) VALUES
    (X'77', 'forward'),       (X'57', 'forward'),
    (X'73', 'backward'),      (X'53', 'backward'),
    (X'61', 'strafe_left'),   (X'41', 'strafe_left'),
    (X'64', 'strafe_right'),  (X'44', 'strafe_right'),
    (X'6A', 'turn_left'),     (X'4A', 'turn_left'),
    (X'6C', 'turn_right'),    (X'4C', 'turn_right'),
    (X'1B5B44', 'turn_left'),
    (X'1B5B43', 'turn_right'),
    (X'20', 'fire'),
    (X'65', 'interact'),      (X'45', 'interact'),
    (X'72', 'restart'),       (X'52', 'restart'),
    (X'70', 'pause'),         (X'50', 'pause');

INSERT INTO action_latches(action, armed, last_event_us) VALUES
    ('interact', 1, 0),
    ('restart', 1, 0),
    ('pause', 1, 0);

INSERT INTO sprites(id, name, width, height) VALUES
    (1, 'orphan', 9, 12),
    (2, 'lock waiter', 9, 12),
    (3, 'deadlock', 13, 14),
    (4, 'index token', 7, 7),
    (5, 'health tuple', 7, 7),
    (6, 'ammo page', 7, 7);

CREATE TEMP TABLE _sprite_rows(
    sprite_id BIGINT NOT NULL,
    y BIGINT NOT NULL,
    row_text TEXT NOT NULL,
    PRIMARY KEY (sprite_id, y)
);

INSERT INTO _sprite_rows(sprite_id, y, row_text) VALUES
    (1,0,'...RRR...'), (1,1,'..RRRRR..'), (1,2,'.RWWRWWR.'),
    (1,3,'.RRRRRRR.'), (1,4,'..RKKKR..'), (1,5,'.RRRRRRR.'),
    (1,6,'RR.RRR.RR'), (1,7,'R..RRR..R'), (1,8,'...RRR...'),
    (1,9,'..RR.RR..'), (1,10,'.RR...RR.'), (1,11,'RR.....RR'),

    (2,0,'...CCC...'), (2,1,'..CCCCC..'), (2,2,'.CYYCYYC.'),
    (2,3,'.CCCCCCC.'), (2,4,'..CKKKC..'), (2,5,'.CCBBBCC.'),
    (2,6,'CC.CCC.CC'), (2,7,'C..CCC..C'), (2,8,'...CCC...'),
    (2,9,'..CC.CC..'), (2,10,'.CC...CC.'), (2,11,'CC.....CC'),

    (3,0,'....RRRRR....'), (3,1,'..RRRRRRRRR..'), (3,2,'.RRRYYYYYRRR.'),
    (3,3,'RRYYRRRRRYYRR'), (3,4,'RRRRRKKKRRRRR'), (3,5,'.RRRRRRRRRRR.'),
    (3,6,'RRR.RRRRR.RRR'), (3,7,'RR..RRRRR..RR'), (3,8,'R...RRRRR...R'),
    (3,9,'....RRRRR....'), (3,10,'...RRR.RRR...'), (3,11,'..RRR...RRR..'),
    (3,12,'.RRR.....RRR.'), (3,13,'RRR.......RRR'),

    (4,0,'..YYY..'), (4,1,'.YCCCY.'), (4,2,'YCCYCCY'),
    (4,3,'YCCYCCY'), (4,4,'YCCYCCY'), (4,5,'.YCCCY.'), (4,6,'..YYY..'),

    (5,0,'..GGG..'), (5,1,'..GGG..'), (5,2,'GGGGGGG'),
    (5,3,'GGGGGGG'), (5,4,'..GGG..'), (5,5,'..GGG..'), (5,6,'..GGG..'),

    (6,0,'.BBBBB.'), (6,1,'BWWWWWB'), (6,2,'BWBWBWB'),
    (6,3,'BWWWWWB'), (6,4,'BWBWBWB'), (6,5,'BWWWWWB'), (6,6,'.BBBBB.');

INSERT INTO sprite_pixels(sprite_id, x, y, r, g, b, a)
SELECT
    sr.sprite_id,
    n.n,
    sr.y,
    CASE substr(sr.row_text, n.n + 1, 1)
        WHEN 'R' THEN 224 WHEN 'C' THEN 36 WHEN 'Y' THEN 255
        WHEN 'G' THEN 48 WHEN 'B' THEN 55 WHEN 'W' THEN 236
        WHEN 'K' THEN 18 ELSE 200 END,
    CASE substr(sr.row_text, n.n + 1, 1)
        WHEN 'R' THEN 45 WHEN 'C' THEN 205 WHEN 'Y' THEN 190
        WHEN 'G' THEN 226 WHEN 'B' THEN 105 WHEN 'W' THEN 241
        WHEN 'K' THEN 12 ELSE 200 END,
    CASE substr(sr.row_text, n.n + 1, 1)
        WHEN 'R' THEN 42 WHEN 'C' THEN 225 WHEN 'Y' THEN 45
        WHEN 'G' THEN 91 WHEN 'B' THEN 235 WHEN 'W' THEN 247
        WHEN 'K' THEN 22 ELSE 200 END,
    255
FROM _sprite_rows AS sr
JOIN numbers AS n ON n.n < length(sr.row_text)
WHERE substr(sr.row_text, n.n + 1, 1) <> '.';

DROP TABLE _sprite_rows;

INSERT INTO entity_types(
    id, name, kind, sprite_id, sprite_scale, radius, max_health, speed,
    contact_damage, attack_range, attack_cooldown_us,
    pickup_kind, pickup_value, score_value, is_boss
) VALUES
    (1, 'ORPHAN', 'enemy', 1, 0.82, 0.24, 48, 0.72, 8, 0.72, 700000, NULL, 0, 100, 0),
    (2, 'LOCK WAITER', 'enemy', 2, 0.86, 0.24, 58, 0.58, 11, 3.80, 1100000, NULL, 0, 150, 0),
    (3, 'DEADLOCK', 'enemy', 3, 1.15, 0.34, 120, 0.66, 16, 0.86, 650000, NULL, 0, 500, 1),
    (4, 'INDEX TOKEN', 'pickup', 4, 0.42, 0.18, 1, 0, 0, 0, 0, 'token', 1, 50, 0),
    (5, 'HEALTH TUPLE', 'pickup', 5, 0.40, 0.18, 1, 0, 0, 0, 0, 'health', 30, 25, 0),
    (6, 'AMMO PAGE', 'pickup', 6, 0.40, 0.18, 1, 0, 0, 0, 0, 'ammo', 12, 25, 0);

INSERT INTO entity_spawns(id, type_id, x, y, angle) VALUES
    (1, 1, 8.3, 3.5, 0),
    (2, 1, 5.5, 11.5, 0),
    (3, 2, 16.5, 7.3, 0),
    (4, 1, 19.3, 13.5, 0),
    (5, 3, 20.2, 15.2, 0),
    (6, 4, 8.5, 13.5, 0),
    (7, 5, 3.2, 10.5, 0),
    (8, 6, 6.8, 5.2, 0);
DROP VIEW IF EXISTS render_frame;
DROP VIEW IF EXISTS frame_pixels;

-- One invocation of this view calculates the full raster. DDA traversal,
-- projection, texturing, sprites, weapon, crosshair and colour effects all
-- remain inside SQL.
CREATE VIEW frame_pixels AS
WITH RECURSIVE
cfg AS MATERIALIZED (
    SELECT
        s.screen_width AS width,
        s.screen_height AS height,
        s.horizontal_fov AS fov,
        s.max_depth,
        s.max_ray_steps,
        p.x AS player_x,
        p.y AS player_y,
        p.angle,
        cos(p.angle) AS dir_x,
        sin(p.angle) AS dir_y,
        -sin(p.angle) * tan(s.horizontal_fov / 2.0) AS plane_x,
        cos(p.angle) * tan(s.horizontal_fov / 2.0) AS plane_y,
        s.screen_width / (2.0 * tan(s.horizontal_fov / 2.0)) AS projection_plane,
        p.damage_flash,
        gs.state AS game_state,
        gc.now_us,
        w.muzzle_until_us
    FROM settings AS s
    JOIN player AS p ON p.id = 1
    JOIN game_status AS gs ON gs.id = 1
    JOIN game_clock AS gc ON gc.id = 1
    JOIN weapon_state AS w ON w.id = 1
    WHERE s.id = 1
),
rays AS MATERIALIZED (
    SELECT
        n.n AS x,
        2.0 * (n.n + 0.5) / c.width - 1.0 AS camera_x,
        c.dir_x + c.plane_x * (2.0 * (n.n + 0.5) / c.width - 1.0) AS ray_x,
        c.dir_y + c.plane_y * (2.0 * (n.n + 0.5) / c.width - 1.0) AS ray_y
    FROM numbers AS n
    CROSS JOIN cfg AS c
    WHERE n.n < c.width
),
ray_start AS (
    SELECT
        r.x,
        r.camera_x,
        r.ray_x,
        r.ray_y,
        cast(trunc(floor(c.player_x)) AS BIGINT) AS map_x,
        cast(trunc(floor(c.player_y)) AS BIGINT) AS map_y,
        CASE WHEN abs(r.ray_x) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_x) END AS delta_x,
        CASE WHEN abs(r.ray_y) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_y) END AS delta_y,
        CASE WHEN r.ray_x < 0 THEN -1 ELSE 1 END AS step_x,
        CASE WHEN r.ray_y < 0 THEN -1 ELSE 1 END AS step_y,
        CASE WHEN r.ray_x < 0
             THEN (c.player_x - floor(c.player_x)) *
                  CASE WHEN abs(r.ray_x) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_x) END
             ELSE (floor(c.player_x) + 1.0 - c.player_x) *
                  CASE WHEN abs(r.ray_x) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_x) END
        END AS side_x,
        CASE WHEN r.ray_y < 0
             THEN (c.player_y - floor(c.player_y)) *
                  CASE WHEN abs(r.ray_y) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_y) END
             ELSE (floor(c.player_y) + 1.0 - c.player_y) *
                  CASE WHEN abs(r.ray_y) < 0.000000000001 THEN 1.0e30 ELSE abs(1.0 / r.ray_y) END
        END AS side_y
    FROM rays AS r
    CROSS JOIN cfg AS c
),
walk(
    x, camera_x, ray_x, ray_y, map_x, map_y,
    delta_x, delta_y, step_x, step_y, side_x, side_y,
    hit_side, depth, distance
) AS (
    SELECT
        x, camera_x, ray_x, ray_y, map_x, map_y,
        delta_x, delta_y, step_x, step_y, side_x, side_y,
        -1, 0, CAST(0.0 AS DOUBLE PRECISION)
    FROM ray_start

    UNION ALL

    SELECT
        w.x,
        w.camera_x,
        w.ray_x,
        w.ray_y,
        w.map_x + CASE WHEN w.side_x < w.side_y THEN w.step_x ELSE 0 END,
        w.map_y + CASE WHEN w.side_x < w.side_y THEN 0 ELSE w.step_y END,
        w.delta_x,
        w.delta_y,
        w.step_x,
        w.step_y,
        w.side_x + CASE WHEN w.side_x < w.side_y THEN w.delta_x ELSE 0 END,
        w.side_y + CASE WHEN w.side_x < w.side_y THEN 0 ELSE w.delta_y END,
        CASE WHEN w.side_x < w.side_y THEN 0 ELSE 1 END,
        w.depth + 1,
        least(w.side_x, w.side_y)
    FROM walk AS w
    CROSS JOIN cfg AS c
    WHERE w.depth < c.max_ray_steps
      AND w.distance < c.max_depth
      AND least(w.side_x, w.side_y) <= c.max_depth
      AND NOT EXISTS (
          SELECT 1
          FROM map_cells AS m
          WHERE m.x = w.map_x AND m.y = w.map_y
            AND (
                m.solid = 1
                OR EXISTS (
                    SELECT 1 FROM doors AS d
                    WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
                )
            )
      )
),
hit_candidates AS (
    SELECT
        w.*,
        m.material_id
    FROM walk AS w
    JOIN map_cells AS m ON m.x = w.map_x AND m.y = w.map_y
    CROSS JOIN cfg AS c
    WHERE w.depth > 0
      AND w.distance <= c.max_depth
      AND (
          m.solid = 1
          OR EXISTS (
              SELECT 1 FROM doors AS d
              WHERE d.x = m.x AND d.y = m.y AND d.open_fraction < 0.82
          )
      )
),
ray_hits AS MATERIALIZED (
    SELECT
        r.x,
        r.camera_x,
        r.ray_x,
        r.ray_y,
        CASE WHEN hc.x IS NULL THEN c.max_depth ELSE greatest(0.001, hc.distance) END AS distance,
        CASE WHEN hc.x IS NULL THEN 0 ELSE 1 END AS did_hit,
        hc.hit_side,
        hc.material_id,
        c.player_x + r.ray_x * CASE WHEN hc.x IS NULL THEN c.max_depth ELSE hc.distance END AS hit_x,
        c.player_y + r.ray_y * CASE WHEN hc.x IS NULL THEN c.max_depth ELSE hc.distance END AS hit_y
    FROM rays AS r
    CROSS JOIN cfg AS c
    LEFT JOIN hit_candidates AS hc ON hc.x = r.x
),
wall_columns AS MATERIALIZED (
    SELECT
        rh.*,
        c.projection_plane / greatest(rh.distance, 0.05) AS wall_height,
        (c.height - c.projection_plane / greatest(rh.distance, 0.05)) / 2.0 AS wall_top,
        (c.height + c.projection_plane / greatest(rh.distance, 0.05)) / 2.0 AS wall_bottom,
        CASE WHEN rh.hit_side = 0
             THEN rh.hit_y - floor(rh.hit_y)
             ELSE rh.hit_x - floor(rh.hit_x)
        END AS texture_u,
        m.base_r, m.base_g, m.base_b,
        m.accent_r, m.accent_g, m.accent_b,
        m.emissive
    FROM ray_hits AS rh
    CROSS JOIN cfg AS c
    LEFT JOIN materials AS m ON m.id = rh.material_id
),
pixel_grid AS (
    SELECT nx.n AS x, ny.n AS y
    FROM numbers AS nx
    CROSS JOIN numbers AS ny
    CROSS JOIN cfg AS c
    WHERE nx.n < c.width AND ny.n < c.height
),
background AS MATERIALIZED (
    SELECT
        pg.x,
        pg.y,
        wc.distance AS wall_distance,
        wc.ray_x,
        wc.ray_y,
        CASE WHEN pg.y < c.height / 2 THEN
            cast(trunc(5 + 10.0 * pg.y / c.height) AS BIGINT)
        ELSE
            cast(trunc(12 + 42 * greatest(0.0, 1.0 - least(1.0,
                (c.height * 0.50 / greatest(0.5, pg.y - c.height / 2.0)) / c.max_depth
            ))) AS BIGINT)
        END AS r,
        CASE WHEN pg.y < c.height / 2 THEN
            cast(trunc(7 + 12.0 * pg.y / c.height) AS BIGINT)
        ELSE
            cast(trunc(15 + 46 * greatest(0.0, 1.0 - least(1.0,
                (c.height * 0.50 / greatest(0.5, pg.y - c.height / 2.0)) / c.max_depth
            ))) AS BIGINT)
        END AS g,
        CASE WHEN pg.y < c.height / 2 THEN
            cast(trunc(15 + 20.0 * pg.y / c.height) AS BIGINT)
        ELSE
            cast(trunc(20 + 53 * greatest(0.0, 1.0 - least(1.0,
                (c.height * 0.50 / greatest(0.5, pg.y - c.height / 2.0)) / c.max_depth
            ))) AS BIGINT)
        END AS b
    FROM pixel_grid AS pg
    JOIN wall_columns AS wc ON wc.x = pg.x
    CROSS JOIN cfg AS c
),
wall_pixels AS (
    SELECT
        bg.x,
        bg.y,
        CASE
            WHEN wc.did_hit = 1 AND bg.y >= wc.wall_top AND bg.y <= wc.wall_bottom
            THEN 1 ELSE 0
        END AS is_wall,
        cast(trunc(least(255, greatest(0,
            (CASE WHEN
                cast(trunc(wc.texture_u * 16) AS BIGINT) % 8 = 0
                OR cast(trunc(((bg.y - wc.wall_top) / greatest(wc.wall_height, 0.01)) * 18) AS BIGINT) % 9 = 0
             THEN wc.accent_r ELSE wc.base_r END)
            * (round((greatest(0.14, 1.0 - wc.distance / c.max_depth)
                * CASE WHEN wc.hit_side = 1 THEN 0.72 ELSE 1.0 END
                + wc.emissive) * 8.0) / 8.0)
        ))) AS BIGINT) AS wall_r,
        cast(trunc(least(255, greatest(0,
            (CASE WHEN
                cast(trunc(wc.texture_u * 16) AS BIGINT) % 8 = 0
                OR cast(trunc(((bg.y - wc.wall_top) / greatest(wc.wall_height, 0.01)) * 18) AS BIGINT) % 9 = 0
             THEN wc.accent_g ELSE wc.base_g END)
            * (round((greatest(0.14, 1.0 - wc.distance / c.max_depth)
                * CASE WHEN wc.hit_side = 1 THEN 0.72 ELSE 1.0 END
                + wc.emissive) * 8.0) / 8.0)
        ))) AS BIGINT) AS wall_g,
        cast(trunc(least(255, greatest(0,
            (CASE WHEN
                cast(trunc(wc.texture_u * 16) AS BIGINT) % 8 = 0
                OR cast(trunc(((bg.y - wc.wall_top) / greatest(wc.wall_height, 0.01)) * 18) AS BIGINT) % 9 = 0
             THEN wc.accent_b ELSE wc.base_b END)
            * (round((greatest(0.14, 1.0 - wc.distance / c.max_depth)
                * CASE WHEN wc.hit_side = 1 THEN 0.72 ELSE 1.0 END
                + wc.emissive) * 8.0) / 8.0)
        ))) AS BIGINT) AS wall_b
    FROM background AS bg
    JOIN wall_columns AS wc ON wc.x = bg.x
    CROSS JOIN cfg AS c
),
base_pixels AS (
    SELECT
        bg.x,
        bg.y,
        bg.wall_distance,
        CASE WHEN wp.is_wall = 1 THEN wp.wall_r ELSE
            CASE WHEN bg.y >= c.height / 2 AND (
                (cast(trunc(floor(c.player_x + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_x)) AS BIGINT)
                + cast(trunc(floor(c.player_y + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_y)) AS BIGINT)) % 2 = 0
            ) THEN cast(trunc(bg.r * 0.72) AS BIGINT) ELSE bg.r END
        END AS r,
        CASE WHEN wp.is_wall = 1 THEN wp.wall_g ELSE
            CASE WHEN bg.y >= c.height / 2 AND (
                (cast(trunc(floor(c.player_x + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_x)) AS BIGINT)
                + cast(trunc(floor(c.player_y + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_y)) AS BIGINT)) % 2 = 0
            ) THEN cast(trunc(bg.g * 0.72) AS BIGINT) ELSE bg.g END
        END AS g,
        CASE WHEN wp.is_wall = 1 THEN wp.wall_b ELSE
            CASE WHEN bg.y >= c.height / 2 AND (
                (cast(trunc(floor(c.player_x + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_x)) AS BIGINT)
                + cast(trunc(floor(c.player_y + (c.height * 0.50 / greatest(0.5, bg.y - c.height / 2.0)) * bg.ray_y)) AS BIGINT)) % 2 = 0
            ) THEN cast(trunc(bg.b * 0.72) AS BIGINT) ELSE bg.b END
        END AS b
    FROM background AS bg
    JOIN wall_pixels AS wp ON wp.x = bg.x AND wp.y = bg.y
    CROSS JOIN cfg AS c
),
projected_entities AS (
    SELECT
        e.id,
        e.type_id,
        et.sprite_id,
        sp.width AS sprite_width,
        sp.height AS sprite_height,
        ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y) AS depth,
        (-(e.x - c.player_x) * c.dir_y + (e.y - c.player_y) * c.dir_x) AS lateral,
        c.projection_plane * et.sprite_scale /
            greatest(0.05, ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y)) AS size,
        c.width / 2.0 * (1.0 +
            (-(e.x - c.player_x) * c.dir_y + (e.y - c.player_y) * c.dir_x)
            / greatest(0.05, ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y))
            / tan(c.fov / 2.0)
        ) AS center_x,
        c.height / 2.0 AS center_y
    FROM entities AS e
    JOIN entity_types AS et ON et.id = e.type_id
    JOIN sprites AS sp ON sp.id = et.sprite_id
    CROSS JOIN cfg AS c
    WHERE e.active = 1
      AND ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y) > 0.08
      AND ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y) < c.max_depth
      AND abs(
          (-(e.x - c.player_x) * c.dir_y + (e.y - c.player_y) * c.dir_x)
          / greatest(0.05, ((e.x - c.player_x) * c.dir_x + (e.y - c.player_y) * c.dir_y))
          / tan(c.fov / 2.0)
      ) < 1.15
),
sprite_samples AS (
    SELECT
        nx.n AS x,
        ny.n AS y,
        pe.id AS entity_id,
        pe.depth,
        spx.r,
        spx.g,
        spx.b,
        row_number() OVER (
            PARTITION BY nx.n, ny.n
            ORDER BY pe.depth, pe.id
        ) AS sprite_rank
    FROM projected_entities AS pe
    CROSS JOIN cfg AS c
    JOIN numbers AS nx
      ON nx.n >= greatest(0, cast(trunc(floor(pe.center_x - pe.size / 2.0)) AS BIGINT))
     AND nx.n < least(c.width, cast(trunc(ceil(pe.center_x + pe.size / 2.0)) AS BIGINT))
    JOIN numbers AS ny
      ON ny.n >= greatest(0, cast(trunc(floor(pe.center_y - pe.size / 2.0)) AS BIGINT))
     AND ny.n < least(c.height, cast(trunc(ceil(pe.center_y + pe.size / 2.0)) AS BIGINT))
    JOIN ray_hits AS rh ON rh.x = nx.n AND pe.depth < rh.distance
    JOIN sprite_pixels AS spx
      ON spx.sprite_id = pe.sprite_id
     AND spx.x = cast(trunc(floor((nx.n - (pe.center_x - pe.size / 2.0)) * pe.sprite_width / pe.size)) AS BIGINT)
     AND spx.y = cast(trunc(floor((ny.n - (pe.center_y - pe.size / 2.0)) * pe.sprite_height / pe.size)) AS BIGINT)
    WHERE spx.a > 0
),
world_pixels AS (
    SELECT
        bp.x,
        bp.y,
        coalesce(ss.r, bp.r) AS r,
        coalesce(ss.g, bp.g) AS g,
        coalesce(ss.b, bp.b) AS b
    FROM base_pixels AS bp
    LEFT JOIN sprite_samples AS ss
      ON ss.x = bp.x AND ss.y = bp.y AND ss.sprite_rank = 1
),
overlay_pixels AS (
    SELECT
        wp.x,
        wp.y,
        CASE
            WHEN (wp.x = cast(trunc(c.width / 2) AS BIGINT) AND abs(wp.y - c.height / 2) <= 2)
              OR (wp.y = cast(trunc(c.height / 2) AS BIGINT) AND abs(wp.x - c.width / 2) <= 2)
            THEN 232
            WHEN c.now_us < c.muzzle_until_us
              AND ((wp.x - c.width / 2.0) * (wp.x - c.width / 2.0) / 48.0
                 + (wp.y - c.height * 0.67) * (wp.y - c.height * 0.67) / 18.0) < 1.0
            THEN 255
            WHEN wp.y >= c.height * 0.76
              AND abs(wp.x - c.width / 2.0) <= 2.3 + (wp.y - c.height * 0.76) * 0.10
            THEN 16
            WHEN wp.y >= c.height * 0.86
              AND abs(wp.x - c.width / 2.0) BETWEEN 10 AND 18
            THEN 126
            WHEN wp.y >= c.height * 0.72
              AND abs(wp.x - c.width / 2.0) <= 3.5 + (wp.y - c.height * 0.72) * 0.47
            THEN cast(trunc(38 + 36 * (wp.y / c.height)) AS BIGINT)
            ELSE wp.r
        END AS r,
        CASE
            WHEN (wp.x = cast(trunc(c.width / 2) AS BIGINT) AND abs(wp.y - c.height / 2) <= 2)
              OR (wp.y = cast(trunc(c.height / 2) AS BIGINT) AND abs(wp.x - c.width / 2) <= 2)
            THEN 240
            WHEN c.now_us < c.muzzle_until_us
              AND ((wp.x - c.width / 2.0) * (wp.x - c.width / 2.0) / 48.0
                 + (wp.y - c.height * 0.67) * (wp.y - c.height * 0.67) / 18.0) < 1.0
            THEN 190
            WHEN wp.y >= c.height * 0.76
              AND abs(wp.x - c.width / 2.0) <= 2.3 + (wp.y - c.height * 0.76) * 0.10
            THEN 20
            WHEN wp.y >= c.height * 0.86
              AND abs(wp.x - c.width / 2.0) BETWEEN 10 AND 18
            THEN 43
            WHEN wp.y >= c.height * 0.72
              AND abs(wp.x - c.width / 2.0) <= 3.5 + (wp.y - c.height * 0.72) * 0.47
            THEN cast(trunc(53 + 42 * (wp.y / c.height)) AS BIGINT)
            ELSE wp.g
        END AS g,
        CASE
            WHEN (wp.x = cast(trunc(c.width / 2) AS BIGINT) AND abs(wp.y - c.height / 2) <= 2)
              OR (wp.y = cast(trunc(c.height / 2) AS BIGINT) AND abs(wp.x - c.width / 2) <= 2)
            THEN 248
            WHEN c.now_us < c.muzzle_until_us
              AND ((wp.x - c.width / 2.0) * (wp.x - c.width / 2.0) / 48.0
                 + (wp.y - c.height * 0.67) * (wp.y - c.height * 0.67) / 18.0) < 1.0
            THEN 42
            WHEN wp.y >= c.height * 0.76
              AND abs(wp.x - c.width / 2.0) <= 2.3 + (wp.y - c.height * 0.76) * 0.10
            THEN 28
            WHEN wp.y >= c.height * 0.86
              AND abs(wp.x - c.width / 2.0) BETWEEN 10 AND 18
            THEN 38
            WHEN wp.y >= c.height * 0.72
              AND abs(wp.x - c.width / 2.0) <= 3.5 + (wp.y - c.height * 0.72) * 0.47
            THEN cast(trunc(72 + 44 * (wp.y / c.height)) AS BIGINT)
            ELSE wp.b
        END AS b
    FROM world_pixels AS wp
    CROSS JOIN cfg AS c
),
final_pixels AS (
    SELECT
        op.x,
        op.y,
        cast(trunc(least(255, greatest(0,
            CASE c.game_state
                WHEN 'dead' THEN op.r * 0.45 + 105
                WHEN 'won' THEN op.r * 0.55
                WHEN 'paused' THEN op.r * 0.42
                ELSE op.r * (1.0 - c.damage_flash * 0.35) + 105 * c.damage_flash
            END
        ))) AS BIGINT) AS r,
        cast(trunc(least(255, greatest(0,
            CASE c.game_state
                WHEN 'dead' THEN op.g * 0.25
                WHEN 'won' THEN op.g * 0.72 + 48
                WHEN 'paused' THEN op.g * 0.42
                ELSE op.g * (1.0 - c.damage_flash * 0.65)
            END
        ))) AS BIGINT) AS g,
        cast(trunc(least(255, greatest(0,
            CASE c.game_state
                WHEN 'dead' THEN op.b * 0.25
                WHEN 'won' THEN op.b * 0.72 + 58
                WHEN 'paused' THEN op.b * 0.50
                ELSE op.b * (1.0 - c.damage_flash * 0.65)
            END
        ))) AS BIGINT) AS b
    FROM overlay_pixels AS op
    CROSS JOIN cfg AS c
)
SELECT x, y, r, g, b
FROM final_pixels;

-- Pair two vertical pixels into one U+2580 terminal cell. SQL emits every SGR
-- colour instruction and the final reset; the host prints rows unchanged.
CREATE VIEW render_frame AS
WITH
pixel_pairs AS (
    SELECT
        x,
        cast(trunc(y / 2) AS BIGINT) AS terminal_y,
        max(CASE WHEN y % 2 = 0 THEN (r << 16) | (g << 8) | b END) AS fg,
        max(CASE WHEN y % 2 = 1 THEN (r << 16) | (g << 8) | b END) AS bg
    FROM frame_pixels
    GROUP BY x, cast(trunc(y / 2) AS BIGINT)
),
with_previous AS (
    SELECT
        *,
        lag(fg) OVER (PARTITION BY terminal_y ORDER BY x) AS previous_fg,
        lag(bg) OVER (PARTITION BY terminal_y ORDER BY x) AS previous_bg
    FROM pixel_pairs
),
tokens AS (
    SELECT
        x,
        terminal_y,
        CASE WHEN previous_fg = fg THEN '' ELSE
            chr(27) || printf('[38;2;%d;%d;%dm',
                (fg >> 16) & 255, (fg >> 8) & 255, fg & 255)
        END ||
        CASE WHEN previous_bg = bg THEN '' ELSE
            chr(27) || printf('[48;2;%d;%d;%dm',
                (bg >> 16) & 255, (bg >> 8) & 255, bg & 255)
        END || '▀' AS token
    FROM with_previous
),
pixel_lines AS (
    SELECT
        terminal_y AS y,
        group_concat(token, '' ORDER BY x) || chr(27) || '[0m' AS scanline
    FROM tokens
    GROUP BY terminal_y
),
hud_source AS MATERIALIZED (
    SELECT
        s.screen_width AS width,
        s.screen_height / 2 AS first_y,
        p.health,
        p.max_health,
        p.ammo,
        p.has_token,
        gs.score,
        gs.state,
        gs.message,
        gc.tick
    FROM settings AS s
    JOIN player AS p ON p.id = 1
    JOIN game_status AS gs ON gs.id = 1
    JOIN game_clock AS gc ON gc.id = 1
    WHERE s.id = 1
),
hud_lines(y, scanline) AS (
    SELECT
        first_y,
        chr(27) || '[48;2;10;14;22m' || chr(27) || '[38;2;232;238;245m' ||
        substr(printf(' HP %03d/%03d   AMMO %03d   SCORE %05d   INDEX %s   TICK %07d',
            health, max_health, ammo, score,
            CASE WHEN has_token = 1 THEN 'ONLINE' ELSE 'MISSING' END,
            tick) || printf('%*s', width, ''), 1, width) || chr(27) || '[0m'
    FROM hud_source
    UNION ALL
    SELECT
        first_y + 1,
        chr(27) || '[48;2;18;9;13m' || chr(27) || '[38;2;255;190;74m' ||
        substr((' ' || message) || printf('%*s', width, ''), 1, width) || chr(27) || '[0m'
    FROM hud_source
    UNION ALL
    SELECT
        first_y + 2,
        chr(27) || '[48;2;6;8;13m' || chr(27) || '[38;2;116;199;214m' ||
        substr(' WASD MOVE  J/L OR ARROWS TURN  SPACE FIRE  E USE  P PAUSE  CTRL-C EXIT' ||
            printf('%*s', width, ''), 1, width) || chr(27) || '[0m'
    FROM hud_source
)
SELECT y, scanline FROM pixel_lines
UNION ALL
SELECT y, scanline FROM hud_lines;
