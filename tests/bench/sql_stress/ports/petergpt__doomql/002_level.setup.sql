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
