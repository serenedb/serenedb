-- Native, transactional observability. These rows never drive gameplay; they
-- record authoritative before/after values and roll back with the tick.
CREATE TABLE sql_trace_events (
    id BIGSERIAL PRIMARY KEY,
    tick BIGINT NOT NULL,
    priority BIGINT NOT NULL,
    kind TEXT NOT NULL,
    source_tag TEXT NOT NULL,
    subject TEXT NOT NULL,
    before_value TEXT NOT NULL,
    after_value TEXT NOT NULL,
    detail TEXT NOT NULL
);

CREATE INDEX idx_sql_trace_events_tick
ON sql_trace_events(tick DESC, priority DESC, id DESC);

-- This row is published only after the game connection has successfully
-- executed the real render query for the same transaction.
CREATE TABLE displayed_frame (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    tick BIGINT NOT NULL,
    width BIGINT NOT NULL,
    height BIGINT NOT NULL,
    pixel_count BIGINT NOT NULL,
    scanline_count BIGINT NOT NULL,
    game_state TEXT NOT NULL,
    query_text TEXT NOT NULL
);

INSERT INTO displayed_frame(
    id,tick,width,height,pixel_count,scanline_count,game_state,query_text
)
SELECT
    1,0,s.screen_width,s.screen_height,
    s.screen_width*s.screen_height,s.screen_height/2+3,gs.state,
    'SELECT scanline FROM render_frame ORDER BY y;'
FROM settings AS s
JOIN game_status AS gs ON gs.id=1
WHERE s.id=1;

-- SQL-owned snapshots used only to construct honest tick deltas.
CREATE TABLE tick_player_before (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    x DOUBLE PRECISION NOT NULL,
    y DOUBLE PRECISION NOT NULL,
    angle DOUBLE PRECISION NOT NULL,
    health BIGINT NOT NULL,
    ammo BIGINT NOT NULL,
    has_token BIGINT NOT NULL
);

CREATE TABLE tick_door_before (
    id BIGINT PRIMARY KEY,
    locked BIGINT NOT NULL,
    open_fraction DOUBLE PRECISION NOT NULL
);

CREATE TABLE tick_entity_before (
    entity_id BIGINT PRIMARY KEY,
    health BIGINT NOT NULL,
    active BIGINT NOT NULL
);

CREATE TABLE tick_status_before (
    id BIGINT PRIMARY KEY CHECK (id = 1),
    state TEXT NOT NULL,
    message TEXT NOT NULL,
    score BIGINT NOT NULL
);
