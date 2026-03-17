CREATE TABLE dashboards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) NOT NULL,
    auto_refresh BOOLEAN NOT NULL DEFAULT 0,
    refresh_interval INTEGER NOT NULL DEFAULT 60,
    row_limit INTEGER NOT NULL DEFAULT 1000,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE dashboard_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dashboard_id INTEGER NOT NULL,
    type VARCHAR(255) NOT NULL,
    position INTEGER NOT NULL DEFAULT 0,
    bounds JSONB NOT NULL,
    text TEXT,
    query TEXT,
    name VARCHAR(255),
    description VARCHAR(1024),
    custom_refresh_interval_enabled BOOLEAN NOT NULL DEFAULT 0,
    custom_refresh_interval INTEGER NOT NULL DEFAULT 60,
    custom_row_limit_enabled BOOLEAN NOT NULL DEFAULT 0,
    custom_row_limit INTEGER NOT NULL DEFAULT 1000,
    config JSONB,
    FOREIGN KEY (dashboard_id) REFERENCES dashboards(id) ON DELETE CASCADE
);

CREATE INDEX idx_dashboard_blocks_dashboard_id
    ON dashboard_blocks(dashboard_id);

CREATE INDEX idx_dashboard_blocks_dashboard_position
    ON dashboard_blocks(dashboard_id, position);
