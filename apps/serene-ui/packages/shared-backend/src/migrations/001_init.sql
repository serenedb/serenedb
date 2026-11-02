CREATE TABLE connections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(255) NOT NULL,
    ssl BOOLEAN NOT NULL,
    authMethod VARCHAR(255) NOT NULL,
    user VARCHAR(255),
    password VARCHAR(255),
    mode VARCHAR(255) NOT NULL,
    host VARCHAR(255),
    socket VARCHAR(255),
    port INT,
    database VARCHAR(255)
);

CREATE TABLE jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    result JSONB,
    error TEXT,
    created_at REAL DEFAULT (unixepoch('now', 'subsec')),
    action_type VARCHAR(255),
    execution_started_at REAL,
    execution_finished_at REAL,
    bind_vars JSONB
);

CREATE TABLE saved_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255) NOT NULL,
    query TEXT NOT NULL,
    bind_vars JSONB,
    usage_count INT DEFAULT 0
);

CREATE TABLE github (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    access_token VARCHAR(255) NOT NULL
);
