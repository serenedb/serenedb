-- The game executes authoritative runtime SQL from this relation. Inspectors
-- read the same row, eliminating drift between displayed and executed source.
CREATE TABLE runtime_sources (
    name TEXT PRIMARY KEY,
    source_text TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    loaded_at_tick BIGINT NOT NULL
);
