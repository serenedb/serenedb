-- Step 1: Run on fresh server to set up data
-- ============================================

CREATE TEXT SEARCH DICTIONARY wal_dict(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'none',
    stemming = false,
    accent = false,
    frequency = true,
    position = true
);

CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT);
INSERT INTO docs VALUES
  (1, 'hello world', 'the quick brown fox jumps over the lazy dog'),
  (2, 'database systems', 'rocksdb is an embedded key value store'),
  (3, 'search engines', 'inverted indexes enable fast full text search');
CREATE INDEX docs_search ON docs USING inverted(id, title, body) WITH (commit_interval = 100000);
INSERT INTO docs VALUES
  (4, 'wal recovery', 'this row should be recovered from the write ahead log'),
  (5, 'crash test', 'data inserted after search commit was blocked');


-- Step 3: Crash the server
-- ========================

SET sdb_fault_crash_on_packet TO DEFAULT;
SELECT 1;

-- Step 4: After restart, verify recovery worked
-- ==============================================
-- Run these queries after restarting the server:

-- VACUUM (UPDATE_INDEXES) docs;
-- SELECT id, title FROM docs_search WHERE PHRASE(body, 'quick brown');
-- SELECT id, title FROM docs_search WHERE PHRASE(body, 'write ahead');
-- SELECT id, title FROM docs_search WHERE PHRASE(body, 'search commit');
-- SELECT COUNT(*) FROM docs;



NULL
NULL
NULL
NULL


NULL != NULL


shard_id / column_id / ...
