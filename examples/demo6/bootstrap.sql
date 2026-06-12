-- One-time bootstrap for demo6: multi-source corpora as PURE VIEWS over
-- Hugging Face parquet, indexed with every column INCLUDEd.
--
-- The pattern this demo showcases (no COPY, no native tables):
--   1. one view per source, each reshaping its own parquet schema in SQL
--      (renames, casts, CASE mappings, computed columns, generated ids);
--   2. one UNION ALL mega view gluing the per-source views together;
--   3. one inverted index over the mega view where every retrieved column is
--      also INCLUDEd -- after the build the index is self-sufficient: queries
--      never re-read the parquet, even across restarts.
--
-- Sources (all openly licensed, attribution in their dataset cards):
--   * open-r1/codeforces (CC-BY-4.0) -- ~10k Codeforces problems with
--     statements, tags, ratings, editorials.
--   * deepmind/code_contests (CC-BY-4.0) -- problems from CodeChef, AtCoder,
--     HackerEarth, AIZU and others; different schema, reshaped in its view.
--   * open-r1/codeforces-submissions, `selected_accepted` config (CC-BY-4.0)
--     -- ~42k curated accepted submissions with per-run time and memory.
--
-- URLs last verified: 2026-06-12.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

\timing on

-- Hugging Face rate-limits anonymous range reads; retry through 429s.
SET http_retries = 10;
SET http_retry_wait_ms = 3000;

DROP INDEX IF EXISTS tasks_idx;
DROP INDEX IF EXISTS solutions_idx;
DROP VIEW IF EXISTS tasks_v;
DROP VIEW IF EXISTS tasks_cf_v;
DROP VIEW IF EXISTS tasks_cc_v;
DROP VIEW IF EXISTS solutions_v;
DROP TABLE IF EXISTS tasks;      -- older demo revisions staged tables
DROP TABLE IF EXISTS solutions;
DROP TEXT SEARCH DICTIONARY IF EXISTS cf_en;
DROP TEXT SEARCH DICTIONARY IF EXISTS code_grams;
DROP TEXT SEARCH DICTIONARY IF EXISTS code_grams_q;

-- English analyzer for statements/editorials. norm/frequency/position enable
-- BM25 with positional phrases.
CREATE TEXT SEARCH DICTIONARY cf_en(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'lower',
    stemming = true,
    accent = false,
    frequency = true,
    position = true,
    norm = true
);

-- Index-side sparse ngrams: all grams, so every substring stays coverable.
CREATE TEXT SEARCH DICTIONARY code_grams(
    template = 'sparse_ngram',
    frequency = true,
    norm = true
);

-- Query-side covering mode: the minimal gram chain for a substring. Never
-- bound to an index -- referenced by name in ts_tokenize at query time.
CREATE TEXT SEARCH DICTIONARY code_grams_q(
    template = 'sparse_ngram',
    covering = true
);

-- Source 1: Codeforces problems. Reshaping in the view: rename
-- description -> statement, flatten the tags list, default missing ratings.
CREATE VIEW tasks_cf_v AS
SELECT id,
       title,
       contest_name,
       COALESCE(rating, 0)::INTEGER AS rating,
       array_to_string(tags, ', ')  AS tags,
       description                  AS statement,
       COALESCE(editorial, '')      AS editorial
FROM read_parquet('hf://datasets/open-r1/codeforces@~parquet/default/*/*.parquet')
WHERE description IS NOT NULL;

-- Source 2: code_contests, a completely different schema -- per-source SQL
-- maps it onto the same shape: a generated id, name -> title, judge enum ->
-- contest_name, cf_tags list -> string. Codeforces rows are excluded (they
-- would duplicate source 1).
CREATE VIEW tasks_cc_v AS
SELECT 'cc/' || row_number() OVER () AS id,
       name                          AS title,
       CASE source WHEN 1 THEN 'CodeChef' WHEN 3 THEN 'HackerEarth'
                   WHEN 4 THEN 'CodeJam' WHEN 5 THEN 'AtCoder'
                   WHEN 6 THEN 'AIZU' ELSE 'Other judge' END AS contest_name,
       COALESCE(cf_rating, 0)::INTEGER AS rating,
       array_to_string(cf_tags, ', ')  AS tags,
       description                     AS statement,
       ''                              AS editorial
FROM read_parquet('hf://datasets/deepmind/code_contests@~parquet/default/*/*.parquet')
WHERE description IS NOT NULL AND length(description) > 50 AND source != 2;

-- The mega view: one relation over both sources.
CREATE VIEW tasks_v AS
SELECT * FROM tasks_cf_v
UNION ALL
SELECT * FROM tasks_cc_v;

-- Every column is either an index key or INCLUDEd (the analyzed columns are
-- in both lists), so the index alone can answer any query below.
CREATE INDEX tasks_idx ON tasks_v
USING inverted(id, rating, title cf_en, statement cf_en, editorial cf_en, tags cf_en)
INCLUDE (id, contest_name, rating, title, statement, editorial, tags);

SELECT count(*) AS tasks FROM tasks_idx;

-- Solutions: the curated accepted set, with per-run measurements kept as
-- INCLUDE columns -- the submission id is a natural unique key, so no
-- row_number() is needed (a global window would serialize the build; with a
-- natural id the parallel index sink runs on all cores).
CREATE VIEW solutions_v AS
SELECT submission_id::BIGINT AS id,
       problem_id            AS task_id,
       source                AS code,
       length(source)        AS code_len,
       programmingLanguage   AS lang,
       timeconsumedmillis::INTEGER           AS time_ms,
       (memoryconsumedbytes / 1024)::INTEGER AS memory_kb
FROM read_parquet('hf://datasets/open-r1/codeforces-submissions@~parquet/selected_accepted/train/*.parquet')
WHERE verdict = 'OK' AND length(source) BETWEEN 20 AND 10000;

CREATE INDEX solutions_idx ON solutions_v
USING inverted(id, task_id, lang, code code_grams)
INCLUDE (id, task_id, code, code_len, lang, time_ms, memory_kb);

SELECT count(*) AS solutions FROM solutions_idx;
