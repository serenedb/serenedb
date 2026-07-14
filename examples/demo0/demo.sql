-- SereneDB Demo: Full-text search over remote Parquet
--
-- Indexes the IMDb sentiment dataset (100k movie reviews, sharded across
-- three Parquet files) directly from Hugging Face. No download, no ETL,
-- no COPY. The hf:// glob expands across all shards via DuckDB's httpfs
-- extension, the index is built straight from the remote stream, and
-- BM25-ranked rows materialise back through the same view.
--
-- Source: stanfordnlp/imdb (auto-converted parquet branch on Hugging Face).
-- URL last verified: 2026-05-04.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo.sql

\timing on

DROP INDEX IF EXISTS imdb_idx;
DROP VIEW IF EXISTS imdb_v;
DROP TEXT SEARCH DICTIONARY IF EXISTS imdb_en;

-- Tokenizer config. norm/frequency/position together enable BM25 with
-- positional phrase matching.
CREATE TEXT SEARCH DICTIONARY imdb_en(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'lower',
    stemming = false,
    accent = false,
    frequency = true,
    position = true,
    norm = true
);

-- The view is the table. The hf:// glob expands across train, test, and
-- unsupervised splits (3 files, 100k rows). SereneDB recognises
-- `SELECT * FROM read_parquet(literal)` as a fast-path source and builds
-- a (file_index, file_row_number) pair PK automatically.
-- Labels: 0 = negative, 1 = positive, -1 = unsupervised (no label).
CREATE VIEW imdb_v AS
  SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/**/*.parquet');

-- Builds an iresearch inverted index over the remote stream.
CREATE INDEX imdb_idx ON imdb_v USING inverted(text imdb_en, label);

-- Q1: phrase count.
SELECT count(*) AS hits
FROM imdb_idx
WHERE text @@ ts_phrase('breathtaking cinematography');

-- Q2: top-K with BM25 over a compound query. Native operator syntax:
--   `'plot' ## 'twist'`   -- adjacent phrase
--   `(... ^ 3.0)`         -- 3x score boost on that phrase
--   `... || ...`          -- OR with a second phrase
SELECT label,
       round(BM25(imdb_idx.tableoid)::numeric, 2) AS score,
       left(text, 100) AS snippet
FROM imdb_idx
WHERE text @@ ((('plot' ## 'twist') ^ 3) || 'surprise ending')
ORDER BY BM25(imdb_idx.tableoid) DESC
LIMIT 5;

-- Q2 (scaling): same query at K = 10, 100, 1000.
\o /dev/null
SELECT text FROM imdb_idx
WHERE text @@ ((('plot' ## 'twist') ^ 3) || 'surprise ending')
ORDER BY BM25(imdb_idx.tableoid) DESC LIMIT 10;
SELECT text FROM imdb_idx
WHERE text @@ ((('plot' ## 'twist') ^ 3) || 'surprise ending')
ORDER BY BM25(imdb_idx.tableoid) DESC LIMIT 100;
SELECT text FROM imdb_idx
WHERE text @@ ((('plot' ## 'twist') ^ 3) || 'surprise ending')
ORDER BY BM25(imdb_idx.tableoid) DESC LIMIT 1000;
\o

-- Q3: hybrid analytics -- BM25-filtered docs feed straight into SQL aggregation.
-- avg(label) over the labeled subset (0/1) is "positivity".
SELECT 'worst movie ever' AS query,
       count(*) AS reviews,
       round(avg(label::float)::numeric, 3) AS positivity
FROM imdb_idx
WHERE text @@ ts_phrase('worst movie ever') AND label >= 0
UNION ALL
SELECT 'a masterpiece',
       count(*),
       round(avg(label::float)::numeric, 3)
FROM imdb_idx
WHERE text @@ ts_phrase('a masterpiece') AND label >= 0;

-- Q4: FTS predicate composes with JOIN. The label column is indexed; we
-- equi-join it to a tiny VALUES lookup that names each sentiment, then
-- aggregate BM25 score per sentiment. One query, one round-trip.
WITH labels(label, name) AS (VALUES
  (-1, 'unsupervised'),
  ( 0, 'negative'),
  ( 1, 'positive')
)
SELECT l.name AS sentiment,
       count(*) AS reviews,
       round(avg(BM25(imdb_idx.tableoid))::numeric, 2) AS avg_relevance,
       round(max(BM25(imdb_idx.tableoid))::numeric, 2) AS top_relevance
FROM imdb_idx JOIN labels l ON imdb_idx.label = l.label
WHERE text @@ ((('plot' ## 'twist') ^ 3) || 'surprise ending')
GROUP BY l.name
ORDER BY reviews DESC;

-- Cleanup (uncomment to drop the demo objects):
-- DROP INDEX imdb_idx;
-- DROP VIEW imdb_v;
-- DROP TEXT SEARCH DICTIONARY imdb_en;
