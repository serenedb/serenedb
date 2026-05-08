-- SereneDB Demo: Full-text search over local Parquet
--
-- The local-data sibling of demo0. Same dataset (IMDb sentiment, 100k
-- reviews, 3 shards), same queries, but the Parquet files live on disk.
-- Network leaves the bottleneck: queries drop from seconds to milliseconds.
--
-- Prereq: stage the parquet files in /tmp once via bootstrap.sql:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
-- Then run this demo:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo.sql

\timing on

DROP INDEX IF EXISTS imdb_idx;
DROP VIEW IF EXISTS imdb_v;
DROP TEXT SEARCH DICTIONARY IF EXISTS imdb_en;

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

-- Same view shape as demo0, just pointing at the local glob produced by
-- bootstrap.sql. The (file_index, file_row_number) pair PK is auto-detected,
-- and per-shard materialisation runs in parallel at query time.
CREATE VIEW imdb_v AS
  SELECT * FROM read_parquet('/tmp/imdb_*.parquet');

-- Indexing now CPU-bound on the tokenizer rather than network-bound on HF.
-- Expect ~25s for 100k rows.
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
