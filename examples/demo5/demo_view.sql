-- SereneDB Demo (view variant): Vector + full-text hybrid search over
-- 100K DBpedia entity abstracts, sourced from remote parquet via a view
-- instead of a local table.
--
-- Prereq: register the view once via bootstrap_view.sql:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap_view.sql
-- Then run this demo:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo_view.sql
--
-- Differences from demo.sql:
--   * No synthetic `id` column is materialised; the inverted index
--     covers `text` + `embedding` only.
--   * Everything else (column names, query shapes) matches demo.sql --
--     the view body in bootstrap_view.sql renames the upstream
--     hyphenated vector column and pins it to FLOAT[1536].

\timing on

DROP INDEX IF EXISTS dbpedia_idx;
DROP TEXT SEARCH DICTIONARY IF EXISTS dbpedia_en;

CREATE TEXT SEARCH DICTIONARY dbpedia_en(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'lower',
    stemming = false,
    accent = false,
    frequency = true,
    position = true,
    norm = true
);

-- Hybrid index over the view. `text` gets BM25/phrase, `embedding` gets
-- HNSW under cosine distance. Both write into rocksdb; the row data
-- still lives at the URLs and is fetched on column materialisation.
CREATE INDEX dbpedia_idx ON dbpedia USING inverted(
  text       dbpedia_en,
  embedding  hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);

-- Q1: Pure FTS top-K. BM25 over a phrase, with positional matching.
SELECT title,
       round(BM25(dbpedia_idx.tableoid)::numeric, 2) AS bm25_score,
       left(text, 80) AS snippet
FROM dbpedia_idx
WHERE text @@ ts_phrase('general relativity')
ORDER BY BM25(dbpedia_idx.tableoid) DESC
LIMIT 5;

-- Reference vector: Edward Tryon's embedding (a physicist whose entity
-- lives in the first parquet shard this view reads). Captured into the
-- psql variable :qvec; `replace(..., ' ', '')` strips spaces so the
-- array text is a single token that `\bind` can pass as one parameter.
SELECT replace(embedding::TEXT, ' ', '') AS qvec
FROM dbpedia WHERE title = 'Edward Tryon' LIMIT 1
\gset

-- Q2: Pure vector ANN -- "entities most like Edward Tryon".
SELECT title
FROM dbpedia_idx d
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q3: Range search -- everything within cosine distance 0.3 of the
-- reference embedding. Plans as IRESEARCH_ANN_RANGE_SCAN.
SELECT d.title
FROM dbpedia_idx d
WHERE d.embedding <=> $1::FLOAT[1536] < 0.3
LIMIT 10
\bind :qvec \g

-- Q4: Hybrid -- BM25 phrase filter + ANN order-by, both against the
-- same index in one round-trip.
SELECT title, left(text, 80) AS snippet
FROM dbpedia_idx d
WHERE text @@ ts_any(['physicist', 'physics', 'scientist'])
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q5: Tighter phrase filter feeding the same ANN order-by.
SELECT title, left(text, 80) AS snippet
FROM dbpedia_idx d
WHERE text @@ ts_phrase('quantum mechanics')
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Cleanup (uncomment to drop the demo objects; dbpedia view itself is durable):
-- DROP INDEX dbpedia_idx;
-- DROP TEXT SEARCH DICTIONARY dbpedia_en;
-- DROP VIEW dbpedia;
