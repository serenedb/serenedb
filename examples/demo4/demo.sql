-- SereneDB Demo: Vector + full-text hybrid search over 100K DBpedia
-- entity abstracts (Qdrant/dbpedia-entities-openai3-text-embedding-3-
-- small-1536-100K), each with a 1536-dim OpenAI text-embedding-3-small
-- vector.
--
-- One inverted index carries both an FTS analyzer over `text` and an
-- HNSW graph over `embedding`. Every query in this file uses the same
-- index -- BM25 phrase search, ANN top-K, range filtering, and hybrid
-- text+vector all hit one structure.
--
-- Prereq: stage the rows in a local table once via bootstrap.sql:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
-- Then run this demo:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo.sql

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

-- Reference vector: Albert Einstein's abstract embedding, captured into
-- the psql variable :qvec. `replace(..., ' ', '')` strips spaces so the
-- array text is a single token that `\bind` can pass as one parameter.
SELECT replace(embedding::TEXT, ' ', '') AS qvec
FROM dbpedia WHERE title = 'Endorphins' LIMIT 1
\gset

-- Hybrid index. text gets a BM25/phrase posting list, embedding gets an
-- HNSW graph under cosine distance. Both share the same rocksdb row
-- store, so filters and projections from one column compose with the
-- other for free.
CREATE INDEX dbpedia_idx ON dbpedia USING inverted(
  id,
  text       dbpedia_en,
  embedding  hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);


-- Q1: Pure vector ANN -- "entities most like Albert Einstein". The
-- reference embedding arrives as a PostgreSQL bind parameter ($1) via
-- the extended protocol; the cast `$1::FLOAT[1536]` folds at plan time
-- so the optimizer picks IRESEARCH_ANN_SCAN over the HNSW graph.
-- EXPLAIN SELECT title
-- FROM dbpedia_idx d
-- ORDER BY d.embedding <=> $1::FLOAT[1536]
-- LIMIT 5
-- \bind :qvec \g

SELECT title
FROM dbpedia_idx d
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q2: Range search -- everything within cosine distance 0.3 of the
-- reference embedding. Plans as IRESEARCH_ANN_RANGE_SCAN.
-- EXPLAIN SELECT d.title
-- FROM dbpedia_idx d
-- WHERE d.embedding <=> $1::FLOAT[1536] < 0.3
-- LIMIT 10
-- \bind :qvec \g

SELECT d.title
FROM dbpedia_idx d
WHERE d.embedding <=> $1::FLOAT[1536] < 0.3
LIMIT 10
\bind :qvec \g

-- Q3: Hybrid -- BM25 phrase filter + ANN order-by, both against the
-- same index in one round-trip. EXPLAIN should show IRESEARCH_ANN_SCAN
-- with the ts_any disjunction pushed in as a stored text filter.
-- EXPLAIN SELECT title, left(text, 80) AS snippet
-- FROM dbpedia_idx d
-- WHERE text @@ ts_any(['physicist', 'physics', 'scientist'])
-- ORDER BY d.embedding <=> $1::FLOAT[1536]
-- LIMIT 5
-- \bind :qvec \g

SELECT title, left(text, 80) AS snippet
FROM dbpedia_idx d
WHERE text @@ ts_any(['physicist', 'physics', 'scientist'])
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q4: Tighter phrase filter feeding the same ANN order-by.
SELECT title, left(text, 80) AS snippet
FROM dbpedia_idx d
WHERE text @@ ts_phrase('quantum mechanics')
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Cleanup (uncomment to drop the demo objects; dbpedia itself is durable):
-- DROP INDEX dbpedia_idx;
-- DROP TEXT SEARCH DICTIONARY dbpedia_en;
-- DROP TABLE dbpedia;
