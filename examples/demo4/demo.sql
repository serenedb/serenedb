-- SereneDB Demo: Vector + full-text hybrid search over a movie catalogue.
--
-- One inverted index carries both an FTS analyzer over `fullplot` and an
-- HNSW graph over a 1536-dim `plot_embedding` (OpenAI ada-002). Every
-- query in this file uses the same index -- BM25 phrase search, ANN
-- top-K, range filtering, and hybrid text+vector all hit one structure.
--
-- Prereq: stage the rows in a local table once via bootstrap.sql:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
-- Then run this demo:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo.sql

\timing on

DROP INDEX IF EXISTS movies_idx;
DROP TEXT SEARCH DICTIONARY IF EXISTS movies_en;
DROP TEXT SEARCH DICTIONARY IF EXISTS movies_genres;

CREATE TEXT SEARCH DICTIONARY movies_en(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'lower',
    stemming = false,
    accent = false,
    frequency = true,
    position = true,
    norm = true
);

-- genres is stored as `"Action, Comedy, Sci-Fi"` (array_to_string with
-- `, ` separator in bootstrap.sql). Split on the literal `, ` and then
-- lowercase each emitted genre so `ts_phrase('sci-fi')` matches `Sci-Fi`.
CREATE TEXT SEARCH DICTIONARY movies_genres(
    template = 'pipeline',
    step1_template = 'delimiter',
    step1_delimiter = ', ',
    step2_template = 'norm',
    step2_locale = 'en_US.UTF-8',
    step2_case = 'lower',
    position = true,
    frequency = true
);

-- Hybrid index. fullplot gets a BM25/phrase posting list, plot_embedding
-- gets an HNSW graph under cosine distance. Both share the same rocksdb
-- row store, so filters and projections from one column compose with
-- the other for free.
CREATE INDEX movies_idx ON movies USING inverted(
  id,
  fullplot       movies_en,
  genres         movies_genres,
  plot_embedding hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);

-- Q1: Pure FTS top-K. BM25 over a phrase, with positional matching.
-- EXPLAIN should show IRESEARCH_SCAN with the phrase as the index filter
-- and BM25 attached as the scorer (no separate sort).
-- EXPLAIN SELECT title, imdb_info.rating,
--        round(BM25(movies_idx.tableoid)::numeric, 2) AS bm25_score,
--        left(fullplot, 50) AS snippet
-- FROM movies_idx
-- WHERE fullplot @@ ts_phrase('time travel')
-- ORDER BY BM25(movies_idx.tableoid) DESC
-- LIMIT 5;

SELECT title, imdb_info.rating,
       round(BM25(movies_idx.tableoid)::numeric, 2) AS bm25_score,
       left(fullplot, 50) AS snippet
FROM movies_idx
WHERE fullplot @@ ts_phrase('time travel')
ORDER BY BM25(movies_idx.tableoid) DESC
LIMIT 5;

-- Reference vector: Die Hard's plot embedding, captured into the psql
-- variable :qvec. `replace(..., ' ', '')` strips spaces so the array
-- text is a single token that `\bind` can pass as one parameter.
SELECT replace(plot_embedding::TEXT, ' ', '') AS qvec
FROM movies WHERE title = 'Die Hard' LIMIT 1
\gset

-- Q2: Pure vector ANN -- "movies most like Die Hard". The reference
-- embedding arrives as a PostgreSQL bind parameter ($1) via the
-- extended protocol; the cast `$1::FLOAT[1536]` folds at plan time so
-- the optimizer picks IRESEARCH_ANN_SCAN over the HNSW graph.
-- EXPLAIN SELECT title, imdb_info.rating
-- FROM movies_idx m
-- ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
-- LIMIT 5
-- \bind :qvec \g

SELECT title, imdb_info.rating
FROM movies_idx m
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q3: Range search -- everything within cosine distance 0.2 of the
-- reference embedding. Plans as IRESEARCH_ANN_RANGE_SCAN.
-- EXPLAIN SELECT m.title
-- FROM movies_idx m
-- WHERE m.plot_embedding <=> $1::FLOAT[1536] < 0.2
-- LIMIT 5
-- \bind :qvec \g

SELECT m.title
FROM movies_idx m
WHERE m.plot_embedding <=> $1::FLOAT[1536] < 0.2
LIMIT 5
\bind :qvec \g

-- Q4: Hybrid -- BM25 phrase filter + ANN order-by, both against the
-- same index in one round-trip. EXPLAIN should show IRESEARCH_ANN_SCAN
-- with the ts_any disjunction pushed in as a stored text filter.
-- EXPLAIN SELECT title, left(fullplot, 50) AS plot, imdb_info.rating
-- FROM movies_idx m
-- WHERE fullplot @@ ts_any(['detective', 'heist'])
-- ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
-- LIMIT 5
-- \bind :qvec \g

SELECT title, left(fullplot, 50) AS plot, imdb_info.rating
FROM movies_idx m
WHERE fullplot @@ ts_any(['detective', 'heist'])
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

SELECT title, left(fullplot, 50) AS plot, imdb_info.rating
FROM movies_idx m
WHERE genres @@ ts_phrase('sci-fi')
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g


SELECT title, left(fullplot, 50) AS plot, imdb_info.rating AS rating, runtime
FROM movies_idx m
WHERE imdb_info.rating BETWEEN 5.5 AND 6.5 AND runtime < 110 AND fullplot @@ ts_any(['detective', 'heist'])
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 3
\bind :qvec \g 

-- Cleanup (uncomment to drop the demo objects; movies itself is durable):
-- DROP INDEX movies_idx;
-- DROP TEXT SEARCH DICTIONARY movies_en;
-- DROP TEXT SEARCH DICTIONARY movies_genres;
-- DROP TABLE movies;
