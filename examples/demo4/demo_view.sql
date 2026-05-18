-- SereneDB Demo (view variant): Vector + full-text hybrid search over a
-- movie catalogue, with the data sourced from a remote JSON via a view
-- instead of a local table.
--
-- Prereq: register the view once via bootstrap_view.sql:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap_view.sql
-- Then run this demo:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo_view.sql
--
-- Differences from demo.sql:
--   * `imdb` is the raw JSON struct (not `imdb_info` from the table
--     variant); rating reads as `imdb.rating`.
--   * `genres` is VARCHAR[] in the view, not the comma-joined VARCHAR
--     the table variant produces. The genres filter uses
--     `list_contains(genres, 'Sci-Fi')` as a residual filter rather than
--     a tokenised FTS predicate.
--   * `genres` is not indexed (no FTS on array columns in this demo);
--     the inverted index covers fullplot + plot_embedding only.

\timing on

DROP INDEX IF EXISTS movies_idx;
DROP TEXT SEARCH DICTIONARY IF EXISTS movies_en;

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

-- Hybrid index over the view. fullplot gets BM25/phrase, plot_embedding
-- gets HNSW under cosine distance. Both write into rocksdb; the row
-- data still lives at the URL and is fetched on column materialisation.
CREATE INDEX movies_idx ON movies USING inverted(
  fullplot       movies_en,
  plot_embedding hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);

-- Q1: Pure FTS top-K. BM25 over a phrase, with positional matching.
SELECT title, imdb.rating,
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

-- Q2: Pure vector ANN -- "movies most like Die Hard".
SELECT title, imdb.rating
FROM movies_idx m
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q3: Range search -- everything within cosine distance 0.2 of the
-- reference embedding. Plans as IRESEARCH_ANN_RANGE_SCAN.
SELECT m.title
FROM movies_idx m
WHERE m.plot_embedding <=> $1::FLOAT[1536] < 0.2
LIMIT 5
\bind :qvec \g

-- Q4a: Hybrid -- BM25 phrase filter + ANN order-by, both against the
-- same index in one round-trip.
SELECT title, left(fullplot, 50) AS plot, imdb.rating
FROM movies_idx m
WHERE fullplot @@ ts_any(['detective', 'heist'])
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q4b: Genre filter without FTS -- `list_contains` matches the array
-- exactly. Applied as a residual filter on top of the ANN scan.
SELECT title, left(fullplot, 50) AS plot, imdb.rating, genres
FROM movies_idx m
WHERE list_contains(genres, 'Sci-Fi')
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q4c: Multi-predicate hybrid -- rating range + runtime cap + plot
-- keyword + ANN order-by.
SELECT title, left(fullplot, 50) AS plot, imdb.rating AS rating, runtime
FROM movies_idx m
WHERE imdb.rating BETWEEN 5.5 AND 6.5
  AND runtime < 110
  AND fullplot @@ ts_any(['detective', 'heist'])
ORDER BY m.plot_embedding <=> $1::FLOAT[1536]
LIMIT 3
\bind :qvec \g

-- Cleanup (uncomment to drop the demo objects; movies view itself is durable):
-- DROP INDEX movies_idx;
-- DROP TEXT SEARCH DICTIONARY movies_en;
-- DROP VIEW movies;
