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
-- Prereq: stage `dbpedia` once via either bootstrap (table or view):
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql
--   -- or --
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap_view.sql
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

-- Hybrid index. text gets a BM25/phrase posting list, embedding gets an
-- HNSW graph under cosine distance. Both share the same rocksdb row
-- store, so filters and projections from one column compose with the
-- other for free. Works against both bootstraps -- the view variant
-- fetches row data from the URLs on column materialisation.
CREATE INDEX dbpedia_idx ON dbpedia USING inverted(
  text       dbpedia_en,
  embedding  hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);

SELECT replace(d.embedding::TEXT, ' ', '') AS qvec,
       d.title AS qtitle
FROM dbpedia_idx d
WHERE text @@ ts_phrase('general relativity')
LIMIT 1
\gset

-- Q1: Pure vector ANN -- entities most like the reference doc. The
-- embedding arrives as a PostgreSQL bind parameter ($1) via the extended
-- protocol; the cast `$1::FLOAT[1536]` folds at plan time so the
-- optimizer picks IRESEARCH_ANN_SCAN over the HNSW graph.

SELECT title
FROM dbpedia_idx d
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q2: Range search -- everything within cosine distance 0.3 of the
-- reference embedding. Plans as IRESEARCH_ANN_RANGE_SCAN.
SELECT d.title
FROM dbpedia_idx d
WHERE d.embedding <=> $1::FLOAT[1536] < 0.3
LIMIT 10
\bind :qvec \g

-- Q3: Hybrid -- boolean BM25 filter composed of &&, ||, !!: require
-- "physicist", forbid philosophy/theology, and require any of
-- "quantum mechanics" / "general relativity" / a relativ-* or
-- particle-* token. Prefix expansion is pushed into the same filter.
SELECT title, left(text, 80) AS snippet
FROM dbpedia_idx d
WHERE text @@ (ts_phrase('physicist')
               && !!ts_phrase('philosophy')
               && (ts_phrase('quantum mechanics') || ts_phrase('general relativity')))
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Q4: Proximity + fuzzy + regex. `##` matches "quantum" next to
-- "mechanics"; ts_levenshtein catches "Schrodinger" / "Schroedinger" /
-- "Schrödinger" with edit distance 2; ts_regexp restricts to
-- Heisenberg / Heisenburg.
SELECT title, left(text, 80) AS snippet
FROM dbpedia_idx d
WHERE text @@ (('quantum' ## 'mechanics')
               || ts_levenshtein('Schrodinger', 2)
               || ts_regexp('heisen[bu]+rg'))
ORDER BY d.embedding <=> $1::FLOAT[1536]
LIMIT 5
\bind :qvec \g

-- Cleanup (uncomment to drop the demo objects; dbpedia itself is durable):
-- DROP INDEX dbpedia_idx;
-- DROP TEXT SEARCH DICTIONARY dbpedia_en;
-- DROP TABLE dbpedia;   -- or: DROP VIEW dbpedia;
