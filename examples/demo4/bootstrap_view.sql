-- Alternative bootstrap for demo4: register the MongoDB embedded_movies
-- dataset as a view over the remote JSON, instead of copying it into a
-- local table.
--
-- What's stored locally: only the iresearch posting lists and the HNSW
-- graph (rocksdb), not the row data. Materialised columns
-- (`SELECT title, ...`) are fetched from the URL on demand through the
-- view fast-path, so queries that only need the index (count(*), BM25
-- score, ANN distance) stay fast while column-projecting queries pay an
-- HTTP fetch.
--
-- Schema note: a view-backed inverted index requires the view body to be
-- `SELECT * FROM read_json(literal_args)` -- no renames, no WHERE, no
-- transforms. That means column names follow the JSON keys (so `imdb`,
-- not `imdb_info`; `genres` stays VARCHAR[], not the comma-joined string
-- the table version produces). demo.sql expects the table-version names
-- and will not run unmodified against this view.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap_view.sql

\timing on

DROP INDEX IF EXISTS movies_idx;
DROP VIEW IF EXISTS movies;
DROP TABLE IF EXISTS movies;

-- `columns={...}` pins the schema and -- crucially -- types
-- plot_embedding as FLOAT[1536] so the HNSW index can attach to it.
-- DOUBLE[] (the default JSON inference) would not satisfy the typed
-- vector column requirement.
CREATE VIEW movies AS
  SELECT * FROM read_json(
    'https://huggingface.co/datasets/MongoDB/embedded_movies/resolve/main/sample_mflix.embedded_movies.json',
    format='array',
    columns={
      title: 'VARCHAR',
      runtime: 'BIGINT',
      imdb: 'STRUCT(rating DOUBLE, votes BIGINT, id BIGINT)',
      genres: 'VARCHAR[]',
      "cast": 'VARCHAR[]',
      fullplot: 'VARCHAR',
      plot_embedding: 'FLOAT[1536]'
    });

SELECT count(*) AS rows FROM movies;
