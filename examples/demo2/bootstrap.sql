-- One-time bootstrap for demo2: load IMDb into a native SereneDB table.
--
-- Pulls the auto-converted parquet branch from Hugging Face once and inserts
-- the rows into a rocksdb-backed table with an INTEGER primary key. After
-- this runs, the demo no longer touches Hugging Face or local parquet --
-- everything lives in the native storage engine.
--
-- ~30s on a typical broadband link. Re-running drops and reloads.
--
-- Run:
--   psql -h <host> -p <port> -U serenedb -d postgres -f bootstrap.sql

\timing on

DROP INDEX IF EXISTS imdb_idx;
DROP TABLE IF EXISTS imdb;

CREATE TABLE imdb (
  id    INTEGER PRIMARY KEY,
  text  TEXT,
  label INTEGER
);

INSERT INTO imdb
SELECT row_number() OVER ()::INTEGER AS id,
       text,
       label::INTEGER
FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/**/*.parquet');

SELECT count(*) AS rows FROM imdb;
