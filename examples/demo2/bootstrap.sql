-- One-time bootstrap for demo2: load IMDb into a native SereneDB table.
--
-- Pulls the auto-converted parquet branch from Hugging Face once and inserts
-- the rows into a rocksdb-backed table.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

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
