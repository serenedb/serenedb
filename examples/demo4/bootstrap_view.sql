-- Alternative bootstrap for demo4: register the Qdrant dbpedia-entities
-- dataset as a view over the remote parquet shards, instead of copying
-- it into a local table.
--
-- What's stored locally: only the iresearch posting lists and the HNSW
-- graph (rocksdb), not the row data. Materialised columns
-- (`SELECT title, ...`) are fetched from the URLs on demand through the
-- view fast-path, so queries that only need the index (count(*), BM25
-- score, ANN distance) stay fast while column-projecting queries pay an
-- HTTP fetch.
--
-- Schema note: a view-backed inverted index requires the view body to be
-- a flat projection of columns straight from
-- `read_parquet(<single literal string>)` -- no joins, no WHERE, no
-- expressions other than per-column casts and aliases, and the reader
-- must take exactly one VARCHAR argument. The table-flow variant
-- (bootstrap.sql) ingests all four shards (100K rows); the view-flow
-- variant uses only the first shard (~25K rows) so the body satisfies
-- the single-path restriction.
--
-- The upstream embedding column is a generic LIST<FLOAT>; we cast it to
-- FLOAT[1536] (HNSW requires a typed, fixed-length vector) and rename
-- the hyphenated upstream name to `embedding` so the same demo.sql runs
-- unchanged against either bootstrap.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap_view.sql

\timing on

DROP INDEX IF EXISTS dbpedia_idx;
DROP VIEW IF EXISTS dbpedia;
DROP TABLE IF EXISTS dbpedia;

CREATE VIEW dbpedia AS
  SELECT _id   AS entity_id,
         title,
         text,
         "text-embedding-3-small-1536-embedding"::FLOAT[1536] AS embedding
  FROM read_parquet(
    'hf://datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/**/*.parquet'
  );

SELECT count(*) AS rows FROM dbpedia;
