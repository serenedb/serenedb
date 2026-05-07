-- One-time bootstrap for demo1: stage IMDb parquet shards locally.
--
-- Server-side fetch via COPY ... TO. Pulls each split from the auto-converted
-- parquet branch on Hugging Face and writes a local file per shard.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

\timing on

COPY (SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/train/0000.parquet'))
  TO '/tmp/imdb_train.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/test/0000.parquet'))
  TO '/tmp/imdb_test.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/unsupervised/0000.parquet'))
  TO '/tmp/imdb_unsupervised.parquet' (FORMAT PARQUET);
