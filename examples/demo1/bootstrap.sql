-- One-time bootstrap for demo1: stage IMDb parquet shards locally.
--
-- Server-side fetch via COPY ... TO. Pulls each split from the auto-converted
-- parquet branch on Hugging Face and writes a local file per shard. The
-- multi-file layout matters: at query time, materialisation is dispatched
-- per shard in parallel, which keeps top-K latency in the tens of ms.
-- A single-file equivalent (one COPY over the recursive glob) works too,
-- but Q2 in demo.sql is several times slower against it.
--
-- ~30s on a typical broadband link. Re-running is safe (overwrites).
--
-- Run:
--   psql -h <host> -p <port> -U serenedb -d postgres -f bootstrap.sql

\timing on

COPY (SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/train/0000.parquet'))
  TO '/tmp/imdb_train.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/test/0000.parquet'))
  TO '/tmp/imdb_test.parquet' (FORMAT PARQUET);

COPY (SELECT * FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/unsupervised/0000.parquet'))
  TO '/tmp/imdb_unsupervised.parquet' (FORMAT PARQUET);
