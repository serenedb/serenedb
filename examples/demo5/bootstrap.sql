-- One-time bootstrap for demo5: load the Qdrant dbpedia-entities dataset
-- (100K DBpedia abstracts with 1536-dim OpenAI text-embedding-3-small
-- vectors) into a native SereneDB table.
--
-- Source: Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K
-- on Hugging Face -- four parquet shards, ~1.65 GB total.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

\timing on

DROP INDEX IF EXISTS dbpedia_idx;
DROP TABLE IF EXISTS dbpedia;

CREATE TABLE dbpedia (
  id         INTEGER PRIMARY KEY,
  entity_id  VARCHAR,
  title      VARCHAR,
  text       VARCHAR,
  embedding  FLOAT[1536]
);

-- Stream the four parquet shards straight from Hugging Face. The vector
-- column is named with hyphens upstream, so it's quoted on the read side
-- and aliased to the local schema name on insert.
INSERT INTO dbpedia
SELECT row_number() OVER ()::INTEGER AS id,
       _id    AS entity_id,
       title,
       text,
       emb    AS embedding
FROM (
  SELECT _id,
         title,
         text,
         "text-embedding-3-small-1536-embedding"::FLOAT[1536] AS emb
  FROM read_parquet([
    'https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/resolve/main/data/train-00000-of-00004.parquet',
    'https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/resolve/main/data/train-00001-of-00004.parquet',
    'https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/resolve/main/data/train-00002-of-00004.parquet',
    'https://huggingface.co/datasets/Qdrant/dbpedia-entities-openai3-text-embedding-3-small-1536-100K/resolve/main/data/train-00003-of-00004.parquet'
  ])
  WHERE text IS NOT NULL
    AND "text-embedding-3-small-1536-embedding" IS NOT NULL
);

SELECT count(*) AS rows FROM dbpedia;
