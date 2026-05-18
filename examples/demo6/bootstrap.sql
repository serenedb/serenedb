\timing on

CREATE SECRET IF NOT EXISTS my_ollama (
  TYPE openai,
  api_key 'ollama',
  base_url 'http://localhost:11434'
);

DROP INDEX IF EXISTS arxiv_idx;
DROP TABLE IF EXISTS arxiv;

CREATE TABLE arxiv (
  id        VARCHAR PRIMARY KEY,
  title     VARCHAR,
  abstract  VARCHAR,
  authors VARCHAR,
  embedding FLOAT[384]
);

INSERT INTO arxiv
SELECT id,
       title,
       abstract,
       authors,
       generate_embedding(
         abstract,
         'openai',
         'snowflake-arctic-embed:22m',
         'my_ollama')::FLOAT[384] AS embedding
FROM (
  SELECT id, title, abstract, authors
  FROM read_parquet(
    'https://huggingface.co/datasets/neuralwork/arxiver/resolve/main/data/train.parquet')
);

SELECT count(*) AS rows FROM arxiv;
