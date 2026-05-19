\timing on

DROP SECRET IF EXISTS gemini;

CREATE SECRET gemini (
    TYPE openai,
    api_key 'API_KEY',
    base_url 'https://generativelanguage.googleapis.com',
    embeddings_path '/v1beta/openai/embeddings'
  );

DROP INDEX IF EXISTS arxiv_idx;
DROP TABLE IF EXISTS arxiv;

CREATE TABLE arxiv (
  id             VARCHAR,
  title          VARCHAR,
  abstract       VARCHAR,
  authors        VARCHAR,
  published_date TIMESTAMP,
  embedding      FLOAT[3072]
);

INSERT INTO arxiv
SELECT id,
       title,
       abstract,
       authors,
       published_date,
       ai_embed(
         abstract,
         'gemini-embedding-001',
         'gemini')::FLOAT[3072] AS embedding
FROM (
  SELECT id, title, abstract, authors,
         strptime(published_date, '%Y-%m-%dT%H:%M:%SZ') AS published_date
  FROM read_parquet(
    'https://huggingface.co/datasets/neuralwork/arxiver/resolve/main/data/train.parquet')
  LIMIT 5000
) src;

SELECT count(*) AS rows FROM arxiv;
