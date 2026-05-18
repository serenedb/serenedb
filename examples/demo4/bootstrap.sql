-- One-time bootstrap for demo4: load the MongoDB embedded_movies dataset
-- into a native SereneDB table.
--
-- Source: MongoDB/embedded_movies on Hugging Face -- a JSON snapshot of the
-- sample_mflix movies collection with a 1536-dimension OpenAI ada-002
-- embedding (`plot_embedding`) per movie. ~1500 movies, ~100 MB JSON.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f bootstrap.sql

\timing on

DROP INDEX IF EXISTS movies_idx;
DROP TABLE IF EXISTS movies;

CREATE TABLE movies (
  id              INTEGER PRIMARY KEY,
  title           VARCHAR,
  runtime         BIGINT,
  imdb_info       STRUCT(rating DOUBLE, votes BIGINT, id BIGINT),
  genres          VARCHAR,
  "cast"          VARCHAR,
  fullplot        VARCHAR,
  plot_embedding  FLOAT[1536]
);

-- Stream the JSON array straight from Hugging Face. Rows without a plot or
-- embedding are dropped -- they can't participate in either search.
INSERT INTO movies
SELECT row_number() OVER ()::INTEGER AS id,
       title,
       runtime,
       imdb_info,
       array_to_string(genres, ', ')  AS genres,
       array_to_string("cast", ', ')  AS "cast",
       fullplot,
       emb                            AS plot_embedding
FROM (
  SELECT title,
         genres,
         "cast",
         fullplot,
         runtime,
         imdb                 AS imdb_info,
         plot_embedding::FLOAT[1536] AS emb
  FROM read_json(
         'https://huggingface.co/datasets/MongoDB/embedded_movies/resolve/main/sample_mflix.embedded_movies.json',
         format='array',
         columns={
           title: 'VARCHAR',
           runtime: 'BIGINT',
           imdb: 'STRUCT(rating DOUBLE, votes BIGINT, id BIGINT)',
           genres: 'VARCHAR[]',
           "cast": 'VARCHAR[]',
           fullplot: 'VARCHAR',
           plot_embedding: 'DOUBLE[]'
         })
  WHERE fullplot IS NOT NULL
    AND plot_embedding IS NOT NULL
);

SELECT count(*) AS rows FROM movies;
