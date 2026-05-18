\timing on

CREATE SECRET IF NOT EXISTS my_ollama (
  TYPE openai,
  api_key 'ollama',
  base_url 'http://localhost:11434'
);

DROP INDEX IF EXISTS arxiv_idx;
DROP TEXT SEARCH DICTIONARY IF EXISTS arxiv_en;

CREATE TEXT SEARCH DICTIONARY arxiv_en(
    template  = 'text',
    locale    = 'en_US.UTF-8',
    case      = 'lower',
    stemming  = false,
    accent    = false,
    frequency = true,
    position  = true,
    norm      = true
);

CREATE INDEX arxiv_idx ON arxiv USING inverted(
  id,
  abstract   arxiv_en,
  embedding  hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);

-- Q1: semantic ANN, NL query embedded inline.
SELECT title,
       left(abstract, 120) AS snippet
FROM arxiv_idx a
ORDER BY a.embedding <=> generate_embedding(
           'transformer architectures for protein structure prediction',
           'openai',
           'snowflake-arctic-embed:22m',
           'my_ollama')::FLOAT[384]
LIMIT 5;

-- Q2: hybrid -- FTS keyword filter + ANN order-by from a NL string.
SELECT title, left(abstract, 120) AS snippet
FROM arxiv_idx a
WHERE abstract @@ ts_any(['reinforcement', 'reward', 'policy'])
ORDER BY a.embedding <=> generate_embedding(
           'sample-efficient policy gradient methods for robotic manipulation',
           'openai',
           'snowflake-arctic-embed:22m',
           'my_ollama')::FLOAT[384]
LIMIT 5;

-- Q3: phrase prune + semantic re-rank.
SELECT title, left(abstract, 120) AS snippet
FROM arxiv_idx a
WHERE abstract @@ ts_phrase('graph neural network')
ORDER BY a.embedding <=> generate_embedding(
           'molecular property prediction on small chemistry datasets',
           'openai',
           'snowflake-arctic-embed:22m',
           'my_ollama')::FLOAT[384]
LIMIT 5;

-- Q4: range search -- everything within cosine distance 0.45 of an NL query.
SELECT title
FROM arxiv_idx a
WHERE a.embedding <=> generate_embedding(
        'self-supervised contrastive pretraining for vision encoders',
        'openai',
        'snowflake-arctic-embed:22m',
        'my_ollama')::FLOAT[384] < 0.45
LIMIT 20;
