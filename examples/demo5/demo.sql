\timing on
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
  abstract   arxiv_en,
  embedding  hnsw (metric = 'cosine', m = 32, ef_construction = 64)
);

-- Q1: pure semantic search. A real user types their intent, not keywords.
-- Nothing forces the abstract to contain the words "small" or "GPT-4" --
-- ANN finds papers about the *idea*: getting compact models to reason well.
\echo === [Q1] "distill reasoning into a small model" ===
SELECT title
FROM arxiv_idx a
ORDER BY a.embedding <=> ai_embed(
           'Compaction in LLM',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;

-- Q2: hybrid. The user wants papers from a specific lab and describes
-- what they care about separately. FTS scopes to the lab; ANN reranks
-- by the property they actually want.
\echo === [Q2] mentions OpenAI/GPT-4/ChatGPT ; intent: "evaluating frontier model limits" ===
SELECT title
FROM arxiv_idx a
WHERE abstract @@ ts_any(['OpenAI', 'GPT-4', 'ChatGPT'])
ORDER BY a.embedding <=> ai_embed(
           'evaluating frontier model limits',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;

-- Q3: phrase prune + semantic re-rank. User pins the source ("Google")
-- and asks for results about a separate property.
\echo === [Q3] must mention "OpenAI" ; intent: "scaling laws for language models" ===
SELECT title
FROM arxiv_idx a
WHERE abstract @@ ts_phrase('OpenAI')
ORDER BY a.embedding <=> ai_embed(
           'scaling laws for language models',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;

-- Q4: "show me everything related to X" -- cosine-distance range search
-- for survey-style browsing. Returns the cluster and its tightness.
\echo === [Q4] cluster around: "LoRA fine-tuning" (dist < 0.35) ===
SELECT round((a.embedding <=> ai_embed(
        'LoRA fine-tuning',
        'gemini-embedding-001',
        'gemini')::FLOAT[3072])::numeric, 3) AS dist,
       title
FROM arxiv_idx a
WHERE a.embedding <=> ai_embed(
        'LoRA fine-tuning',
        'gemini-embedding-001',
        'gemini')::FLOAT[3072] < 0.35
ORDER BY dist
LIMIT 5;

\echo === [Q5] published 2023-07..2023-12 ; intent: "LLM agents using tools" ===
SELECT published_date::DATE AS published, title
FROM arxiv_idx a
WHERE published_date <  TIMESTAMP '2024-01-01'
AND abstract @@ ts_phrase('OpenAI')
ORDER BY a.embedding <=> ai_embed(
           'LLM agents using tools',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;
