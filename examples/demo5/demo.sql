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
\echo === [Q2] frontier-lab paper (must mention a lab; should mention a model; not a survey) ; intent: "evaluating frontier model limits" ===
SELECT title
FROM arxiv_idx a
WHERE abstract @@ ts_compound(
        ts_any(['OpenAI', 'Anthropic', 'DeepMind', 'Google']),
        ts_any(['survey', 'tutorial', 'workshop']),
        [ts_starts_with('gpt') ^ 2.0,
         ts_starts_with('claude'),
         ts_starts_with('gemini'),
         ts_levenshtein('chatgpt', 1)],
        1)
ORDER BY a.embedding <=> ai_embed(
           'evaluating frontier model limits',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;

-- Q3: prefix + proximity. "language model(s|ing)" via prefix,
-- "scaling law(s)" via `##` so "scaling power law" still hits, and a
-- regex catches the model-size shorthand 7B / 13B / 70B / 175B.
\echo === [Q3] scaling laws near language models ; intent: "scaling laws for language models" ===
SELECT title
FROM arxiv_idx a
WHERE abstract @@ (('scaling' ## [0, 1] ## ts_starts_with('law'))
                   && ('language' ## [0, 1] ## ts_starts_with('model')))
   OR abstract @@ ts_regexp('[0-9]+b')
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

-- Q5: structured filter on `published_date` composes with a compound
-- BM25 query: agent/tool terminology required (any of), reasoning hint
-- preferred (should), pure-RL / robotics papers demoted (must_not).
\echo === [Q5] pre-2024 ; intent: "LLM agents using tools" ===
SELECT published_date::DATE AS published, title
FROM arxiv_idx a
WHERE published_date <  TIMESTAMP '2024-01-01'
AND abstract @@ ts_compound(
        ts_any([ts_starts_with('agent'), ts_starts_with('tool'), ts_phrase('function calling')]),
        ts_any(['robotic', 'manipulation']),
        [ts_phrase('chain of thought') ^ 1.5,
         ts_phrase('react'),
         ts_levenshtein('toolformer', 1)],
        1)
ORDER BY a.embedding <=> ai_embed(
           'LLM agents using tools',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;
