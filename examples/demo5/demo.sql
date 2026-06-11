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
WHERE abstract @@ (ts_phrase('OpenAI')
                   && !!ts_phrase('survey')
                   && (ts_starts_with('gpt') || ts_starts_with('gemini')))
ORDER BY a.embedding <=> ai_embed(
           'evaluating frontier model limits',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;

-- Q5: structured filter on `published_date` composes with a compound
-- BM25 query: agent/tool terminology required (any of), reasoning hint
-- preferred (should), pure-RL / robotics papers demoted (must_not).
\echo === [Q5] pre-2024 ; intent: "LLM agents using tools" ===
SELECT published_date::DATE AS published, title
FROM arxiv_idx a
WHERE published_date <  TIMESTAMP '2024-01-01'
AND abstract @@ ((ts_starts_with('agent') || ts_starts_with('tool'))
                  && !!ts_phrase('robotic')
                  && ts_phrase('chain of thought'))
ORDER BY a.embedding <=> ai_embed(
           'LLM agents using tools',
           'gemini-embedding-001',
           'gemini')::FLOAT[3072]
LIMIT 5;
