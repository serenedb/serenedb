-- SereneDB Demo: Elastic-grade FTS, plain SQL, against remote IMDb reviews.
--
-- Indexes one shard of the IMDb review dataset (~100k reviews) directly
-- from Hugging Face. No download, no schema mapping, no client library.
-- After the index builds, the rest of the file walks the FTS surface:
-- phrase with slop, boolean composition, prefix / wildcard / regex / fuzzy,
-- ngram (typo-tolerant), four scoring models side-by-side, inline match
-- highlights via byte offsets, and a composite query that wires several
-- of these features together at once.
--
-- Run:
--   psql -h <host> -p <port> -U postgres -d postgres -f demo.sql
--
-- text is exposed twice in the view body -- once as `text` (indexed by
-- the english analyzer) and once as `text_ngram` (indexed by the ngram
-- analyzer). Both read from the same parquet column; the duplication is
-- virtual and lets one index pass cover both analyzers.

\timing on

DROP INDEX IF EXISTS imdb_fts_idx;
DROP VIEW IF EXISTS imdb_fts;
DROP TEXT SEARCH DICTIONARY IF EXISTS imdb_fts_ngram;
DROP TEXT SEARCH DICTIONARY IF EXISTS imdb_fts_en;

-- Main english tokenizer with offsets enabled (for snippet generation).
CREATE TEXT SEARCH DICTIONARY imdb_fts_en(
    template = 'text',
    locale = 'en_US.UTF-8',
    case = 'lower',
    stemming = false,
    accent = false,
    frequency = true,
    position = true,
    norm = true,
    offset = true
);

-- 3-gram tokenizer for typo-tolerant substring search.
CREATE TEXT SEARCH DICTIONARY imdb_fts_ngram(
    template = 'ngram',
    mingram = 3,
    maxgram = 3,
    preserveoriginal = false,
    frequency = true,
    position = true
);

CREATE VIEW imdb_fts AS
  SELECT text, label, text AS text_ngram
  FROM read_parquet('hf://datasets/stanfordnlp/imdb@~parquet/plain_text/**/*.parquet');

CREATE INDEX imdb_fts_idx ON imdb_fts USING inverted(
  text imdb_fts_en,
  text_ngram imdb_fts_ngram,
  label
);


-- =============================================================================
-- 1. Phrase with slop
-- =============================================================================
-- ts_phrase takes optional gap arguments. ARRAY[0,3] means "0 to 3 tokens"
-- between adjacent terms -- "plot twist", "plot has a twist", "plot was
-- such a twist" all match.
\echo === [1] Phrase with slop: 'plot ... twist' (0..3 tokens between) ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ ts_phrase('plot', ARRAY[0,3], 'twist')
LIMIT 5;


-- =============================================================================
-- 2. Boolean composition, two flavors
-- =============================================================================
-- Native operators: && AND, || OR, !! NOT, ## phrase-sequence, ^ score boost.
-- Composes inside @@ like any other expression.
\echo === [2a] Native operators: ('special' ## 'effects') AND NOT 'cgi' ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ (('special' ## 'effects') && !!ts_phrase('cgi'))
LIMIT 5;

-- Same shape in Lucene-style websearch syntax for users coming from Elastic.
\echo === [2b] Lucene-style: "plot twist" OR "happy ending" -boring ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ to_tsquery('"plot twist" OR "happy ending" -boring')
LIMIT 5;


-- =============================================================================
-- 3. Prefix / wildcard / regex / fuzzy
-- =============================================================================
\echo === [3a] Prefix: ts_starts_with('cinemat') -> cinematic, cinematography, ... ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ ts_starts_with('cinemat') LIMIT 5;

\echo === [3b] Wildcard: ts_like('photo%graphy') ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ ts_like('photo%graphy') LIMIT 5;

\echo === [3c] Regex: ts_regexp('osc[ae]r') ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ ts_regexp('osc[ae]r') LIMIT 5;

\echo === [3d] Fuzzy: ts_levenshtein('tarintino', 2) -- typo finds 'Tarantino' ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text @@ ts_levenshtein('tarintino', 2) LIMIT 5;


-- =============================================================================
-- 4. Ngram (typo-tolerant substring matching)
-- =============================================================================
-- The ngram index breaks each value into 3-character grams; the query is
-- tokenized the same way and matched by Jaccard similarity.
\echo === [4] Ngram: ts_ngram('directur', 0.6) -- substring/typo finds 'director'-like ===
SELECT label, left(text, 100) AS snippet FROM imdb_fts_idx
WHERE text_ngram @@ ts_ngram('directur', 0.6) LIMIT 5;


-- =============================================================================
-- 5. Scoring models side-by-side
-- =============================================================================
-- Same query ranked by four different scoring functions. Swap the function
-- name; everything else stays the same. Elasticsearch ships BM25 only by
-- default; SereneDB has BM25, TFIDF, LM-Dirichlet, and DFI as first-class.
\echo === [5a] BM25 (Elastic default) ===
SELECT round(BM25(imdb_fts_idx.tableoid)::numeric, 2) AS score, label
FROM imdb_fts_idx WHERE text @@ ts_phrase('special effects')
ORDER BY BM25(imdb_fts_idx.tableoid) DESC LIMIT 3;

\echo === [5b] TFIDF (classic) ===
SELECT round(TFIDF(imdb_fts_idx.tableoid)::numeric, 2) AS score, label
FROM imdb_fts_idx WHERE text @@ ts_phrase('special effects')
ORDER BY TFIDF(imdb_fts_idx.tableoid) DESC LIMIT 3;

\echo === [5c] LM-Dirichlet (language model) ===
SELECT round(lm_dirichlet(imdb_fts_idx.tableoid, 5.0)::numeric, 2) AS score, label
FROM imdb_fts_idx WHERE text @@ ts_phrase('special effects')
ORDER BY lm_dirichlet(imdb_fts_idx.tableoid, 5.0) DESC LIMIT 3;

\echo === [5d] DFI (Divergence From Independence) ===
SELECT round(dfi(imdb_fts_idx.tableoid)::numeric, 2) AS score, label
FROM imdb_fts_idx WHERE text @@ ts_phrase('special effects')
ORDER BY dfi(imdb_fts_idx.tableoid) DESC LIMIT 3;


-- =============================================================================
-- 6. Inline highlights / match offsets
-- =============================================================================
-- OFFSETS returns byte ranges of every match in the indexed column, in order.
-- Combine with substring extraction in SQL for snippet rendering.
\echo === [6] OFFSETS: byte ranges of every 'oscar' match in matching reviews ===
SELECT label, OFFSETS(text, 6) AS match_ranges
FROM imdb_fts_idx
WHERE text @@ ts_phrase('oscar')
LIMIT 5;


-- =============================================================================
-- 7. Composite query: regex + fuzzy + ngram + facet, scored
-- =============================================================================
-- One SQL expression that wires several FTS surfaces together:
--   - regex on text          (Oscar / Oscars)
--   - fuzzy on text          (Tarantino, allowing 2-edit typos)
--   - ngram on text_ngram    (director-like, via the ngram analyzer)
--   - filter on label        (positive sentiment, label = 1)
-- Then ranks the survivors by BM25 across all matched terms.
\echo === [7] Composite: regex && fuzzy on text, ngram on text_ngram, label = 1, ranked by BM25 ===
SELECT round(BM25(imdb_fts_idx.tableoid)::numeric, 2) AS score, label,
       left(text, 160) AS snippet
FROM imdb_fts_idx
WHERE text @@ (ts_regexp('osc[ae]r') && ts_levenshtein('tarintino', 2))
  AND text_ngram @@ ts_ngram('directur', 0.6)
  AND label = 1
ORDER BY BM25(imdb_fts_idx.tableoid) DESC LIMIT 5;


-- =============================================================================
-- 8. Complex phrase: ## chains mixed query parts with configurable slop
-- =============================================================================
-- The ## operator stitches a phrase together from heterogeneous parts and
-- per-gap slop windows. Each integer or ARRAY[min,max] between two parts
-- becomes the allowed token gap there. Supported parts: bare 'word',
-- ts_starts_with, ts_like, ts_levenshtein, ts_phrase, ts_any, ts_between.
--
--   ts_levenshtein('tarintino', 2)   -- fuzzy: matches 'tarantino' (and typos)
--   ## ARRAY[1,5]                    -- 1..5 tokens between
--   ts_starts_with('direct')         -- prefix: matches 'direct', 'directs', ...
--   ## ARRAY[0,8]                    -- 0..8 tokens between
--   'film'                           -- bare term
--
-- Reads: "Tarantino-like word, 1..5 tokens later a direct*-word, 0..8
-- tokens later 'film'." Recovers reviews mentioning Tarantino in a
-- writer/director context near the word "film".
\echo === [8] Complex phrase: ts_levenshtein ## [1,5] ## ts_starts_with ## [0,8] ## bare ===
SELECT label, left(text, 200) AS snippet FROM imdb_fts_idx
WHERE text @@ (
        ts_levenshtein('tarintino', 2)
        ## ARRAY[1,5]
        ## ts_starts_with('direct')
        ## ARRAY[0,8]
        ## 'film'
      )
LIMIT 5;


-- Cleanup (uncomment to drop demo objects):
-- DROP INDEX imdb_fts_idx;
-- DROP VIEW imdb_fts;
-- DROP TEXT SEARCH DICTIONARY imdb_fts_ngram;
-- DROP TEXT SEARCH DICTIONARY imdb_fts_en;
