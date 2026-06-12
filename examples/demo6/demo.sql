-- SereneDB Demo: code search + statistics over self-sufficient indexes.
--
-- Everything below runs against TWO inverted indexes built over UNION ALL
-- views of Hugging Face parquet (see bootstrap.sql) -- there are no tables,
-- and after the build no query touches the parquet again: every retrieved
-- column lives in the index (INCLUDE).
--
-- The `sparse_ngram` tokenizer implements the n-gram selection scheme behind
-- GitHub code search (https://github.com/danlark1/sparse_ngrams): a monotonic
-- stack over bigram hashes picks a sparse subset of variable-length ngrams
-- (>= 3 bytes) such that any query substring is covered by ngrams present in
-- the index. Indexing emits at most 2n-2 grams per document
-- (covering = false); querying needs only a minimal covering chain of at most
-- n-2 grams (covering = true), and the conjunction of those grams -- via
-- ts_all(ts_tokenize(...)) -- selects exactly the documents that can contain
-- the substring. A LIKE post-filter makes the match exact.
--
-- Prereq: bootstrap.sql (builds dictionaries, views and both indexes).

\timing on

-- What the tokenizer does: index side emits all sparse grams of a line of
-- code, query side only the covering chain.
SELECT ts_lexize('code_grams', 'for i in range(n):') AS index_grams;
SELECT ts_lexize('code_grams_q', 'for i in range(n):') AS covering_grams;

-- Q1: grep over the solutions. The covering grams of the query must ALL
-- match (ts_all); the LIKE post-filter guarantees exactness in general.
-- Compare with the postings-free scan at the bottom of the file.
SELECT count(*) AS recursive_solutions
FROM solutions_idx
WHERE code @@ ts_all(ts_tokenize(ARRAY['sys.setrecursionlimit('], 'code_grams_q'))
  AND code LIKE '%sys.setrecursionlimit(%';

-- Q2: code idiom -> tasks. Which problems were solved with memoised
-- recursion? (task metadata comes from the other index)
SELECT DISTINCT t.id, t.title, t.rating
FROM solutions_idx s
JOIN tasks_idx t ON t.id = s.task_id
WHERE s.code @@ ts_all(ts_tokenize(ARRAY['from functools import lru_cache'], 'code_grams_q'))
  AND t.rating >= 1500
ORDER BY t.rating DESC
LIMIT 10;

-- Q3: BM25 over task statements -- classic full-text ranking, across BOTH
-- sources of the union view (Codeforces and the other judges).
SELECT id, contest_name, title, rating,
       round(bm25(tasks_idx.tableoid)::numeric, 2) AS score
FROM tasks_idx
WHERE statement @@ ts_phrase('shortest path')
ORDER BY bm25(tasks_idx.tableoid) DESC
LIMIT 8;

-- Q4: fuzzy code search. Require any 3 of the covering grams of a canonical
-- binary-search loop -- variable names may differ, the shape survives.
SELECT task_id, lang, round(bm25(solutions_idx.tableoid)::numeric, 2) AS score
FROM solutions_idx
WHERE code @@ ts_any(ts_tokenize(ARRAY['while l < r: m = (l + r) // 2'], 'code_grams_q'), 3)
ORDER BY bm25(solutions_idx.tableoid) DESC
LIMIT 5;

-- Q5: STATISTICS over INCLUDE columns. The per-run measurements (time_ms,
-- memory_kb) were carried into the index at build time, so a database-grade
-- aggregation over them is a pure index read. Per-language profile of one
-- problem:
SELECT lang,
       count(*)                                  AS solutions,
       min(time_ms)                              AS best_ms,
       round(approx_quantile(time_ms, 0.5))      AS p50_ms,
       min(memory_kb)                            AS best_kb,
       min(code_len)                             AS shortest
FROM solutions_idx
WHERE task_id = '4/A'
GROUP BY lang
ORDER BY solutions DESC;

-- Q6: leaderboards. Fastest accepted solutions of a problem, ties broken by
-- memory then brevity -- again, index-only.
SELECT id AS submission, lang, time_ms, memory_kb, code_len
FROM solutions_idx
WHERE task_id = '4/A'
ORDER BY time_ms ASC, memory_kb ASC, code_len ASC
LIMIT 5;

-- Q7: corpus-wide statistics: which problems make accepted solutions sweat?
-- (highest median runtime among problems with enough samples)
SELECT s.task_id, t.title, t.rating,
       count(*)                             AS n,
       round(approx_quantile(s.time_ms, 0.5)) AS p50_ms,
       max(s.time_ms)                       AS worst_ms
FROM solutions_idx s
JOIN tasks_idx t ON t.id = s.task_id
GROUP BY s.task_id, t.title, t.rating
HAVING count(*) >= 10
ORDER BY p50_ms DESC
LIMIT 10;

-- Baseline: the same predicate as Q1 without the postings -- a LIKE over the
-- index-stored code column materialises every row. The gap versus Q1 grows
-- linearly with corpus size; the ts_all path stays flat.
SELECT count(*) AS recursive_solutions_no_postings
FROM solutions_idx
WHERE code LIKE '%sys.setrecursionlimit(%';
