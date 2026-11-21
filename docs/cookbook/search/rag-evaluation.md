---
title: "RAG Evaluation: Measure Retrieval Quality in SQL"
sidebar_label: RAG Evaluation
sidebar_position: 16.5
description: Score BM25, vector and hybrid retrieval against labelled queries with recall@k, MRR and nDCG in SQL, and check your vector index's recall against exact search.
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# RAG Evaluation: Measure Retrieval Quality in SQL

RAG evaluation has two layers: did retrieval return the right chunks, and did the model write a good answer from them. This recipe measures the retrieval layer. It scores BM25, vector and hybrid runs over one SereneDB index against a set of labelled queries with recall@k, precision@k, hit rate, MRR and nDCG, all in SQL, and then checks how many of the exact nearest neighbours the vector index returns.

A RAG pipeline can fail without an error: the retriever returns plausible but wrong chunks, and the model answers fluently from them. Retrieval metrics catch that failure before anyone reads an answer.

<details>
<summary>Schema and sample data</summary>

Nine support-article chunks, each with a `body` and a three-dimensional `emb` embedding written by hand so the distances stay readable. In production `emb` holds the vector your embedding model returns, usually hundreds of dimensions wide. One [inverted index](../../sql/indexes/inverted/index.md) holds the text and an `hnsw` vector index side by side. `queries` holds four test queries with their embeddings, `qrels` holds the labels, and `runs` collects what each retriever returned.

<SqlLogicTest id="cookbook/search/rag-evaluation/setup" />

</details>

## Retrieval first, answers second

Evaluate RAG retrieval on its own before you judge any generated answer. A retrieval score needs no LLM, costs one SQL query and gives the same number every time you run it on the same data. It also sets a ceiling: a chunk that is not in the top k never reaches the prompt, so no prompt or model change recovers an answer that depends on it.

Once retrieval scores well, measure generation on top of it (see [What retrieval metrics don't tell you](#what-retrieval-metrics-dont-tell-you)).

## Build a labelled query set

A RAG evaluation dataset is two tables: the test queries and their relevance labels, often called a golden dataset, ground truth or qrels.

- `queries(qid, text, emb)` holds each query's text and the embedding your application computes for it.
- `qrels(qid, doc_id, rel)` lists the chunks that answer each query, with a graded label: `2` for a chunk that answers the query, `1` for a chunk that helps, and no row (read as `0`) for everything else.

Where labels come from:

- Take queries from your search or chat logs, so the set reflects what users ask, not what is easy to answer.
- Label the chunks a person would want in the prompt. Clicks and thumbs-up from logs are a starting point; an LLM can draft labels, and a person reviews them.
- Pool the labels: judge the top k of every retriever you compare, not only one. Labels drawn from a single retriever favour that retriever.
- Cover each kind of query your users send (keywords and identifiers, natural-language questions, paraphrases), and report the number of queries next to every metric.

The sample set has four queries, one of each kind that separates retrievers: two keyword questions, a paraphrase that shares no words with its answers (`slow dashboard`), and an error code (`error E1042`).

## Record one run per retriever

A run is the ranked list one retriever returns for each query, stored as `runs(system, qid, doc_id, rank)`. Run each retriever through the same statement your application uses, as a [prepared statement](../../sql/query_syntax/prepared_statements.md) executed once per query. Break ties on `id` so the ranks are the same every time.

### BM25 run

`bm25_run` searches `body` with the query text and keeps the top 10 by [BM25](../../sql/functions/search/scoring.md#bm25). A bare string after `@@` matches chunks that contain any of its words.

<SqlLogicTest id="cookbook/search/rag-evaluation/bm25_run" />

### Vector run

`vector_run` takes the query embedding as a `FLOAT[3]` parameter and returns the 10 nearest chunks by `<->` (L2 distance) through the `hnsw` index. See [Vector Search](../../sql/indexes/inverted/vector-search.md).

<SqlLogicTest id="cookbook/search/rag-evaluation/vector_run" />

### Hybrid run

The hybrid run fuses the two runs already stored with [Reciprocal Rank Fusion](./reciprocal-rank-fusion.md): each chunk scores the sum of `1 / (60 + rank)` over the runs that returned it. Fusing stored runs scores exactly what the two retrievers returned; the window is their `LIMIT 10`.

<SqlLogicTest id="cookbook/search/rag-evaluation/hybrid_run" />

The top 3 of each run:

<SqlLogicTest id="cookbook/search/rag-evaluation/top_3" />

BM25 returns nothing for `slow dashboard`: the answers say "load" and "dashboards", not "slow" or "dashboard". The vector run misses `error E1042`: an embedding carries the meaning of a sentence, not the identity of an error code. The hybrid run returns a relevant chunk first for every query.

## Recall@k, MRR and nDCG in one query

Each metric compares the top k of a run with the labels; here k is 3:

| Metric | Per query | What it tells you |
|---|---|---|
| recall@k | relevant chunks in the top k ÷ all relevant chunks for the query | Did the prompt get everything it needs? |
| precision@k | relevant chunks in the top k ÷ k | How much of the prompt is noise? |
| hit rate@k | 1 if any relevant chunk is in the top k, else 0 | Did the prompt get anything useful? |
| MRR (mean reciprocal rank) | 1 ÷ rank of the first relevant chunk, 0 if none is in the top k | How far down is the first useful chunk? |
| nDCG@k | DCG ÷ ideal DCG, where DCG = Σ (2^rel − 1) ÷ log2(rank + 1) | Are the best chunks at the top? Uses the graded labels |

Each metric is averaged over the queries. The view `eval_at_3` computes them per system and query. It starts from every system and every labelled query and `LEFT JOIN`s the runs, so a query a retriever returned nothing for scores 0 instead of dropping out of the average. The ideal DCG comes from the labels themselves, sorted best first.

<SqlLogicTest id="cookbook/search/rag-evaluation/eval_view" />

Averaged per system:

<SqlLogicTest id="cookbook/search/rag-evaluation/metrics" />

Hybrid retrieval reaches recall@3 of 1.000 and nDCG@3 of 0.991 on these four queries; BM25 and vector each miss one query completely. Precision@3 stays below 1 for every system because no query has more than two relevant chunks, so at least one of the three slots holds a non-relevant chunk; 0.583 is the highest precision@3 these labels allow.

## Compare BM25, vector and hybrid on your own queries

Averages hide which queries a change helps and which it hurts. Put the per-query nDCG of each system side by side:

<SqlLogicTest id="cookbook/search/rag-evaluation/per_query" />

Each retriever wins where the others lose: BM25 on the error code, the vector run on the paraphrase. Hybrid wins both, and loses a little on `reset password`, where the BM25 branch lifts "Password rules" (a keyword match that does not answer the query) to rank 2.

Every variant you want to compare is one more run under its own `system` name: BM25 with other `k1` and `b` values, another scorer from [Scoring](../../sql/functions/search/scoring.md) such as `lm_dirichlet` or `dfi`, a different RRF `k` or window, or a reranker. Prepare it, execute it for every query, and the view scores it with the others:

<SqlLogicTest id="cookbook/search/rag-evaluation/scorer_variant" />

## Check the vector index's recall against exact search

The relevance metrics above say whether the right chunks come back. A second number says whether the vector index returns the true nearest neighbours: its recall against exact search. Measure it with [`sdb_ann_force_exact`](../../sql/indexes/inverted/maintenance.md#session-settings), which answers a nearest-k query by scoring every row, and compare its top k with what the index returns under the settings you run in production.

This example uses 20,000 generated 64-dimension vectors, 20 query vectors, and the `hnsw` index with its default quantization. It runs in `psql`: `\gexec` executes each row of a query's result as a statement, so one line runs the prepared statement once per query vector.

```sql
CREATE TABLE vecs (id INTEGER PRIMARY KEY, emb FLOAT[64]);

INSERT INTO vecs
SELECT i, list_transform(range(64), lambda j: (hash(i * 64 + j) % 2000) / 1000.0 - 1.0)::FLOAT[64]
FROM range(20000) t(i);

CREATE INDEX vecs_idx ON vecs USING inverted (id, emb hnsw (metric = 'l2'));

CREATE TABLE ann_queries AS
SELECT q AS qid, list_transform(range(64), lambda j: (hash(1000000 + q * 64 + j) % 2000) / 1000.0 - 1.0)::FLOAT[64] AS emb
FROM range(1, 21) t(q);

CREATE TABLE ann_runs (setting VARCHAR, qid INTEGER, id INTEGER);

PREPARE knn_top10(VARCHAR, INTEGER, FLOAT[64]) AS
INSERT INTO ann_runs
SELECT $1, $2, id
FROM vecs_idx
ORDER BY emb <-> $3
LIMIT 10;
```

Record the exact top 10 first, then the index's answer under each setting you want to compare:

```sql
SET sdb_ann_force_exact = true;
SELECT format('EXECUTE knn_top10(%L, %s, %L)', 'exact', qid, emb) FROM ann_queries \gexec
RESET sdb_ann_force_exact;

SELECT format('EXECUTE knn_top10(%L, %s, %L)', 'default', qid, emb) FROM ann_queries \gexec

SET sdb_hnsw_ef_search = 10;
SELECT format('EXECUTE knn_top10(%L, %s, %L)', 'ef_search = 10', qid, emb) FROM ann_queries \gexec
SET sdb_hnsw_ef_search = 20;
SELECT format('EXECUTE knn_top10(%L, %s, %L)', 'ef_search = 20', qid, emb) FROM ann_queries \gexec
RESET sdb_hnsw_ef_search;

SET sdb_ann_rescore_width = 0;
SELECT format('EXECUTE knn_top10(%L, %s, %L)', 'rescore_width = 0', qid, emb) FROM ann_queries \gexec
RESET sdb_ann_rescore_width;
```

Recall@10 is the share of the exact top 10 that each setting returned:

```sql
SELECT a.setting,
       COUNT(DISTINCT a.qid) AS queries,
       (COUNT(e.id)::DOUBLE / COUNT(*))::DECIMAL(4,3) AS recall_at_10
FROM ann_runs a
LEFT JOIN ann_runs e ON e.setting = 'exact' AND e.qid = a.qid AND e.id = a.id
WHERE a.setting <> 'exact'
GROUP BY a.setting
ORDER BY recall_at_10, a.setting;
```

On this data the result reads:

| setting | queries | recall_at_10 |
|---|---|---|
| ef_search = 10 | 20 | 0.965 |
| ef_search = 20 | 20 | 0.980 |
| rescore_width = 0 | 20 | 0.985 |
| default | 20 | 1.000 |

Your numbers depend on your vectors, so measure on a sample of your own embeddings and the queries your application sends. The knobs to sweep, all documented under [session settings](../../sql/indexes/inverted/maintenance.md#session-settings):

- `sdb_hnsw_ef_search`: the HNSW search beam;
- `sdb_ivf_search_nprobe`: the IVF clusters scanned per query;
- `sdb_ann_rescore_width`: how many candidates of a quantized index are re-scored at full precision (see [Re-scoring](../../sql/indexes/inverted/vector-search.md#rescoring));
- the index's `quant` option: build a second index with another quantizer (see [Quantization](../../sql/indexes/inverted/vector-search.md#quantization)) and run the same comparison.

Index recall and relevance are different numbers. An index with recall@10 of 1.000 returns exactly the nearest vectors, and those can still be the wrong chunks if the embedding model places them close to the query. Track both: index recall against exact search, and recall@k and nDCG against your labels.

## What retrieval metrics don't tell you {#what-retrieval-metrics-dont-tell-you}

Retrieval metrics stop at the prompt. Whether the answer is faithful to the retrieved chunks, relevant to the question and grounded in its sources needs a judge: an LLM scoring each answer, or people. RAG evaluation frameworks provide those judges and their metrics:

- [Ragas](https://docs.ragas.io/en/stable/concepts/metrics/available_metrics/): faithfulness, response relevancy, context precision and context recall;
- [DeepEval](https://deepeval.com/docs/metrics-faithfulness): faithfulness, answer relevancy and contextual precision and recall;
- [MLflow GenAI evaluation](https://mlflow.org/docs/latest/genai/eval-monitor/): LLM-as-a-judge scorers for answers and agents;
- [TruLens](https://www.trulens.org/getting_started/core_concepts/rag_triad/): the RAG triad of context relevance, groundedness and answer relevance;
- [Arize Phoenix](https://arize.com/docs/phoenix): faithfulness, hallucination and retrieval relevance evaluators.

They score an answer from the question and the retrieved contexts, so export those from the run you chose with [`COPY`](../../sql/statements/copy/index.md), one JSON line per query. The field names below are the ones Ragas uses (`user_input`, `retrieved_contexts`); rename the columns to match your framework:

<SqlLogicTest id="cookbook/search/rag-evaluation/export" />

## Pitfalls

- **Zero-hit queries.** Average over every labelled query. Starting from the runs instead of the labels drops the queries a retriever returned nothing for, and inflates its score.
- **Ties.** Break every ranking tie on a stable column such as `id`, or two runs of the same retriever give different ranks and different metrics.
- **Labels from one retriever.** Pool the top k of every system before you label, or the system that produced the candidates wins by construction.
- **Too few queries.** Four queries show the method; a decision needs enough queries of each kind your users send for a one-query change not to move the average.
- **Embedding model mismatch.** Embed the queries with the same model and version as the chunks. A query embedded by another model lands in another space.
- **Stale runs.** After you change a dictionary, an index option or the embedding model, re-run every system before you compare.
- **LLM judges.** A judge model has its own biases: it can favour longer answers or answers from its own model family. Check a sample of its labels by hand.
- **Offline is not online.** Labelled queries measure what you anticipated. Search logs, clicks and follow-up questions show what users actually ask; feed the queries that fail back into `queries` and `qrels`.

## See also

- [BM25/TFIDF Ranking](./ranking.md): the scoring functions behind the BM25 run.
- [Reciprocal Rank Fusion](./reciprocal-rank-fusion.md): `k`, window size and the normalized-score alternative.
- [Semantic and Hybrid Search](./hybrid-search.md): vector and hybrid retrieval step by step.
- [Vector Search](../../sql/indexes/inverted/vector-search.md): `hnsw` and `ivf` indexes, quantization and re-scoring.
- [Maintenance & Introspection](../../sql/indexes/inverted/maintenance.md#session-settings): the session settings that tune vector recall.
- [Window Functions](../../sql/functions/window_functions/index.md): `ROW_NUMBER()` and the other ranking functions used here.
- [Vector Database for RAG](https://serenedb.com/use-cases/rag-database): hybrid retrieval with access filters for a RAG pipeline.
