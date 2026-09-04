---
title: Ranking
sidebar_position: 6
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

[Matching](./full-text-search.md) answers a yes/no question — does a row match? **Ranking** answers a different one — *how well* does it match? — and orders the results accordingly. The two are separate steps: a `WHERE col @@ query` filter selects the matching rows, and a scorer in `ORDER BY` sorts them by relevance.

<DocCallout type="tip">

A scorer reads per-term statistics, so the indexed column must have the `frequency` [feature flag](./text-analysis.md#token-positions-and-feature-flags) enabled. Every scorer takes the index's `tableoid` as its first argument and returns a `FLOAT`.

</DocCallout>

## Scoring with BM25

Okapi [`BM25`](../../functions/search/scoring.md) is the standard relevance scorer. Use it in `ORDER BY`, highest score first:

<SqlLogicTest id="sql/indexes/inverted/ranking/example_001" />

## Other scorers

SereneDB ships several scorers; [`TFIDF`](../../functions/search/scoring.md) is the classic alternative:

<SqlLogicTest id="sql/indexes/inverted/ranking/example_002" />

| Scorer | Parameters (defaults) | Notes |
|---|---|---|
| `BM25` | `k1` (1.2), `b` (0.75) | Okapi BM25; `b = 0` disables length normalization (BM15) |
| `TFIDF` | `with_norms` (false) | Classic TF-IDF |
| `lm_jm` | `lambda` (0.1) | Language model, Jelinek-Mercer smoothing |
| `lm_dirichlet` | `mu` (2000) | Language model, Dirichlet smoothing |
| `indri_dirichlet` | `mu` (2000) | Indri-style Dirichlet (no floor clamp) |
| `dfi` | `measure` (`'standardized'`) | Divergence-from-independence; also `'saturated'`, `'chi_squared'` |
| `raw_tf` / `raw_boost` / `raw_dl` | — | Raw term frequency, boost and document length |

## Boosting

The `^` operator multiplies a sub-query's contribution to the score, so you can weight some clauses above others. Here a title match is boosted so it outranks a description-only match:

<SqlLogicTest id="sql/indexes/inverted/ranking/example_004" />

## Top-K queries and WAND pruning

The common shape `ORDER BY <scorer> DESC LIMIT k` returns the best `k` matches. Building the index with the [`optimize_top_k` index option](../../statements/create_index/inverted.md#index-options) enables **WAND** pruning, which skips candidates that provably cannot reach the top `k`:

```sql
CREATE INDEX movies_idx ON movies
    USING inverted (id, description ranking_dict)
    WITH (optimize_top_k = 'bm25(1.2, 0.75)');
```

<SqlLogicTest id="sql/indexes/inverted/ranking/example_003" />

Pruning engages only when all of the following hold; otherwise the query still runs correctly, just without the optimization:

- the query is `ORDER BY <scorer>(idx.tableoid) DESC` with a `LIMIT`;
- the scorer **matches the one named in `optimize_top_k` exactly** (a different scorer falls back to a full scan);
- the filter is a single term or an `OR` of terms (not a phrase, `AND` or `NOT`).

You can confirm pruning is active in the query plan — `EXPLAIN` shows `Top: k, optimized` on the scan. The [`sdb_disable_top_k_optimization` and `sdb_scored_terms_limit` session settings](./maintenance.md#session-settings) tune this at query time.

## Tie-breaking

Scores can tie. Add further `ORDER BY` columns after the scorer for a deterministic order — typically the primary key:

```sql
SELECT id, title
FROM movies_idx
WHERE description @@ 'galaxy'
ORDER BY BM25(movies_idx.tableoid) DESC, id;
```

## Combining ranked queries

To blend a lexical (BM25) ranking with a [vector](./vector-search.md) ranking — or any two ranked result sets — use **Reciprocal Rank Fusion**, which combines *ranks* rather than incomparable scores. See [Hybrid Search](./hybrid-search.md#score-fusion) and the [Reciprocal Rank Fusion](../../../cookbook/search/reciprocal-rank-fusion.md) recipe.

## See also

- [Full-Text Search](./full-text-search.md) — matching and query construction
- [Scoring functions](../../functions/search/scoring.md) — full scorer reference
- [Text Analysis](./text-analysis.md#token-positions-and-feature-flags) — the `frequency` flag scoring requires
- [BM25/TFIDF Ranking](../../../cookbook/search/ranking.md) — parameter tuning
