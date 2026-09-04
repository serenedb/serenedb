---
title: Reciprocal Rank Fusion
sidebar_position: 15
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Reciprocal Rank Fusion

Reciprocal Rank Fusion (RRF) combines two or more ranked result lists into one. Reach for it when no single signal — BM25 over one field, BM25 over another, fuzzy match, vector distance — captures every relevant document, and each surfaces some that the others miss.

See [Setup](./index.md#setup) for the shared dataset used in the examples. The [normalized-scores comparison](#when-the-two-strategies-disagree) at the end brings its own small corpus.

## How it works

For each branch, every matching document gets a **rank** (1, 2, 3, ...) under that branch's own scoring. RRF combines those ranks per document with:

```
rrf_score(d) = Σ over branches  1 / (k + rank_in_branch(d))
```

A document missing from a branch contributes nothing for that branch. Documents that rank high in *any* branch end up with a high combined score; documents that rank high in *several* branches dominate.

`k` controls how steeply top ranks outweigh lower ranks. The default in the original paper and in Elasticsearch is `60`.

## Template

Copy the skeleton and replace each branch with your own ranking query:

```sql
WITH fused AS (
  -- Branch 1
  SELECT id, RANK() OVER (ORDER BY s DESC) AS rank FROM (
    SELECT id, BM25(movies_idx.tableoid) AS s FROM movies_idx
    WHERE title @@ ts_phrase('YOUR_QUERY')
    ORDER BY s DESC LIMIT 100
  ) t
  UNION ALL
  -- Branch 2
  SELECT id, RANK() OVER (ORDER BY s DESC) AS rank FROM (
    SELECT id, BM25(movies_idx.tableoid) AS s FROM movies_idx
    WHERE description @@ ts_phrase('YOUR_QUERY')
    ORDER BY s DESC LIMIT 100
  ) t
)
SELECT id, SUM(1.0 / (60 + rank)) AS rrf_score
FROM fused
GROUP BY id
ORDER BY rrf_score DESC
LIMIT 10;
```

Each branch:

- selects `(id, score)` rows that match whatever predicate you want,
- sorts by its own score, capped with a per-branch `LIMIT` (the **window size** — see [Tuning](#tuning)),
- assigns ranks with `RANK()`.

The outer query sums `1 / (60 + rank)` per id and returns the top fused results.

## Worked example

Same query word, two fields. "Alien" appears in the title of one film and only in the description of another:

<SqlLogicTest id="cookbook/search/reciprocal-rank-fusion/example_001" />

Each branch alone returns one document. Fused, both surface — and a document matching *both* fields would score about twice as high.

## Tuning

### `k` — top-rank weight

`k = 60` is the published default and works well out of the box. Lower `k` widens the gap between top ranks; higher `k` flattens the curve so that the *set* of candidates matters more than the order within each branch.

| `k` | `1/(k+1)` | `1/(k+10)` | Top-vs-10 ratio |
|---|---|---|---|
| `10` | 0.0909 | 0.0500 | 1.8× |
| `60` | 0.0164 | 0.0143 | 1.15× |
| `200` | 0.00498 | 0.00476 | 1.05× |

### Window size — per-branch `LIMIT`

Each branch's `LIMIT N` is the **window**: only the top `N` results per branch contribute. A document outside every branch's window scores 0.

- Navigational ("I know what I want") queries: `LIMIT 50–100`.
- Exploratory queries where the long tail matters: `LIMIT 200+`, at the cost of more rows flowing into the `GROUP BY`.

## More branches

Add another `UNION ALL` block per extra signal — the shape doesn't change:

```sql
WITH fused AS (
  -- branch 1: title BM25
  SELECT id, RANK() OVER (ORDER BY s DESC) AS rank FROM (...) t
  UNION ALL
  -- branch 2: description BM25
  SELECT id, RANK() OVER (ORDER BY s DESC) AS rank FROM (...) t
  UNION ALL
  -- branch 3: fuzzy, n-gram, or any other ranked source
  SELECT id, RANK() OVER (ORDER BY s DESC) AS rank FROM (...) t
)
SELECT id, SUM(1.0 / (60 + rank)) AS rrf_score
FROM fused GROUP BY id ORDER BY rrf_score DESC LIMIT 10;
```

## Another RRF strategy: normalized scores

`RANK()` deliberately discards how far apart the scores are: whether the top hit beats the runner-up by 10× or by a rounding error, they fuse as ranks 1 and 2 either way. When that magnitude carries real signal, keep it — min–max normalize each branch's scores to `[0, 1]` and sum the normalized values instead of reciprocal ranks:

```sql
WITH hits AS (
  SELECT 1 AS branch, id, s FROM (
    SELECT id, BM25(movies_idx.tableoid) AS s FROM movies_idx
    WHERE title @@ ts_phrase('YOUR_QUERY')
    ORDER BY s DESC LIMIT 100
  ) t
  UNION ALL
  SELECT 2 AS branch, id, s FROM (
    SELECT id, BM25(movies_idx.tableoid) AS s FROM movies_idx
    WHERE description @@ ts_phrase('YOUR_QUERY')
    ORDER BY s DESC LIMIT 100
  ) t
),
normed AS (
  SELECT id,
         CASE WHEN MAX(s) OVER w = MIN(s) OVER w THEN 1.0
              ELSE (s - MIN(s) OVER w) / (MAX(s) OVER w - MIN(s) OVER w)
         END AS ns
  FROM hits
  WINDOW w AS (PARTITION BY branch)
)
SELECT id, SUM(ns) AS fused_score
FROM normed
GROUP BY id
ORDER BY fused_score DESC
LIMIT 10;
```

The branches are unchanged; each row just carries a `branch` tag so the `normed` CTE can rescale scores per branch (`PARTITION BY branch`): the best hit in a branch maps to 1, the worst in the window to 0, and everything in between keeps its relative distance. The `CASE` guards a branch whose scores are all equal, which would otherwise divide by zero. A document that wins one branch by a wide margin keeps that advantage in the fused score — exactly what rank-based fusion erases.

Two caveats: a single outlier score stretches the whole scale and compresses everyone else toward 0, and the formula assumes higher-is-better — for a distance branch (smaller is better), invert it with `(MAX(s) OVER w - s) / (MAX(s) OVER w - MIN(s) OVER w)`.

### When the two strategies disagree

The two strategies don't just produce different numbers — they can put a different document on top. A small corpus of blog articles, searched for **vector search performance** over `title` and `body` with `ts_any` (match *any* of the terms, so partial matches rank lower):

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/reciprocal-rank-fusion/normalized_setup" />

</details>

Rank-based RRF first:

<SqlLogicTest id="cookbook/search/reciprocal-rank-fusion/example_002" />

"Search-First Design" wins — yet it never came close to winning either branch. It finished a *distant* second in both: its title score is 1.8 against the title winner's 4.1, its body score 1.3 against the body winner's 6.6. Ranks erase those margins; all RRF sees is "2nd + 2nd", which beats any single first place.

Now the same two branches fused with normalized scores:

<SqlLogicTest id="cookbook/search/reciprocal-rank-fusion/example_003" />

The documents that actually dominated a branch move to the top: "The Performance Handbook" (the body-branch winner, with a weak title match as a bonus) edges out "Vector Search Performance in Production" (the title-branch winner), and "Search-First Design" drops to third with a fused score of 0.29 — its two second places are now worth what they were actually worth. Neither ordering is universally right: rank fusion rewards showing up in many signals, score fusion rewards decisive wins in one. Pick which one suits your search better.

## Which strategy when

- **Rank-based RRF** — the default. Ranks are indifferent to scale, so BM25, vector distance and fuzzy similarity fuse as they are — no per-branch weights or score calibration, just `k` and the window size from [Tuning](#tuning). Choose it when consensus should win: a document that several signals agree on belongs above a document only one signal likes.
- **Normalized scores** — when the margins carry real signal and a decisive win in one branch should outrank lukewarm presence in several. The trade-off is robustness: one outlier score rescales the whole branch, where ranks wouldn't move.
- **Weighted sum of raw scores** `α·s₁ + β·s₂` — when the branches are already on the same scale (say, BM25 over two similar fields). It keeps magnitudes without the min–max distortion, and the weights give per-branch control that neither strategy above offers.
- **No fusion at all** — when one signal dominates. If BM25 alone gives the right answer, fusing in a weaker signal only dilutes the ranking.
- **A calibrated reranker** — when downstream code needs "this document is 92% relevant". Every fused score on this page is ordinal, good only for sorting; fuse to get a candidate set, then rerank it with a calibrated model.

## See also

- [BM25/TFIDF Ranking](./ranking.md) — the scoring functions you'll most often feed into RRF.
- [Fuzzy Search](./fuzzy-search.md) — a natural second branch alongside exact-form BM25.
