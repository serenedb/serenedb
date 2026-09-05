---
title: BM25/TFIDF Ranking
sidebar_position: 11
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# BM25/TFIDF Ranking

Relevance scoring ranks search results by how well they match a query. The two most common algorithms are [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) and [TF-IDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf), both based on term frequency and inverse document frequency.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## How ranking works

Both algorithms rely on two statistical measures:

- **Term frequency (TF)** — how often a search term appears in a given document. A document mentioning "galaxy" five times is considered more relevant than one mentioning it once.
- **Inverse document frequency (IDF)** — how rare the term is across all indexed documents. Common words like "the" appear everywhere and carry little signal. A rare term like "paleontologist" is a much stronger match indicator.

### BM25 vs TF-IDF

The key difference is that BM25 adds **document length normalization** and **term frequency saturation** on top of TF-IDF. In practice this means BM25 handles varying document lengths better — a short document with two mentions of a term can rank above a long document with three.

| | TF-IDF | BM25 |
|---|---|---|
| Length normalization | No | Yes (parameter `b`) |
| TF saturation | No — score grows linearly | Yes — diminishing returns (parameter `k1`) |
| Reads norms from index | No | Yes |
| Performance | Faster — fewer index reads | Slightly slower due to norm lookups |
| Best for | Uniform-length documents, latency-sensitive workloads | Mixed-length documents, general-purpose ranking |

Use **BM25** as the default. Use **TF-IDF** when your documents are roughly the same length or when you need lower scoring latency — TF-IDF is faster because it does not need to read document length norms from the index.

## BM25 scoring

Use `BM25(<index>.tableoid)` in the SELECT and ORDER BY clauses to rank results by relevance:

<SqlLogicTest id="cookbook/search/ranking/example_001" />

### Custom parameters

Pass `k1` and `b` to tune the ranking:

<SqlLogicTest id="cookbook/search/ranking/example_002" />

| Parameter | Default | Description |
|---|---|---|
| `k1` | `1.2` | Term frequency saturation. Higher values increase the impact of term frequency |
| `b` | `0.75` | Document length normalization. `0` disables normalization, `1` fully normalizes |

Favor exact matches over frequency by lowering `k1`, and disable length normalization with `b = 0`:

<SqlLogicTest id="cookbook/search/ranking/example_003" />

Increase `k1` to reward documents that mention the term many times:

<SqlLogicTest id="cookbook/search/ranking/example_004" />

### Named variants

Specific combinations of `k1` and `b` produce well-known BM25 variants:

| Variant | Parameters | Behavior |
|---|---|---|
| **BM25** | `BM25(1.2, 0.75)` | Default — balanced saturation and length normalization |
| **BM15** | `BM25(1.2, 0)` | No length normalization (`b=0`). Treats all documents equally regardless of length |
| **BM11** | `BM25(1.2, 1)` | Full length normalization (`b=1`). Strongly penalizes long documents |
| **BM0** | `BM25(0, 0)` | Pure IDF — term frequency is ignored entirely. Only document rarity matters |

<SqlLogicTest id="cookbook/search/ranking/example_005" />

### Combine with filters

<SqlLogicTest id="cookbook/search/ranking/example_006" />

### Combine with analytics

<SqlLogicTest id="cookbook/search/ranking/example_007" />

### Pagination with stable ordering

When paginating, add a tiebreaker column to ensure consistent ordering across pages:

<SqlLogicTest id="cookbook/search/ranking/example_008" />

## TFIDF scoring

Use `TFIDF(<index>.tableoid)` as an alternative scoring function:

<SqlLogicTest id="cookbook/search/ranking/example_009" />

### With normalization

Pass `true` to enable normalization:

<SqlLogicTest id="cookbook/search/ranking/example_010" />

## Custom scoring

Combine relevance scores with other columns for domain-specific ranking:

<SqlLogicTest id="cookbook/search/ranking/example_011" />

## Dictionary requirements

To use scoring functions, your dictionary must have `FREQUENCY = true`:

<SqlLogicTest id="cookbook/search/ranking/example_012" />

The `FREQUENCY` flag stores term frequency data in the index, which BM25 and TF-IDF need for scoring.

## See also

- [Phrase and Proximity Search](./phrase-and-proximity-search.md) — finding phrase matches to rank
- [CREATE TEXT SEARCH DICTIONARY](../../sql/statements/create_text_search_dictionary/index.md) — frequency and position flags
