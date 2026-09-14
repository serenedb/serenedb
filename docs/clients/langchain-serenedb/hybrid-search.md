---
title: Hybrid Search
sidebar_position: 4
split: headings
---

# Hybrid Search

Hybrid retrieval combines a **lexical** ranking (full-text `@@` matching on the content column, scored with `BM25`) and a [**dense**](./vector-store.md#dense) ranking (vector ANN distance) into a single relevance order. Because one SereneDB [inverted index](../../sql/indexes/inverted/index.md) can cover both a text column and a vector column, the integration does this in **one** query rather than two round trips — see [Hybrid Search](../../sql/indexes/inverted/hybrid-search.md) for the SQL-level picture and [Reciprocal Rank Fusion](../../cookbook/search/reciprocal-rank-fusion.md) for the fusion maths.

Two separate concerns are configured with two separate classes, so it is clear which belongs where:

| Class | Concern | Where it goes |
| :--- | :--- | :--- |
| [`HybridIndexConfig`](#hybridindexconfig) | **Build time** — the text search dictionary the content column is analyzed with | [`init_vectorstore_table()`](./engine.md#init_vectorstore_table), or [`apply_hybrid_search_index()`](./indexes.md#apply_hybrid_search_index) |
| [`HybridSearchConfig`](#hybridsearchconfig) | **Query time** — fusion strategy, weights, per-branch windows, scorer, tsquery function | The store factory |

They are independent: you can rebuild the index without touching how queries are fused, and change fusion without rebuilding. Neither needs to be passed twice, and the types make it impossible to hand one to the wrong side.

## Setting it up {#setup}

```python
from langchain_serenedb import (
    FusionStrategy,
    HybridIndexConfig,
    HybridSearchConfig,
    SereneDBEngine,
    SereneDBVectorStore,
)

engine = SereneDBEngine.from_connection_string(
    "host=127.0.0.1 port=7890 user=postgres dbname=postgres"
)

# Build time: creates the text search dictionary and the combined
# content + embedding index alongside the table.
engine.init_vectorstore_table("my_docs", 768, hybrid_index_config=HybridIndexConfig())

# Query time: how each search fuses the two branches.
store = SereneDBVectorStore.create_sync(
    engine,
    embeddings,
    "my_docs",
    hybrid_search_config=HybridSearchConfig(
        fusion=FusionStrategy.RRF, primary_top_k=20, secondary_top_k=20
    ),
)

store.add_texts(docs, metadatas=metas)

results = store.similarity_search("how do I configure the index?", k=5)
```

Both defaults are usable as-is, so `HybridIndexConfig()` and `HybridSearchConfig()` with no arguments are a valid starting point.

If the table already exists, build the combined index from the store instead:

```python
store.apply_hybrid_search_index()                                     # default dictionary
store.apply_hybrid_search_index(index_config=HybridIndexConfig(...))  # or a custom one
```

:::caution
**A hybrid query has no fallback.** Both branches select from the combined index *by name*, because `BM25` needs the index's `tableoid` and `@@` only resolves against an indexed column. If the combined index does not exist, the query fails — unlike a dense search, which quietly falls back to an exact table scan.

So the one thing still worth checking is that the two sides agree in intent: a store carrying a `HybridSearchConfig` needs a combined index to have been built for its table. Giving the store a search config does not create one.
:::

## `HybridIndexConfig` {#hybridindexconfig}

The build-time half: which text search dictionary the content column is analyzed with when the combined index is created. It is not consulted at query time — the `@@` predicate resolves through the index's own analyzer.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `dictionary_name` | `str` | `"langchain_fts_dict"` | Name of the dictionary created for the content column. |
| `dictionary_options` | `str` | `"template = 'segmentation', case = 'lower', frequency = true, position = true, norm = true"` | Options for `CREATE TEXT SEARCH DICTIONARY`. See [The text search dictionary](#dictionary). |

Pass it to [`init_vectorstore_table()`](./engine.md#init_vectorstore_table) as `hybrid_index_config=`, or to [`apply_hybrid_search_index()`](./indexes.md#apply_hybrid_search_index) as `index_config=`. Both default to `HybridIndexConfig()` when a combined index is requested without one.

## `HybridSearchConfig` {#hybridsearchconfig}

The query-time half: how each search windows, scores and fuses the two branches. Every field has a default, so `HybridSearchConfig()` is valid. It carries no index or dictionary settings, and the full-text query is not a field either — that is per call, see [Choosing the query text](#fts-query).

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `fusion` | `FusionStrategy` | `RRF` | How the two rankings are combined. See [`FusionStrategy`](#fusionstrategy). |
| `rrf_k` | `int` | `60` | The RRF `k` constant. Higher values flatten the advantage of top ranks. `RRF` only. |
| `primary_results_weight` | `float` | `0.5` | Weight of the **vector** branch. `NORMALIZED` and `WEIGHTED_SUM` only. |
| `secondary_results_weight` | `float` | `0.5` | Weight of the **lexical** branch. `NORMALIZED` and `WEIGHTED_SUM` only. |
| `primary_top_k` | `int` | `4` | Per-branch window: rows the vector branch contributes to fusion. |
| `secondary_top_k` | `int` | `4` | Per-branch window: rows the lexical branch contributes to fusion. |
| `scorer` | `str` | `"BM25"` | Relevance scorer. Any [SereneDB scorer](../../sql/functions/search/scoring.md) name — `BM25`, `TFIDF`, `dfi`, … |
| `tsquery_function` | `str` | `"plainto_tsquery"` | Query constructor: `plainto_tsquery`, `to_tsquery`, `phraseto_tsquery` or `websearch_to_tsquery`. |

:::note
The default per-branch windows are only **4** rows each, and a document has to reach the top 4 of one branch to enter fusion at all. The two windows therefore contribute at most 8 distinct documents, so a request for `k=10` against the defaults can never return more than 8 results. Raise `primary_top_k` and `secondary_top_k` to at least a few times your `k`.
:::

`scorer` and `tsquery_function` are interpolated into the SQL as identifiers, not bound as parameters. Pass function names, not user input.

## `FusionStrategy` {#fusionstrategy}

A `str` enum, so the raw strings `"rrf"`, `"normalized"` and `"weighted_sum"` are accepted wherever a member is.

| Member | Combines | Uses | Choose it when |
| :--- | :--- | :--- | :--- |
| `RRF` | Ranks: `sum(1 / (rrf_k + rank))` over both branches | `rrf_k` | Consensus should win. Scale-free, so BM25 scores and vector distances fuse with no tuning — a document both signals like beats one only a single signal likes. This is the default and the usual right answer. |
| `NORMALIZED` | Min-max normalizes each branch to `[0, 1]` (the vector branch inverted so nearer is higher), then takes a weighted sum | both weights | Score margins matter — a decisive win in one branch should outrank lukewarm presence in both. The cost is that one outlier rescales its whole branch. |
| `WEIGHTED_SUM` | A weighted sum of the **raw** branch scores | both weights | The branches are already on comparable scales. BM25 magnitudes and raw vector distances usually are not, so reach for this only when you have measured your own. |

## Query shape {#query-shape}

The generated SQL follows SereneDB's standard fusion patterns, see [Reciprocal Rank Fusion](../../cookbook/search/reciprocal-rank-fusion.md) — [How it works](../../cookbook/search/reciprocal-rank-fusion.md#how-it-works) for the formula, [Template](../../cookbook/search/reciprocal-rank-fusion.md#template) for the `WITH fused AS (...)` skeleton the store emits, and [Another RRF strategy: normalized scores](../../cookbook/search/reciprocal-rank-fusion.md#another-rrf-strategy-normalized-scores) for what `NORMALIZED` does.

The store fills that skeleton in as follows:

| In the pattern | Here |
| :--- | :--- |
| Branch 1 | Lexical: `content @@ tsquery_function(...)`, ranked by `scorer` |
| Branch 2 | Vector: the [distance strategy](./indexes.md#distancestrategy)'s operator against the query embedding |
| Per-branch [`LIMIT`](../../cookbook/search/reciprocal-rank-fusion.md#window-size-per-branch-limit) | `secondary_top_k` for the lexical branch, `primary_top_k` for the vector one |
| RRF [`k`](../../cookbook/search/reciprocal-rank-fusion.md#k-top-rank-weight) | `rrf_k` |
| Final `LIMIT` | The search's `k` |

Both branches read from the combined index *by name*, and the outer query joins the base table back to project the content and metadata columns.

Two details the pattern does not cover: a metadata [filter](./filtering.md) is applied inside **both** branches, so it narrows the candidate set before fusion rather than trimming the result afterwards; and ties in the fused score are broken by id, which makes result order deterministic.

## Choosing the query text {#fts-query}

The lexical branch needs a query string, and it is **per call** — the config holds no default for it. Resolution is simple:

1. An explicit `fts_query=` keyword argument on the search call, if given.
2. Otherwise, for [`similarity_search()`](./vector-store.md#similarity_search), the query text itself, filled in automatically.

If neither applies, no lexical branch runs and the search is a plain dense query. That is what happens with [`similarity_search_by_vector()`](./vector-store.md#similarity_search_by_vector) and the MMR methods, which have no query text to borrow — pass `fts_query` yourself if you want fusion:

```python
store.similarity_search_by_vector(embedding, k=5, fts_query="index configuration")
```

:::note
The [scored methods](#scores) are dense-only by design and **reject** `fts_query`, so it is available on `similarity_search()` and `similarity_search_by_vector()` but not on `similarity_search_with_score()`.
:::

A different config can also be supplied per call, which is handy for A/B testing fusion strategies against one store:

```python
store.similarity_search(
    "index configuration",
    k=5,
    hybrid_search_config=HybridSearchConfig(fusion=FusionStrategy.NORMALIZED),
)
```

## The text search dictionary {#dictionary}

The lexical branch scores with `BM25`, which needs term frequencies recorded in the index. That comes from the content column's [text search dictionary](../../sql/statements/create_text_search_dictionary/index.md), created for you from the [`HybridIndexConfig`](#hybridindexconfig) in the table's schema — it has to live there, because an index in a non-`public` schema cannot resolve a dictionary from elsewhere.

Because this is settled when the index is built, changing it later means rebuilding: drop the dictionary, then re-create the index with the new config.

If you customize `dictionary_options`, keep these:

| Option | Needed for |
| :--- | :--- |
| `frequency = true` | Any relevance scoring at all |
| `position = true` | Phrase and proximity queries |
| `norm = true` | The language-model scorers |

See [Text Analysis](../../sql/indexes/inverted/text-analysis.md) for the available templates and options, and [Scoring](../../sql/functions/search/scoring.md) for the scorers.

:::note
[`drop_table()`](./engine.md#drop_table) removes the table and its index but leaves the dictionary behind. It is created with `IF NOT EXISTS`, so recreating the table reuses it — including its old options. Drop it by hand if you change `dictionary_options`.
:::

## Rankings, not scores {#scores}

Hybrid fusion produces a **ranking**, not a distance. That is why the scored methods are dense-only: [`similarity_search_with_score()`](./vector-store.md#similarity_search_with_score) and `similarity_search_with_score_by_vector()` always return a vector distance, never a fused score, so anything that interprets that number — the relevance helpers, the `similarity_score_threshold` retriever — stays well-defined.

Hybrid ranking is available on the unscored path, [`similarity_search()`](./vector-store.md#similarity_search), and through `as_retriever()` with the default `"similarity"` search type. There is no API that hands you a fused score.

The fused value is neither a distance nor a BM25 score — it is something the fusion step constructs, and what it is made of depends on the strategy:

| `fusion` | The number is | Range | BM25 magnitude survives? |
| :--- | :--- | :--- | :--- |
| `RRF` | `SUM(1 / (rrf_k + rank))` over the branches a document appears in | `0` to `2 / (rrf_k + 1)` — about `0.033` at the default `rrf_k = 60` | No. Each branch's scores are used only to compute `RANK()`, then discarded |
| `NORMALIZED` | Each branch's score min-max rescaled to `[0, 1]` within its own window (the vector branch inverted), times that branch's weight, summed | `0` to `primary_results_weight + secondary_results_weight` — `1.0` by default | No. Only a document's relative position inside the returned window |
| `WEIGHTED_SUM` | `secondary_results_weight * bm25 + primary_results_weight * (-distance)` | Unbounded, and can be negative | Yes — this is the only strategy where raw BM25 reaches the output |

Under `RRF`, then, the number says nothing about how close anything actually was — it is a function of ranks, `rrf_k` and how many branches matched, so two result sets with very different similarity can fuse to identical values. It orders results correctly and means nothing on its own scale. That is exactly why it is not exposed as a score, and why feeding it to a distance-based relevance function would be wrong in two ways at once: the polarity is backwards, and the input was never a distance.

### Asking for a scored hybrid search {#scored-hybrid}

Because the two are incompatible, requesting fusion on a scored path is refused rather than silently approximated. Passing `fts_query` or `hybrid_search_config` to `similarity_search_with_score()` or `similarity_search_with_score_by_vector()` raises `NotImplementedError`:

```text
Scored search does not support hybrid fusion: fused scores are rankings, not
distances. Use similarity_search()/as_retriever() for hybrid ranking, or drop
fts_query/hybrid_search_config for distance scores.
```

A store-level `hybrid_search_config` is not an error on these methods — it is simply bypassed, and you get a dense query with real distances. So on a hybrid store:

| Call | Path | Second tuple element |
| :--- | :--- | :--- |
| `similarity_search(query)` | Hybrid | — |
| `similarity_search_by_vector(emb, fts_query="…")` | Hybrid | — |
| `similarity_search_with_score(query)` | Dense | Vector distance |
| `similarity_search_with_score(query, fts_query="…")` | Raises | — |
| `max_marginal_relevance_search(query)` | Dense | — |

The consequence to keep in mind is that a scored call on a hybrid store answers a different question than an unscored one: it ranks by vector distance alone. If you need the fused ordering, use the unscored method.

Separately, [`max_marginal_relevance_search()`](./vector-store.md#max_marginal_relevance_search) always runs the dense path and ignores the lexical branch, so its scores really are distances — but its results are not hybrid.

## See also

- [Vector Store](./vector-store.md) — the search methods themselves
- [Indexes and Tuning](./indexes.md#apply_hybrid_search_index) — building the combined index
- [Hybrid Search](../../sql/indexes/inverted/hybrid-search.md) · [Ranking](../../sql/indexes/inverted/ranking.md) · [Full-Text Search](../../sql/indexes/inverted/full-text-search.md)
- [FAQ](./faq.md)
