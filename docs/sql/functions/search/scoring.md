---
title: Relevance Scoring
sidebar_label: Scoring
sidebar_position: 4
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Scorer Functions {#scorer-functions}

Scorer functions compute a relevance score for each row matched by a full-text search predicate, measuring how well the row matches the `@@` query in the same index scan. Each takes the index `tableoid` as its first argument and returns a `FLOAT`.

The score is an ordinary value — use it wherever you need one: project it in the `SELECT` list, rank with it in `ORDER BY <scorer> DESC`, filter on it in a `WHERE` comparison or fold it into a larger expression (for example, blending `BM25` with business signals). Ranking with `ORDER BY` is the most common use, but it is not required.

| Function | Description |
| :--- | :--- |
| [`BM25(tableoid[, k1, b])`](#bm25) | Okapi BM25 relevance score — the recommended default. |
| [`TFIDF(tableoid[, with_norms])`](#tfidf) | Classic TF-IDF score. |
| [`lm_jm(tableoid[, lambda])`](#lm_jm) | Language model, Jelinek-Mercer smoothing. |
| [`lm_dirichlet(tableoid[, mu])`](#lm_dirichlet) | Language model, Dirichlet smoothing. |
| [`indri_dirichlet(tableoid[, mu])`](#indri_dirichlet) | Indri-style Dirichlet smoothing. |
| [`dfi(tableoid[, measure])`](#dfi) | Divergence-from-independence score (parameter-free). |
| [`raw_tf(tableoid)`](#raw_tf) | Raw term frequency. |
| [`raw_boost(tableoid)`](#raw_boost) | Raw query boost factor. |
| [`raw_dl(tableoid)`](#raw_dl) | Raw document length. |

## Quick start

Filter with `@@` to select matching rows, then rank them with a scorer in `ORDER BY ... DESC`. Add the primary key as a final sort key so ties resolve deterministically:

```sql
SELECT id, BM25(docs_idx.tableoid) AS score
FROM docs_idx
WHERE body @@ ts_phrase('fox')
ORDER BY score DESC, id;
```

Every scorer follows this shape — swap `BM25` for any function in the table above.

## Requirements {#requirements}

| Requirement | Applies to | What happens without it |
| :--- | :--- | :--- |
| `frequency` feature flag on the column | **all** scorers | Scoring is unavailable on the column. |
| `norm` feature flag on the column | `lm_jm`, `lm_dirichlet`, `indri_dirichlet`, `dfi` | The scorer returns `0` for every row. |

Set both flags on the text search dictionary used by the indexed column:

```sql
CREATE TEXT SEARCH DICTIONARY scored_en (
    template = 'text',
    locale = 'en_US.UTF-8',
    frequency = true,   -- term frequency, needed by every scorer
    position = true,
    norm = true         -- document length norms, needed by lm_* and dfi
);
```

See [token positions and feature flags](../../indexes/inverted/text-analysis.md#token-positions-and-feature-flags) for the full list. `BM25`, `TFIDF`, `raw_tf`, `raw_boost` and `raw_dl` need only `frequency`; the language-model scorers and `dfi` silently score `0` until `norm` is enabled.

:::note One scorer per index per query
A single index scan can apply only one scorer function. Two different scorers over the same index in one `SELECT` raise `Only one scorer function is allowed per inverted index`. To compute several scores for the same rows, combine the per-scorer queries with `UNION`.
:::

## Choosing a scorer

| If you want… | Use |
| :--- | :--- |
| A robust general-purpose default | **`BM25`** — start here; tune `k1` and `b` only if needed |
| A simple classic baseline / minimal tuning | **`TFIDF`** |
| Language-model ranking, short keyword queries | **`lm_dirichlet`** (length-aware smoothing) |
| Language-model ranking, longer / verbose queries | **`lm_jm`** (linear smoothing) |
| Indri / Lemur-compatible scores | **`indri_dirichlet`** |
| Good ranking with **no parameters to tune** | **`dfi`** |
| Raw signals to build your own score | **`raw_tf`**, **`raw_boost`**, **`raw_dl`** |

In practice **BM25 is the right default** for almost all full-text ranking — it models term-frequency *saturation* (extra occurrences of a term add ever less) and document-length normalization, which plain TF-IDF does not. Reach for the language-model scorers (`lm_*`) when you want probabilistic query-likelihood ranking, `dfi` when you can't tune parameters and the `raw_*` features when you compose a custom relevance expression (for example mixing BM25 with business signals).

How they differ, simple to advanced:

- **TF-IDF** rewards a term that is frequent in the document and rare across the collection. It is linear in term frequency, so a few very common terms can dominate.
- **BM25** is TF-IDF with two refinements: term-frequency *saturation* (`k1`) so the 10th occurrence counts far less than the 1st, and *length normalization* (`b`) so long documents are not unfairly favored for containing a term more times.
- **Language models** (`lm_*`) flip the question: instead of weighting terms, they estimate the probability that the document's word distribution would *generate* the query, smoothing each document with the whole-collection distribution so unseen terms do not zero out the score. They tend to track BM25 closely while exposing a single, interpretable smoothing knob.
- **DFI** is *parameter-free*: it scores a term by how far its observed frequency diverges from what statistical independence would predict, so there is nothing to tune at all.

## Scorers

### `BM25(tableoid[, k1, b])` {#bm25}

**Signature.** `BM25(tableoid) -> FLOAT`, `BM25(tableoid, k1, b) -> FLOAT`.
**Captures.** Term-frequency saturation plus document-length normalization — the best all-round relevance signal.

The [Okapi BM25](https://en.wikipedia.org/wiki/Okapi_BM25) relevance score.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `k1` | `FLOAT` | `1.2` | Term-frequency saturation. Higher = extra occurrences keep mattering; lower = they saturate sooner. |
| `b` | `FLOAT` | `0.75` | Document-length normalization, in `[0, 1]`. `b = 0` disables it (a.k.a. BM15); `b = 1` fully normalizes by length. |

**How it works.** A term contributes more when it appears often in a document and rarely across the collection, but each extra occurrence adds ever less (controlled by `k1`), and the contribution is scaled down for long documents (controlled by `b`). This is the standard relevance model behind Lucene, Elasticsearch and OpenSearch — start here and tune only if results need it.

<SqlLogicTest id="sql/functions/full_text_search/bm25" />

Passing `k1` and `b` explicitly tunes the score. With `b = 0` (no length normalization) the two shorter documents are no longer penalized relative to each other and tie, where the default ranked the shorter one higher:

<SqlLogicTest id="sql/functions/full_text_search/bm25_params" />

### `TFIDF(tableoid[, with_norms])` {#tfidf}

**Signature.** `TFIDF(tableoid) -> FLOAT`, `TFIDF(tableoid, with_norms) -> FLOAT`.
**Captures.** Term frequency weighted by inverse document frequency — a simple, classic baseline.

The classic [tf–idf](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) (term-frequency × inverse-document-frequency) score.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `with_norms` | `BOOLEAN` | `false` | Apply document-length normalization. Requires the `norm` flag on the column to have an effect. |

**How it works.** Each matched term contributes `tf × idf`: more occurrences in the row and rarer occurrence across the collection both raise the score. It is cheaper than BM25 but has no term-frequency saturation, so a handful of very frequent terms can dominate — prefer BM25 unless you specifically want this baseline.

<SqlLogicTest id="sql/functions/full_text_search/tfidf" />

With `with_norms = true` the score is divided down for longer documents, which reorders nothing here but compresses the values:

<SqlLogicTest id="sql/functions/full_text_search/tfidf_norms" />

### `lm_jm(tableoid[, lambda])` {#lm_jm}

**Signature.** `lm_jm(tableoid) -> FLOAT`, `lm_jm(tableoid, lambda) -> FLOAT`.
**Captures.** Query-likelihood under a language model with fixed linear smoothing — suited to longer queries.

[Query-likelihood](https://en.wikipedia.org/wiki/Query_likelihood_model) language-model score with Jelinek-Mercer (linear) smoothing.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `lambda` | `FLOAT` | `0.1` | Smoothing weight in `(0, 1]`: each document's term probabilities are mixed with the collection's by `lambda`. Smaller favors precision on short queries; larger suits longer, verbose queries. |

**How it works.** The model estimates the probability that the document would generate the query, mixing the document's own term distribution with the collection-wide distribution by a *fixed* fraction `lambda`. Requires the `norm` flag (returns `0` without it).

<SqlLogicTest id="sql/functions/full_text_search/lm_jm" />

### `lm_dirichlet(tableoid[, mu])` {#lm_dirichlet}

**Signature.** `lm_dirichlet(tableoid) -> FLOAT`, `lm_dirichlet(tableoid, mu) -> FLOAT`.
**Captures.** Query-likelihood with length-adaptive smoothing — usually the best language model for short keyword queries.

[Query-likelihood](https://en.wikipedia.org/wiki/Query_likelihood_model) language-model score with Dirichlet smoothing.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `mu` | `FLOAT` | `2000` | Dirichlet prior. Larger = more smoothing (the collection prior dominates); set it near your average document length. |

**How it works.** Like `lm_jm`, but the smoothing strength adapts to document length via the prior `mu` — short documents are smoothed proportionally more, which generally ranks short keyword queries better. Requires the `norm` flag (returns `0` without it).

<SqlLogicTest id="sql/functions/full_text_search/lm_dirichlet" />

### `indri_dirichlet(tableoid[, mu])` {#indri_dirichlet}

**Signature.** `indri_dirichlet(tableoid) -> FLOAT`, `indri_dirichlet(tableoid, mu) -> FLOAT`.
**Captures.** Dirichlet-smoothed query-likelihood in the log domain, matching the Indri / Lemur search engine.

The [Indri](https://en.wikipedia.org/wiki/Lemur_Project)/Lemur variant of Dirichlet smoothing, without the score-floor clamp.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `mu` | `FLOAT` | `2000` | Dirichlet prior, as in `lm_dirichlet`. |

**How it works.** Same smoothing as `lm_dirichlet` but scores are returned in the log domain (typically negative) and the low-score floor clamp is omitted, so values match those produced by the Indri search engine. Use it when you need Indri-comparable scores. Requires the `norm` flag (returns `0` without it).

<SqlLogicTest id="sql/functions/full_text_search/indri_dirichlet" />

### `dfi(tableoid[, measure])` {#dfi}

**Signature.** `dfi(tableoid) -> FLOAT`, `dfi(tableoid, measure) -> FLOAT`.
**Captures.** How far a term's frequency in a document diverges from statistical independence — with **nothing to tune**.

[Divergence-from-independence](https://scholar.google.com/scholar?q=A+nonparametric+term+weighting+method+for+information+retrieval+based+on+measuring+the+divergence+from+independence) term weighting.

| Parameter | Type | Default | Meaning |
| :--- | :--- | :--- | :--- |
| `measure` | `VARCHAR` | `'standardized'` | Divergence statistic. One of `'standardized'`, `'saturated'`, `'chi_squared'`. |

**How it works.** For each term the model computes the frequency expected under independence, then scores by how far the observed frequency diverges from it — the `measure` selects which divergence statistic to use. It is *parameter-free* (no `k1`, `b`, `lambda` or `mu`), making it a strong choice when you can't or don't want to tune. Requires the `norm` flag (returns `0` without it).

<SqlLogicTest id="sql/functions/full_text_search/dfi" />

The `'saturated'` and `'chi_squared'` measures rank the same documents here but produce different score magnitudes:

<SqlLogicTest id="sql/functions/full_text_search/dfi_saturated" />

<SqlLogicTest id="sql/functions/full_text_search/dfi_chi_squared" />

### `raw_tf(tableoid)` {#raw_tf}

**Signature.** `raw_tf(tableoid) -> FLOAT`. No parameters.
**Captures.** The raw count of matched-term occurrences in each row — a building block, not a ranking model.

Raw term frequency of the matched terms in each row. Use it inside a custom relevance expression; it does not normalize for length or rarity.

<SqlLogicTest id="sql/functions/full_text_search/raw_tf" />

### `raw_boost(tableoid)` {#raw_boost}

**Signature.** `raw_boost(tableoid) -> FLOAT`. No parameters.
**Captures.** The query-time boost factor that applied to each match (see [Boosting](#boosting) below).

Raw query boost contribution for each row. With no `^` boost in the query every match returns `1` — equivalent to a *constant score* (see [cross-engine notes](#cross-engine-notes)). When a clause is boosted with `^ f`, the matched rows carry that factor `f`.

<SqlLogicTest id="sql/functions/full_text_search/raw_boost" />

### `raw_dl(tableoid)` {#raw_dl}

**Signature.** `raw_dl(tableoid) -> FLOAT`. No parameters.
**Captures.** The length (token count) of the matched column for each row — the normalization input that `BM25(b>0)` and the `lm_*`/`dfi` scorers use internally.

Raw document length (number of tokens) of the matched column.

<SqlLogicTest id="sql/functions/full_text_search/raw_dl" />

## Boosting {#boosting}

The `^` operator multiplies a query clause's contribution to the score, so you can weight some clauses above others. The factor flows straight through every scorer: boosting `ts_phrase('fox') ^ 3.0` multiplies each matched row's BM25 score by exactly `3`.

<SqlLogicTest id="sql/functions/full_text_search/bm25_boost" />

`raw_boost` exposes the applied factor directly. See [Relevance ranking → Boosting](../../indexes/inverted/ranking.md#boosting) for boosting across multiple columns.

## Top-K and WAND pruning {#top-k}

The common shape `ORDER BY <scorer>(idx.tableoid) DESC LIMIT k` returns the best `k` matches. Building the index with the `optimize_top_k` option enables **WAND** pruning, which skips candidates that provably cannot reach the top `k`:

```sql
CREATE INDEX docs_idx ON docs
    USING inverted (id, body scored_en)
    WITH (optimize_top_k = 'bm25(1.2, 0.75)');
```

Pruning engages only when the `ORDER BY` scorer **matches the one named in `optimize_top_k` exactly** and the filter is a single term or an `OR` of terms; otherwise the query still runs correctly, just without the optimization. `EXPLAIN` shows `Top: k, optimized` on the scan when pruning is active. See [Relevance ranking → Top-K queries and WAND pruning](../../indexes/inverted/ranking.md#top-k-queries-and-wand-pruning) for the full conditions.

## Cross-engine notes {#cross-engine-notes}

If you are coming from Elasticsearch or OpenSearch, here is how their relevance-tuning concepts map onto SereneDB (the left column links to the Elasticsearch reference):

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| `_score` (implicit relevance) | any scorer over `tableoid`, e.g. `BM25(idx.tableoid)` |
| [`boosting`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html) query / per-clause `"^2"` | the `^` operator: `ts_phrase('fox') ^ 2.0` ([Boosting](#boosting)) |
| [`constant_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html) | `raw_boost(idx.tableoid)` returns `1` for every match when no `^` boost is applied; or `ORDER BY` a literal |
| [`function_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html) `weight` | fold the scorer into an arithmetic expression, e.g. `BM25(idx.tableoid) * 2` |
| [`function_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html) `field_value_factor` | blend the scorer with a column in the `SELECT` / `ORDER BY` expression |
| Top-K acceleration | `optimize_top_k` + WAND pruning ([Top-K](#top-k)) |
| Tie-breaking | extra `ORDER BY` columns, typically the primary key |
| [Reciprocal Rank Fusion](https://www.elastic.co/guide/en/elasticsearch/reference/current/rrf.html) | [Reciprocal Rank Fusion](../../../cookbook/search/reciprocal-rank-fusion.md) |

SereneDB has no single `function_score`-style query type. Because a scorer is just a `FLOAT`-valued expression, you compose the same effects directly in SQL — multiply, add, threshold in `WHERE`, or blend with table columns — rather than through a dedicated DSL.

## See also

- [Full-Text Search Functions](./full-text.md) — operators, constructors, parsers
- [Inverted Index](../../indexes/inverted/index.md) · [Relevance ranking guide](../../indexes/inverted/ranking.md)
- [Text Analysis](../../indexes/inverted/text-analysis.md#token-positions-and-feature-flags) — the `frequency` and `norm` flags scoring requires
- [BM25/TFIDF Ranking recipe](../../../cookbook/search/ranking.md) — parameter tuning
