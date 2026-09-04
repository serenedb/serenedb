---
title: Vector Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Vector-Distance Functions {#vector-distance-functions}

These functions measure how close two equal-length `FLOAT` vectors are — the basis of [vector search](../indexes/inverted/vector-search.md), where a query embedding is compared against the embeddings stored in a column. They come in two flavours that compute the same metrics but serve different roles.

- **Named functions** (`l2_distance`, `cosine_distance`, …) are plain scalars: pass any two vectors and get the distance back. They work in any expression, with or without an index, and are the right tool for ad-hoc scoring, re-ranking a candidate set or comparing two specific rows.
- **Operators** (`<->`, `<+>`, `<=>`, `<#>`) are the indexed form. When written as `ORDER BY embedding <-> :query LIMIT k`, the planner can route the query through the column's [IVF index](../indexes/inverted/vector-search.md) and return the approximate `k` nearest neighbours — far faster than scoring every row. Each operator computes one fixed metric and only accelerates a query when that metric **matches the index's configured `metric`** (`l2`, `l1`, `cosine` or `ip`).

Lower distance means more similar. `cosine_similarity` and `inner_product` are the exceptions — there, *higher* means more similar. `negative_inner_product` flips the sign so that lower is again more similar, which is what the `ip` index metric needs.

:::info Fixed-size `FLOAT[N]` is required
Every vector argument must be a fixed-size float array — `FLOAT[3]`, `FLOAT[768]`, etc. — and both operands of a distance must share the same dimension `N`. An unsized `FLOAT[]` is rejected, both as a function argument and as an indexed column. Write a literal as `[1, 0, 0]::FLOAT[3]`.
:::

| Function / operator | Metric | Direction | Description |
| :--- | :--- | :--- | :--- |
| [`l2_distance(a, b)`](#l2_distance) | L2 | lower = closer | Euclidean (straight-line) distance. |
| [`l2_sqr_distance(a, b)`](#l2_sqr_distance) | L2² | lower = closer | Squared Euclidean distance (skips the square root). |
| [`l1_distance(a, b)`](#l1_distance) | L1 | lower = closer | Manhattan distance. |
| [`cosine_distance(a, b)`](#cosine_distance) | cosine | lower = closer | `1 − cosine_similarity`. |
| [`cosine_similarity(a, b)`](#cosine_similarity) | cosine | **higher = closer** | Cosine of the angle between the vectors. |
| [`inner_product(a, b)`](#inner_product) | dot | **higher = closer** | Dot product. |
| [`negative_inner_product(a, b)`](#negative_inner_product) | ip | lower = closer | `−inner_product`; backs the `ip` index metric. |
| [`a <-> b`](#distance-operators) | L2 | lower = closer | Indexed distance, equivalent to `l2_distance`. |
| [`a <+> b`](#distance-operators) | L1 | lower = closer | Indexed distance, equivalent to `l1_distance`. |
| [`a <=> b`](#distance-operators) | cosine | lower = closer | Indexed distance, equivalent to `cosine_distance`. |
| [`a <#> b`](#distance-operators) | ip | lower = closer | Indexed distance, equivalent to `negative_inner_product`. |
| [`l2_norm(a)`](#norms) · [`l1_norm(a)`](#norms) | — | — | Vector magnitude (L2 / L1). |
| [`l2_normalize(a)`](#norms) · [`l1_normalize(a)`](#norms) | — | — | Scale to a unit vector (L2 / L1). |

## Choosing a metric {#choosing-a-metric}

The four metrics answer different questions about two vectors `x` and `y` of length `n`:

- **Euclidean / L2** — `sqrt(Σ (xᵢ − yᵢ)²)`. Straight-line distance through space. It accounts for both *direction* and *magnitude*, so it is the natural default when the absolute size of the components is meaningful (e.g. raw feature vectors, coordinates).
- **Squared Euclidean / L2²** — `Σ (xᵢ − yᵢ)²`. The same ordering as L2 with the square root dropped, so it is cheaper whenever you only rank or threshold and never need the true distance value.
- **Manhattan / L1** — `Σ |xᵢ − yᵢ|`. Sums the per-axis differences instead of combining them with Pythagoras. Less sensitive to a single large-deviation dimension than L2, and a common choice for sparse or high-dimensional data.
- **Cosine** — `1 − (x · y) / (‖x‖ · ‖y‖)`. Compares only the *direction* of the vectors and ignores their length. This is the usual choice for text and embedding models, where two documents about the same topic point the same way regardless of length. A zero vector has no direction, so cosine of a zero vector is undefined.
- **Inner product (dot)** — `Σ xᵢ yᵢ`. Rewards vectors that are both aligned *and* large. For vectors that are already L2-normalized to unit length, inner product equals cosine similarity, so many embedding pipelines normalize once and then use the cheaper dot product. Because higher means more similar, the index uses **negative** inner product (`negative_inner_product`) so that, like every other metric, a *smaller* value sorts first.

### Operators, metrics and the IVF index {#operators-and-the-index}

Each operator is bound to exactly one metric and is **independent of how a column happens to be indexed** — `a <-> b` always computes L2 even if `a` comes from a cosine-indexed column. To get acceleration, the operator's metric must match the index's `metric`:

| Operator | Metric | Equivalent function | Matching index `metric` |
| :--- | :--- | :--- | :--- |
| `a <-> b` | L2 | `l2_distance` | `l2` |
| `a <+> b` | L1 | `l1_distance` | `l1` |
| `a <=> b` | cosine | `cosine_distance` | `cosine` |
| `a <#> b` | ip | `negative_inner_product` | `ip` |

A query of the shape

```sql
SELECT id FROM index_name ORDER BY emb <-> $query LIMIT k;
```

lets the planner probe the [IVF index](../indexes/inverted/vector-search.md)'s clusters closest to `$query` and return the approximate `k` nearest neighbours, touching only a small fraction of the rows. Use the operator whose metric matches the index you built; mixing them (for example `<=>` against an `l2` index) still returns correct distances but cannot use the index, so it falls back to scanning every row. For tiny tables the planner may scan anyway because a sequential pass is cheaper than probing clusters — the speed-up matters at scale.

## Distance functions {#distance-functions}

#### `l2_distance(a, b)` {#l2_distance}

Euclidean (L2) distance between two equal-length `FLOAT` vectors — the straight-line distance in space.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`) |

<SqlLogicTest id="sql/functions/full_text_search/l2_distance" />

#### `l2_sqr_distance(a, b)` {#l2_sqr_distance}

The squared L2 distance. It preserves the same ordering as `l2_distance` while skipping the square root, so it is cheaper when you only need to rank or threshold.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`) |

<SqlLogicTest id="sql/functions/full_text_search/l2_sqr_distance" />

#### `l1_distance(a, b)` {#l1_distance}

Manhattan (L1) distance — the sum of the absolute per-component differences.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`) |

<SqlLogicTest id="sql/functions/full_text_search/l1_distance" />

#### `cosine_distance(a, b)` {#cosine_distance}

Cosine distance, defined as `1 − cosine_similarity`. It ignores vector magnitude and depends only on direction, which makes it the usual choice for normalized text/embedding models. Lies in `[0, 2]`.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`, non-zero) |

<SqlLogicTest id="sql/functions/full_text_search/cosine_distance" />

#### `cosine_similarity(a, b)` {#cosine_similarity}

The cosine of the angle between the vectors, in `[−1, 1]`. Unlike the distance functions, **higher** values mean more similar. For unit-length vectors it equals `inner_product`.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`, non-zero) |

<SqlLogicTest id="sql/functions/full_text_search/cosine_similarity" />

#### `inner_product(a, b)` {#inner_product}

The dot product of the two vectors. **Higher** means more similar; for unit-length vectors it equals `cosine_similarity`.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`) |

<SqlLogicTest id="sql/functions/full_text_search/inner_product" />

#### `negative_inner_product(a, b)` {#negative_inner_product}

`−inner_product`, so that lower values mean more similar — the form used by the `ip` index metric, where the index orders neighbours by ascending distance. It is the function behind the `<#>` operator.

| Operand | Type |
| :--- | :--- |
| `a`, `b` | `FLOAT[N]` (same `N`) |

<SqlLogicTest id="sql/functions/full_text_search/negative_inner_product" />

## Distance operators: `<->`, `<+>`, `<=>`, `<#>` {#distance-operators}

The operator forms are what drive an indexed nearest-neighbour search. Each maps to one metric and to a named function, and only accelerates a query when its metric matches the index (see [Operators, metrics and the IVF index](#operators-and-the-index)).

| Operator | Metric | Equivalent function |
| :--- | :--- | :--- |
| `a <-> b` | `l2` | `l2_distance` |
| `a <+> b` | `l1` | `l1_distance` |
| `a <=> b` | `cosine` | `cosine_distance` |
| `a <#> b` | `ip` | `negative_inner_product` |

### kNN: the `k` closest vectors {#knn}

Order by the operator and `LIMIT` to the number of neighbours you want. The query below asks the `l2`-metric index for the two nearest rows to the origin and projects the distance alongside the id:

<SqlLogicTest id="sql/functions/full_text_search/a---b-distance" />

### Range search: every vector within a radius {#range}

Compare the same operator in a `WHERE` clause to return **all** vectors closer than a threshold, rather than a fixed count:

<SqlLogicTest id="sql/functions/full_text_search/a---b-range" />

The two forms combine: add `ORDER BY emb <-> $query LIMIT k` to a range query to take the closest `k` *within* the radius.

### The other metrics {#operator-metrics}

The same pattern works with each metric's operator against an index built for that metric.

Manhattan (`<+>`, `l1` index):

<SqlLogicTest id="sql/functions/full_text_search/a-plus-b-distance" />

Cosine (`<=>`, `cosine` index) — id `1` is the query direction itself, so its distance is `0`:

<SqlLogicTest id="sql/functions/full_text_search/a-eq-b-distance" />

Inner product (`<#>`, `ip` index) — the operator returns the *negative* dot product, so the largest dot product (id `3`, the longest aligned vector) sorts first:

<SqlLogicTest id="sql/functions/full_text_search/a-hash-b-distance" />

## Norms and normalization {#norms}

`l2_norm` and `l1_norm` return a vector's magnitude; `l2_normalize` and `l1_normalize` scale it to a unit vector under the corresponding norm. Normalizing before indexing with `cosine` (or comparing with `inner_product`) keeps comparisons magnitude-independent — once vectors are unit-length, `inner_product` and `cosine_similarity` coincide.

| Function | Operand | Returns |
| :--- | :--- | :--- |
| `l2_norm(a)` · `l1_norm(a)` | `FLOAT[N]` | scalar magnitude |
| `l2_normalize(a)` · `l1_normalize(a)` | `FLOAT[N]` | `FLOAT[N]` unit vector |

<SqlLogicTest id="sql/functions/full_text_search/l2_norm" />

<SqlLogicTest id="sql/functions/full_text_search/l1_norm" />

<SqlLogicTest id="sql/functions/full_text_search/l2_normalize" />

<SqlLogicTest id="sql/functions/full_text_search/l1_normalize" />

For array-typed distance helpers (`array_distance`, `array_cosine_distance`, …) see [Array Functions](./array.md).

## Coming from Elasticsearch

Elasticsearch [kNN search](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html) over a [`dense_vector`](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html) field maps onto a SereneDB `ivf` index queried with a distance operator:

| Elasticsearch / OpenSearch | SereneDB |
| :--- | :--- |
| [`dense_vector`](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html) field + `index: ivf` | `FLOAT[N]` column with an [`ivf` operator class](../indexes/inverted/vector-search.md) |
| [`similarity: l2_norm`](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html#dense-vector-similarity) | [`<->`](#distance-operators) / [`l2_distance`](#l2_distance) (metric `l2`) |
| [`similarity: cosine`](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html#dense-vector-similarity) | [`<=>`](#distance-operators) / [`cosine_distance`](#cosine_distance) (metric `cosine`) |
| [`similarity: dot_product`](https://www.elastic.co/guide/en/elasticsearch/reference/current/dense-vector.html#dense-vector-similarity) / `max_inner_product` | [`<#>`](#distance-operators) / [`negative_inner_product`](#negative_inner_product) (metric `ip`) |
| [`knn`](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html) query (`k`, `num_candidates`) | [`ORDER BY emb <-> $q LIMIT k`](#knn) |
| [`knn`](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html) with `filter` | a `WHERE` clause beside the `ORDER BY` ([Hybrid Search](../indexes/inverted/hybrid-search.md)) |

**Notable differences.** SereneDB adds a Manhattan metric ([`<+>`](#distance-operators) / [`l1_distance`](#l1_distance)) that Elasticsearch lacks, while Elasticsearch's `l_inf` / Hamming (binary-vector) similarities have no SereneDB equivalent. Cluster count ([`nlist`](../indexes/inverted/vector-search.md)) and vector compression ([`quant`](../indexes/inverted/vector-search.md#quantization)) are set at index build time, while [`sdb_nprobe`](../indexes/inverted/maintenance.md#session-settings) — SereneDB's analogue of `num_candidates` — and [`sdb_rerank_factor`](../indexes/inverted/maintenance.md#session-settings) tune recall per session.

## See also

- [Full-Text Search Functions](./search/full-text.md) — operators, constructors, parsers
- [Inverted Index](../indexes/inverted/index.md) · [Vector Search](../indexes/inverted/vector-search.md)
