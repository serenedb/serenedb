---
title: Vector Search
sidebar_position: 7
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The [inverted index](./index.md) also indexes **vector embeddings** for approximate nearest neighbor (ANN) search. This powers semantic search, recommendations and other similarity workloads over `FLOAT` vectors. Two index kinds are available: **IVF** (`ivf (...)`), which partitions the vectors into clusters, and **[HNSW](#hnsw)** (`hnsw (...)`), which links them into a navigable graph.

IVF partitions the vectors into `nlist` coarse clusters (found by k-means at build time). A query first identifies the clusters closest to the query vector, then computes distances only within those, so a search touches a small fraction of the vectors instead of scanning them all. That is what makes it *approximate* — it trades a little recall for a large speed-up, and the number of clusters scanned (`nprobe`, a query-time setting) tunes that trade-off. Optional quantization (`quant`) compresses each stored vector to shrink the index further, at some additional recall cost that can be recovered by reranking.

## Creating a vector index

A vector column uses the `ivf (...)` or [`hnsw (...)`](#hnsw) operator class. The column must be a fixed-size `FLOAT` array (`FLOAT[N]`) — every row shares the same dimension `N`:

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_001" />

For `ivf`, the `metric` is required and everything else is optional, defaulting to an unquantized index sized automatically from the row count:

| Parameter | Description |
|---|---|
| `metric` | Distance metric: `l2` (Euclidean), `cosine`, `ip` (inner product) or `l1` (Manhattan) |
| `nlist` | Number of coarse clusters. Higher values narrow each cluster (faster, more precise probes) at the cost of build time. Mutually exclusive with `nlist_factor` |
| `nlist_factor` | Sizes `nlist` relative to the row count as `round(nlist_factor * sqrt(rows))`. Default `2.0`. Mutually exclusive with `nlist` |
| `quant` | Vector compression: `none` (default), `sq8`, `sq4`, `pq` or `rabitq` — see [Quantization](#quantization) below. Only valid with `metric` `l2` or `ip` |
| `pq_m` | Number of subquantizers for `quant = 'pq'`. Must evenly divide the vector dimension `N`. Defaults to a value close to a 2-dimensional subvector |
| `rabitq_bits` | Extra magnitude bits per dimension for `quant = 'rabitq'`, `1`–`9`. Default `1` (sign-only) |

There are two ways to query a vector index: **k-nearest-neighbor** search (the closest `k` vectors) and **range** search (every vector within a distance threshold).

### k-nearest-neighbor (kNN)

Order by the distance to a query vector and `LIMIT` to the number of neighbors you want. Each distance operator computes a **fixed** metric — `<->` is L2, `<=>` is cosine, `<+>` is L1 and `<#>` is inner product — so use the one matching the metric your index was built with; the optimizer then routes the query through the IVF index:

```sql
SELECT id FROM index_name ORDER BY emb <-> $query_vector LIMIT k;
```

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_004" />

The named distance functions [`l2_distance`](../../functions/vector.md), `cosine_distance`, `l1_distance` and `negative_inner_product` are equivalent to the matching operator and can be used explicitly:

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_003" />

<DocCallout type="tip">

The same kNN query works whether you select from the index by name or from the base table — the optimizer routes an `ORDER BY emb <-> ... LIMIT k` through the IVF index automatically.

</DocCallout>

<DocCallout type="tip">

Scan more clusters for better recall with the [`sdb_ivf_search_nprobe` session setting](./maintenance.md#session-settings) (default `8`), or set an HNSW beam with [`sdb_hnsw_ef_search`](./maintenance.md#session-settings) (default `-1`: the index's `ef_construction` or the `LIMIT`, whichever is larger). Neither affects range queries, which always prune across every cluster.

</DocCallout>

<DocCallout type="tip">

For a quantized index (`quant` other than `none`), the [`sdb_ann_oversample` session setting](./maintenance.md#session-settings) controls how many candidates are re-scored with exact distances before the top `k` is picked; `0` answers from the codes alone. See [Oversampling](#oversampling).

</DocCallout>

### Range (radius) search

Instead of a fixed number of neighbors, return **every** vector within a distance **threshold** (a radius) by comparing the distance in a `WHERE` clause:

```sql
SELECT id FROM index_name WHERE emb <-> $query_vector < radius;
```

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_002" />

The two forms combine: add `ORDER BY emb <-> $query_vector LIMIT k` to a range query to take the closest `k` *within* the radius.

## Quantization {#quantization}

By default an `ivf` index stores full-precision vectors (`quant = 'none'`); an [`hnsw`](#hnsw) index quantizes to `sq8`. Setting `quant` compresses the stored codes to shrink the index, trading some recall for size — recoverable with [oversampling](#oversampling), which re-scores a candidate pool with exact distances before picking the final `k`:

| `quant` | Compression | Notes |
|---|---|---|
| `none` | none | Full-precision vectors; never reranks regardless of `sdb_ann_oversample`. The `ivf` default |
| `sq8` | 8-bit scalar quantization per dimension | Good recall/size trade-off. The `hnsw` default |
| `usq8` / `usq4` | As `sq8` / `sq4`, but one min/max for the whole vector instead of one per dimension | Cheaper to decode; `hnsw` only |
| `tq` | TurboQuant, `nb_bits` per component | `hnsw` only |
| `sq4` | 4-bit scalar quantization per dimension | Smaller than `sq8`, lower recall before reranking |
| `pq` | Product quantization — the vector is split into `pq_m` subvectors, each quantized against its own small codebook | Highest compression; recall is sensitive to `pq_m` (must divide `N`) |
| `rabitq` | RaBitQ binary quantization, 1 bit per dimension plus `rabitq_bits` − 1 extra magnitude bits | Very compact; `rabitq_bits` (1–9) trades size for recall |

`quant` only applies to `metric = 'l2'` or `'ip'` indexes — `cosine` and `l1` indexes are always unquantized.

Quantization also speeds up the scan itself, not just the index size: quantized codes are stored inline in each cluster's postings, laid out contiguously per cluster, so a probe reads them sequentially instead of chasing full vectors elsewhere; and comparing quantized codes (a table lookup for `pq`, a popcount for `rabitq`, integer arithmetic for `sq8`/`sq4`) is cheaper than a full-precision `FLOAT[N]` distance. So `quant` is a query-latency optimization as much as a storage one — the smaller, posting-aware layout is what lets `nprobe` scan more clusters for the same latency budget.

### Oversampling {#oversampling}

A quantized score is an *estimate*, and two segments' estimates are not comparable — each segment trains its own quantizer. [`sdb_ann_oversample`](./maintenance.md#session-settings) says how much of that to undo: the search runs on the codes, `ceil(sdb_ann_oversample * k)` of each segment's candidates are read back at full precision and re-ordered, and only then are segments compared against each other.

- `-1` (the default) lets the engine choose by code width: 8-bit codes rank well enough alone, everything narrower is re-scored.
- `0` answers from the codes, which is faster and caps recall at whatever the codes can tell apart.
- A value above `1` widens the pool. For HNSW the beam is widened with it, since a beam of a hundred cannot hand four hundred candidates to the rescorer.

Elasticsearch spells this `rescore_vector.oversample`, with the same `0`; Qdrant splits it into `quantization.oversampling` and `quantization.rescore`.

## HNSW {#hnsw}

`hnsw (...)` builds a **navigable small-world graph** instead of coarse clusters. Every vector is a node; each node keeps up to `m` neighbours per layer (`2m` on the bottom one), and a query descends the layers greedily, keeping a beam of the `sdb_hnsw_ef_search` best candidates it has seen. Where IVF narrows the search by *partitioning* the vectors, HNSW narrows it by *navigating* between them, which is usually the better trade at high recall:

```sql
CREATE INDEX idx ON docs USING inverted(id, emb hnsw (metric = 'l2'));
```

| Parameter | Description |
|---|---|
| `metric` | Distance metric: `l2`, `cosine`, `ip` or `l1`. Required |
| `m` | Neighbours kept per node per layer, `2m` on the bottom layer. Default `32`. Higher = better recall and a larger index |
| `ef_construction` | Beam width while building. Default `200`, must be `>= m`. Higher = a better graph and a slower build; it does not affect query time |
| `quant` | `none`, `sq8` (the default), `sq4`, `usq8`, `usq4`, `rabitq` or `tq` — see [Quantization](#quantization). `pq` is IVF-only, and `metric = 'l1'` accepts `none` only |
| `nb_bits` | Bits per component for `quant = 'tq'` |
| `compression` | Whether the stored vectors are compressed. Default `true` |

Unlike IVF, HNSW quantizes by default: a graph walk reads a great many candidate vectors, so the smaller code pays for itself, and [oversampling](#oversampling) restores the recall it costs.

### Filtered search {#filtered-search}

A `WHERE` beside a kNN `ORDER BY` is answered **inside** the graph walk rather than after it — the walk keeps moving through every node but admits only the rows the predicate passes, so a selective filter does not empty the result the way filtering the top `k` afterwards would:

```sql
SELECT id FROM idx
WHERE lang = 'ja'
ORDER BY emb <-> $query_vector
LIMIT 10;
```

How a rejected row is treated is [`sdb_hnsw_filter_mode`](./maintenance.md#session-settings). `auto`, the default, decides per segment from the predicate's estimated selectivity: a predicate that admits few enough rows is answered by scoring exactly those rows, which is both faster and exact; otherwise the graph is walked. The remaining values force one shape and exist for measurement:

| Mode | The walk |
|---|---|
| `auto` | Picks by estimated selectivity. The default |
| `scan` | Scores every row the predicate admits; no graph |
| `walk` | Scores every neighbour and passes through rejected rows |
| `prune` | Scores and expands admitted rows only |
| `twohop` | Expands a rejected row's own neighbours in its place |
| `bridge` | Scores every neighbour but expands a rejected row into admitted ones only |

Every walk is capped at what the scan would have cost and falls back to it, forced ones included: a mode is a preference, never a way to spend more than the exact answer costs.

## Column types

A vector column must be a fixed-size `FLOAT[N]` array — all rows share dimension `N` (an unsized `FLOAT[]` is rejected). Unlike text and `INCLUDE`d columns, a vector column does not take a storage `compression` codec — use `quant` instead to control its on-disk size.

## See also

- [Hybrid Search](./hybrid-search.md) — combine vector ranking with full-text or structured filters
- [Inverted Index](./index.md) · [Full-Text Search](./full-text-search.md)
- [Vector Functions](../../functions/vector.md) — distance functions and the `<->` operator
