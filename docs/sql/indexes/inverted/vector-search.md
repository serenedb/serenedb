---
title: Vector Search
sidebar_position: 7
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The [inverted index](./index.md) also indexes **vector embeddings** for approximate nearest neighbor (ANN) search, using an IVF (Inverted File) index. This powers semantic search, recommendations and other similarity workloads over `FLOAT` vectors.

IVF partitions the vectors into `nlist` coarse clusters (found by k-means at build time). A query first identifies the clusters closest to the query vector, then computes distances only within those, so a search touches a small fraction of the vectors instead of scanning them all. That is what makes it *approximate* — it trades a little recall for a large speed-up, and the number of clusters scanned (`nprobe`, a query-time setting) tunes that trade-off. Optional quantization (`quant`) compresses each stored vector to shrink the index further, at some additional recall cost that can be recovered by reranking.

## Creating a vector index

A vector column uses the `ivf (...)` operator class. The column must be a fixed-size `FLOAT` array (`FLOAT[N]`) — every row shares the same dimension `N`:

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_001" />

The `metric` is required. Everything else is optional and defaults to an unquantized index sized automatically from the row count:

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

Scan more clusters for better recall with the [`sdb_nprobe` session setting](./maintenance.md#session-settings) (default `8`). It only affects kNN — range queries always prune across every cluster.

</DocCallout>

<DocCallout type="tip">

For a quantized index (`quant` other than `none`), the [`sdb_rerank_factor` session setting](./maintenance.md#session-settings) controls how many candidates are re-scored with exact distances before the top `k` is picked; `0` disables reranking.

</DocCallout>

### Range (radius) search

Instead of a fixed number of neighbors, return **every** vector within a distance **threshold** (a radius) by comparing the distance in a `WHERE` clause:

```sql
SELECT id FROM index_name WHERE emb <-> $query_vector < radius;
```

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_002" />

The two forms combine: add `ORDER BY emb <-> $query_vector LIMIT k` to a range query to take the closest `k` *within* the radius.

## Quantization {#quantization}

By default (`quant = 'none'`) the index stores full-precision vectors. Setting `quant` compresses the stored codes to shrink the index, trading some recall for size — recoverable with [`sdb_rerank_factor`](./maintenance.md#session-settings), which re-scores a candidate pool with exact distances before picking the final `k`:

| `quant` | Compression | Notes |
|---|---|---|
| `none` (default) | none | Full-precision vectors; never reranks regardless of `sdb_rerank_factor` |
| `sq8` | 8-bit scalar quantization per dimension | Good recall/size trade-off; a reasonable default when shrinking a large index |
| `sq4` | 4-bit scalar quantization per dimension | Smaller than `sq8`, lower recall before reranking |
| `pq` | Product quantization — the vector is split into `pq_m` subvectors, each quantized against its own small codebook | Highest compression; recall is sensitive to `pq_m` (must divide `N`) |
| `rabitq` | RaBitQ binary quantization, 1 bit per dimension plus `rabitq_bits` − 1 extra magnitude bits | Very compact; `rabitq_bits` (1–9) trades size for recall |

`quant` only applies to `metric = 'l2'` or `'ip'` indexes — `cosine` and `l1` indexes are always unquantized.

Quantization also speeds up the scan itself, not just the index size: quantized codes are stored inline in each cluster's postings, laid out contiguously per cluster, so a probe reads them sequentially instead of chasing full vectors elsewhere; and comparing quantized codes (a table lookup for `pq`, a popcount for `rabitq`, integer arithmetic for `sq8`/`sq4`) is cheaper than a full-precision `FLOAT[N]` distance. So `quant` is a query-latency optimization as much as a storage one — the smaller, posting-aware layout is what lets `nprobe` scan more clusters for the same latency budget.

## Column types

A vector column must be a fixed-size `FLOAT[N]` array — all rows share dimension `N` (an unsized `FLOAT[]` is rejected). Unlike text and `INCLUDE`d columns, a vector column does not take a storage `compression` codec — use `quant` instead to control its on-disk size.

## See also

- [Hybrid Search](./hybrid-search.md) — combine vector ranking with full-text or structured filters
- [Inverted Index](./index.md) · [Full-Text Search](./full-text-search.md)
- [Vector Functions](../../functions/vector.md) — distance functions and the `<->` operator
