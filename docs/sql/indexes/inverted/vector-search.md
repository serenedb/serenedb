---
title: Vector Search
sidebar_position: 7
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The [inverted index](./index.md) also indexes **vector embeddings** for approximate nearest neighbor (ANN) search. This powers semantic search, recommendations and other similarity workloads over `FLOAT` vectors. Two index kinds are available: **IVF** (`ivf (...)`), which partitions the vectors into clusters, and **[HNSW](#hnsw)** (`hnsw (...)`), which links them into a navigable graph.

IVF groups the vectors of each index segment into clusters, sized by the `sdb_ivf_posting_size` setting (1024 vectors by default, read when the index is created), so the number of clusters follows the segment's row count. A query first identifies the clusters closest to the query vector, then computes distances only within those, so a search touches a small fraction of the vectors instead of scanning them all. That is what makes it *approximate* -- it trades a little recall for a large speed-up, and the number of clusters scanned, set by the [`sdb_ivf_search_nprobe`](./maintenance.md#session-settings) session setting, tunes that trade-off. Optional quantization (`quant`) compresses each stored vector to shrink the index further, at some additional recall cost that can be recovered by reranking.

## Creating a vector index

A vector column uses the `ivf (...)` or [`hnsw (...)`](#hnsw) operator class. The column must be a fixed-size `FLOAT` array (`FLOAT[N]`) -- every row shares the same dimension `N`:

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_001" />

For `ivf`, the `metric` is required. Everything else is optional: no option sizes the clusters, and for `l2`, `ip` and `cosine` the vectors are quantized with `sq8` unless `quant` says otherwise:

| Parameter | Description |
|---|---|
| `metric` | Distance metric: `l2` (Euclidean), `cosine`, `ip` (inner product) or `l1` (Manhattan) |
| `quant` | Vector compression: `sq8` (the default for `l2`, `ip` and `cosine`), `sq4`, `usq8`, `usq4`, `fp16`, `bf16`, `pq`, `rabitq`, `tq`, `bq`, `osq` or `none` (the default for `l1`) -- see [Quantization](#quantization) below. `l1` indexes are always unquantized, and `rabitq` needs `l2` or `ip` |
| `pq_m` | Number of subquantizers for `quant = 'pq'`. Must evenly divide the vector dimension `N`. Defaults to a value close to a 2-dimensional subvector |
| `rabitq_bits` | Extra magnitude bits per dimension for `quant = 'rabitq'`, `1` to `9`. Default `1` (sign-only). Also spelled `nb_bits` |
| `nb_bits` | Bits per dimension for `quant = 'tq'`: a whole number from `1` to `5`, or `1.5`. Default `3`. For `quant = 'bq'`: `1`, `1.5` or `2`, default `1`. For `quant = 'osq'`: `1`, `2`, `4`, `7` or `8`, default `1` |
| `query_bits` | How the query is encoded. For `quant = 'bq'`: `1` compares it bit for bit with the stored codes, `4` and `8` keep it as 4- or 8-bit integers for a finer estimate; default `1`. For `quant = 'osq'`: `4` (the default) or `8` when `nb_bits` is at most `4`, otherwise the same as `nb_bits` |
| `compression` | Whether the stored vectors are compressed. Default `true`; `false` stores them uncompressed, which searches faster and takes more disk |

There are two ways to query a vector index: **k-nearest-neighbor** search (the closest `k` vectors) and **range** search (every vector within a distance threshold).

### k-nearest-neighbor (kNN)

Order by the distance to a query vector and `LIMIT` to the number of neighbors you want. Each distance operator computes a **fixed** metric -- `<->` is L2, `<=>` is cosine, `<+>` is L1 and `<#>` is inner product -- so use the one matching the metric your index was built with; the optimizer then routes the query through the vector index, and `EXPLAIN` shows the distance as the scan's `Score` with a `Top: k` row. With another operator the query still returns correct distances, but from a full scan: a `TOP_N` over a `PROJECTION` that computes each distance.

A kNN query returns `k` rows whenever at least `k` rows match its `WHERE`. When the clusters an `ivf` index probes hold fewer, the query is answered exactly from every matching row instead.

```sql
SELECT id FROM index_name ORDER BY emb <-> $query_vector LIMIT k;
```

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_004" />

The named distance functions [`l2_distance`](../../functions/vector.md), `cosine_distance`, `l1_distance` and `negative_inner_product` are equivalent to the matching operator and can be used explicitly:

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_003" />

<DocCallout type="tip">

Select from the index by name. The same query against the base table returns exact distances from a full scan: its plan shows a `TOP_N` over a sequential scan instead of an `IRESEARCH_SCAN`.

</DocCallout>

<DocCallout type="tip">

Scan more clusters for better recall with the [`sdb_ivf_search_nprobe` session setting](./maintenance.md#session-settings) (default `-1`: chosen per segment from the `LIMIT` and the segment's list count), or set an HNSW beam with [`sdb_hnsw_ef_search`](./maintenance.md#session-settings) (default `-1`: the index's `ef_construction` or the `LIMIT`, whichever is larger). Neither affects range queries, which always prune across every cluster.

</DocCallout>

<DocCallout type="tip">

For a quantized index (`quant` other than `none`), the [`sdb_ann_rescore_width` session setting](./maintenance.md#session-settings) controls how many candidates are re-scored with exact distances before the top `k` is picked; `0` answers from the codes alone. See [Re-scoring](#rescoring).

</DocCallout>

### Range (radius) search

Instead of a fixed number of neighbors, return **every** vector within a distance **threshold** (a radius) by comparing the distance in a `WHERE` clause:

```sql
SELECT id FROM index_name WHERE emb <-> $query_vector < radius;
```

<SqlLogicTest id="sql/indexes/inverted/vector-search/example_002" />

The two forms combine: add `ORDER BY emb <-> $query_vector LIMIT k` to a range query to take the closest `k` *within* the radius.

On a quantized index the codes pick the candidates and each candidate is then checked against its exact distance, so every returned row is inside the radius. A row whose code places it farther away than it really is can still be missed, more often with coarser codes. With [`sdb_ann_force_exact`](./maintenance.md#session-settings) on, the radius is answered by scoring every row from its stored vector, so no row inside it is missed.

### Distances without a limit

A distance selected without `ORDER BY ... LIMIT` and without a radius, such as `SELECT id, emb <-> $query_vector FROM index_name` or an aggregate over it, is computed exactly from the stored vector of every row the `WHERE` admits. The index is not walked, and `EXPLAIN` shows the scan as `Exact`.

## Quantization {#quantization}

An `l2`, `ip` or `cosine` index stores its codes quantized with `sq8` unless `quant` says otherwise, `ivf` and [`hnsw`](#hnsw) alike; an `l1` index stores full-precision vectors. Quantization shrinks the index, trading some recall for size -- recoverable with [re-scoring](#rescoring), which reads candidates back at full precision before picking the final `k`:

| `quant` | Compression | Notes |
|---|---|---|
| `none` | none | Full-precision vectors; never re-scored. The only choice for `l1`. With 256 to 4096 dimensions and at least eight rows per dimension, an `hnsw` index searches faster with the same answer; it stores one more float per row, and holds a `N` x `N` float matrix in memory |
| `sq8` (default) | 8-bit scalar quantization per dimension | Good recall/size trade-off. With `ip` or `cosine`, 256 to 4096 dimensions and at least eight rows per dimension, an `hnsw` index searches faster; it stores one more float per row for every 128 dimensions, and holds the same matrix as `none` in memory, plus an 8-bit copy of it |
| `sq4` | 4-bit scalar quantization per dimension | Smaller than `sq8`, lower recall before reranking |
| `usq8` / `usq4` | As `sq8` / `sq4`, but one min/max for the whole vector instead of one per dimension | Cheaper to decode |
| `fp16` / `bf16` | Every component stored as a 16-bit float: IEEE half precision, or bfloat16 (the float exponent with a shorter mantissa) | Half the size of `none` with scores nearly as exact, so they are not re-scored by default; `bf16` keeps the full float range, `fp16` keeps more precision |
| `pq` | Product quantization -- the vector is split into `pq_m` subvectors, each quantized against its own codebook: 16 centroids (4 bits) per subvector under `ivf`, 256 centroids (one byte) under `hnsw` | Highest compression; recall is sensitive to `pq_m` (must divide `N`) |
| `rabitq` | RaBitQ binary quantization, 1 bit per dimension plus `rabitq_bits` - 1 extra magnitude bits | Very compact; `rabitq_bits` (1 to 9) trades size for recall. Needs `l2` or `ip` |
| `tq` | Low-bit quantization with `nb_bits` (1 to 5 or 1.5, default 3) bits per dimension | Size and recall grow with `nb_bits` |
| `bq` | Binary quantization: 1 bit per dimension, set above a cut point fitted to that dimension's values; with `nb_bits = 2`, a second bit splits each dimension into a low, middle and high range; `1.5` shares the second bit between two neighbouring dimensions | Smallest and cheapest to compare. With `query_bits = 1` the search compares bit counts, which is fastest; `4` or `8` estimate the real distance throughout, as a range query always does |
| `osq` | `nb_bits` (1, 2, 4, 7 or 8, default 1) bits per dimension relative to the centroid, on a quantization interval fitted to each vector, with a few correction terms stored beside the codes; the query is quantized to `query_bits` | Scores estimate the real distance; per-vector intervals pay off most at 1 to 4 bits. Each vector carries 16 extra bytes |

Quantization applies to `l2`, `ip` and `cosine` indexes; `l1` indexes are always unquantized.

Quantization makes queries faster as well as the index smaller: a quantized `ivf` index scans its clusters faster, so [`sdb_ivf_search_nprobe`](./maintenance.md#session-settings) can probe more of them at the same latency.

### Re-scoring {#rescoring}

A quantized score is an *estimate*. [`sdb_ann_rescore_width`](./maintenance.md#session-settings) sets how many candidates are read back at full precision before the final `k` is picked: a candidate, from whichever segment, is re-scored while its code score plus `sdb_ann_rescore_width` times the code's estimated error could still beat the `k`-th best exact score found so far. A segment holding many of the nearest neighbours has more of them re-scored, and a segment holding none has none.

- `-1` (the default) uses a width per quantizer, larger where the error estimate is looser: `2` for `sq8` and 1-bit `rabitq`, `3` for `tq` and multi-bit `rabitq`, `4` for `usq8`, `5` for `sq4`, `6` for `usq4`, and `7` for `osq` at 4 or more bits. `pq` uses `2` on an `ivf` index and `3` on an `hnsw` index, `bq` `1` and `3`, and `osq` at 1 or 2 bits `3` and `2`. `fp16` and `bf16` answer from their codes, which are close to exact already.
- `0` answers from the codes, which is faster and caps recall at whatever the codes can tell apart.
- A larger value re-scores more candidates and misses fewer: a neighbour sitting exactly at the `k`-th score is missed with the probability that a normal error exceeds that many standard deviations, about 2% at `2` and 0.1% at `3`. Neighbours further above the `k`-th score are missed less often.
- It does not widen the search: [`sdb_hnsw_ef_search`](./maintenance.md#session-settings) and [`sdb_ivf_search_nprobe`](./maintenance.md#session-settings) decide which candidates there are to re-score.

## HNSW {#hnsw}

`hnsw (...)` builds a **navigable small-world graph** instead of coarse clusters. Every vector is a node; each node keeps up to `m` neighbours per layer (`2m` on the bottom one), and a query descends the layers greedily, keeping a beam of the `sdb_hnsw_ef_search` best candidates it has seen. Where IVF narrows the search by *partitioning* the vectors, HNSW narrows it by *navigating* between them, which is usually the better trade at high recall. It takes the same `metric` and is queried the same way, with the distance operator that matches the metric:

<SqlLogicTest id="sql/indexes/inverted/vector-search/hnsw" />

| Parameter | Description |
|---|---|
| `metric` | Distance metric: `l2`, `cosine`, `ip` or `l1`. Required |
| `m` | Neighbours kept per node per layer, `2m` on the bottom layer, at least `2`. Default `32`. Higher = better recall and a larger index |
| `ef_construction` | Beam width while building. Default `200`, must be `>= m`. Higher = a better graph and a slower build; it does not affect query time |
| `quant` | `none`, `sq8` (the default), `sq4`, `usq8`, `usq4`, `fp16`, `bf16`, `pq`, `rabitq`, `tq`, `bq` or `osq` -- see [Quantization](#quantization). `metric = 'l1'` accepts `none` only |
| `pq_m` | Number of subquantizers for `quant = 'pq'`, each coded in one byte. Must evenly divide the vector dimension `N`. Defaults to a value close to a 4-dimensional subvector, 2 bits per dimension |
| `nb_bits` | Bits per dimension for `quant = 'tq'`, `1` to `5` or `1.5`, default `3`; for `quant = 'bq'`, `1`, `1.5` or `2`, default `1`; for `quant = 'osq'`, `1`, `2`, `4`, `7` or `8`, default `1`; extra magnitude bits for `quant = 'rabitq'`, `1` to `9`, default `1` |
| `query_bits` | Query encoding for `quant = 'bq'` and `quant = 'osq'` -- see the `ivf` table above |
| `compression` | Whether the stored vectors are compressed. Default `true` |

A graph walk reads a great many candidate vectors, so the smaller code pays for itself, and [re-scoring](#rescoring) restores the recall it costs.

### Filtered search {#filtered-search}

A `WHERE` beside a kNN `ORDER BY` is answered **inside** the graph walk rather than after it -- the walk keeps moving through every node but admits only the rows the predicate passes, so a selective filter does not empty the result the way filtering the top `k` afterwards would:

```sql
SELECT id FROM idx
WHERE lang = 'ja'
ORDER BY emb <-> $query_vector
LIMIT 10;
```

How a rejected row is treated is [`sdb_hnsw_filter_mode`](./maintenance.md#session-settings). `auto`, the default, decides per segment from the predicate's estimated selectivity: a predicate that admits few enough rows is answered by scoring exactly those rows, which is both faster and exact; otherwise the graph is walked. Where the vectors' codes are long -- longer than a row's links plus the fixed cost of visiting it, about 320 bytes, as with 1024 dimensions at 8 bits, unquantized vectors, or `pq` codes longer than 64 bytes -- the walk is `twohop`: it scores admitted rows only and crosses a rejected one through its neighbours, because reading a rejected row's links is then cheaper than scoring its code. Shorter codes are cheap enough to score, and the walk starts by passing through rejected rows as `walk` does; once the rows it has scored are admitted at well under the predicate's share, as when the predicate keeps rows far from the query, it crosses rejected rows as `twohop` does for the rest of the walk. The remaining values force one shape and exist for measurement:

| Mode | The walk |
|---|---|
| `auto` | Picks by estimated selectivity; walks as `twohop` where codes are long, and where they are short as `walk` until the rows it scores turn out mostly rejected. The default |
| `scan` | Scores every row the predicate admits; no graph |
| `walk` | Scores every neighbour and passes through rejected rows |
| `prune` | Scores and expands admitted rows only |
| `twohop` | Expands a rejected row's own neighbours in its place |
| `bridge` | Scores every neighbour but expands a rejected row into admitted ones only |

Every walk is capped at what the scan would have cost and falls back to it, forced ones included: a mode is a preference, never a way to spend more than the exact answer costs.

A predicate on an `INCLUDE`d column -- one the index stores but does not index -- is answered from the columnstore, in one of two ways. The predicate is either folded over the whole segment before the walk starts, a vectorised compare per row, or the walk reads the columns of each row it reaches, one positioned read per row at the cost of a few dozen rows of the fold. The walk reads while it is expected to reach few enough rows for that to be the cheaper -- a narrow beam over a large segment -- and folds otherwise. A column compressed with `zstd`, whose read decompresses its block up to the row, is priced at a few hundred rows of the fold. [`sdb_hnsw_column_filter`](./maintenance.md#session-settings) forces either for measurement.

The predicate need not be a single comparison. An `IN` list or an `OR` on one `INCLUDE`d column, an expression over several of them, and `AND`s and `OR`s that mix them with predicates on indexed fields are all answered inside the walk too:

```sql
SELECT id FROM idx
WHERE lang = 'ja' OR id < 1000
ORDER BY emb <-> $query_vector
LIMIT 10;
```

Here `lang` is `INCLUDE`d and `id` is indexed: the part on indexed fields -- a comparison, a range, a full-text `@@` match -- is answered from the index once per segment, and the rest is read from the columnstore for each row the walk reaches. `EXPLAIN` shows such a predicate as the scan's `Row Filter`. A predicate on a column that is neither indexed nor `INCLUDE`d needs the base table, which the walk cannot read. Its rows are checked after the search, nearest first. If fewer than `k` pass, every row is ranked by its exact distance and checked in that order until `k` pass or none are left: the answer is complete, but it can cost a scan of the whole index. When such a predicate is `OR`ed with the others, the distance is computed exactly for every row instead. Add the column to `INCLUDE` to filter inside the walk.

## Column types

A vector column must be a fixed-size `FLOAT[N]` array -- all rows share dimension `N` (an unsized `FLOAT[]` is rejected). Unlike text and `INCLUDE`d columns, a vector column does not take a storage `compression` codec -- use `quant` instead to control its on-disk size.

## See also

- [Hybrid Search](./hybrid-search.md) -- combine vector ranking with full-text or structured filters
- [Inverted Index](./index.md) · [Full-Text Search](./full-text-search.md)
- [Vector Functions](../../functions/vector.md) -- distance functions and the `<->` operator
