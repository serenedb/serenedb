---
title: Maintenance & Introspection
sidebar_position: 12
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

An inverted index is **eventually consistent**. Writes to the base table are buffered and become searchable only after the index is *refreshed*; segments are *compacted* in the background to keep queries fast; and per-term statistics used by scorers can be *recomputed*. This page covers that lifecycle and how to inspect an index.

```mermaid
flowchart LR
    w["INSERT / UPDATE / DELETE"] --> buf["pending writes"]
    buf -->|refresh| vis["searchable"]
    vis -->|compaction| merged["fewer, larger segments"]
```

## Visibility and the refresh model

A newly written row is not searchable until a refresh publishes it to readers. There are two ways a refresh happens:

- **Automatically** — a background thread refreshes each index on an interval (`refresh_interval`, default 1000 ms).
- **Explicitly** — run `VACUUM (REFRESH_TABLE)` to publish pending writes immediately. This is what you want right after a bulk load, before querying:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_001" />

## Background intervals

Three index `WITH` options control the background lifecycle (set at `CREATE INDEX`); `0` disables each:

| Option | Default | Controls |
|---|---|---|
| `refresh_interval` | `1000` | Milliseconds between automatic refreshes |
| `compaction_interval` | `1000` | Milliseconds between automatic segment compactions |
| `cleanup_interval_step` | `1` | Commit ticks between cleanup passes |

View-backed indexes have a fourth interval, `reindex_interval`, which re-scans the view's *source* for new, changed and removed data — see [Refreshing the index](./views.md#refreshing-the-index).

## Manual maintenance with `VACUUM`

Each operation comes in a family scoped to a single index, a table, a schema, a database, or instance-wide (`*_ALL`, which takes no argument). At most one maintenance option may appear per `VACUUM` statement.

| Operation | Forms | Effect |
|---|---|---|
| Refresh | `REFRESH_INDEX` · `REFRESH_TABLE` · `REFRESH_SCHEMA` · `REFRESH_DATABASE` · `REFRESH_ALL` | Publish pending writes to readers |
| Compact | `COMPACT_INDEX` · `COMPACT_TABLE` · `COMPACT_SCHEMA` · `COMPACT_DATABASE` · `COMPACT_ALL` | Merge segments to reclaim space and speed queries |
| Recompute statistics | `RECOMPUTE_STATS_COLUMN` · `RECOMPUTE_STATS_TABLE` · `RECOMPUTE_STATS_SCHEMA` · `RECOMPUTE_STATS_DATABASE` · `RECOMPUTE_STATS_ALL` | Rebuild the term statistics used by relevance scoring and planning |

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_002" />

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_003" />

Recompute statistics after large changes in data distribution so that [relevance scores](./ranking.md) and planning stay accurate.

## Rebuilding

A [view- or external-data-backed index](./views.md) is a static snapshot taken at `CREATE INDEX` time — it does not track later changes to its source. To pick up new data, rebuild it with `DROP INDEX` followed by `CREATE INDEX`.

## Schema changes on an indexed table

An inverted index pins every column it reads, including columns reached only through an [indexed expression](./modeling.md#indexing-expressions) — for example a struct sub-field such as `(s['id']::INTEGER)` pins the whole `s` column.

- **`RENAME` column, table or index** — allowed; the index keys columns by id and follows the rename automatically.
- **`ADD COLUMN`** — allowed; it is index-neutral (existing rows are backfilled).
- **`ALTER COLUMN … TYPE`** on a pinned column — rejected; the index stores values of the old type. Drop the index first, change the type, then recreate it.
- **`ADD` / `DROP` / `RENAME` of a struct field** on a pinned column — rejected for the same reason, even when the indexed expression targets a *different* sub-field (the whole column is pinned). Drop the index first.
- **`DROP COLUMN`** on a pinned column — allowed; it cascade-drops every index that covers the column.

## Performance

Tuning is mostly about the background cadence and segment layout; use only the options that exist:

- **Refresh vs. compaction cadence** — lower `refresh_interval` for fresher results, raise it (or `0`) to reduce overhead on write-heavy tables; `compaction_interval` / `cleanup_interval_step` govern how aggressively segments merge.
- **Row-group sizes** — `row_group_size` (stored `INCLUDE`d columns) and `norm_row_group_size` (norm columns) control the columnstore batch size.
- **Build then index** — for a bulk load, create the table, load the data, then create the index; this produces a more compact index than loading into an already-indexed table.
- **Top-K** — set [`optimize_top_k`](./ranking.md#top-k-queries-and-wand-pruning) to accelerate `ORDER BY <scorer> … LIMIT k`.

## Session settings {#session-settings}

Beyond the per-index `WITH` options, a few **`sdb_`-prefixed session settings** tune search at query time. Set them per connection with `SET` (and restore with `RESET`); they affect only the current session:

| Setting | Default | Effect |
| :--- | :--- | :--- |
| `sdb_disable_top_k_optimization` | `false` | When `true`, the optimizer does **not** pull `ORDER BY <scorer> DESC LIMIT k` into the index scan, so [WAND top-K pruning](./ranking.md#top-k-queries-and-wand-pruning) never engages. Useful to A/B the optimization or work around a plan regression. |
| `sdb_scored_terms_limit` | `1024` | Maximum number of terms considered for scoring in multi-term filters. Higher values give more accurate IDF-style [scoring](./ranking.md) at the cost of memory and per-query work; `0` disables scored-term collection entirely. |
| `sdb_levenshtein_max_terms` | `64` | Maximum number of dictionary terms a fuzzy predicate ([`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein)) expands to, per index segment. The terms closest to the query survive; the rest neither match nor contribute to scoring. Raise it for wide expansions, or set `0` to match every term within the edit distance. A predicate on a column that a `ts_dict_*` query enumerates is exempt, since there the terms are the result; other predicates in the same query keep the cap. |
| `sdb_nprobe` | `8` | Number of IVF cluster lists scanned per [vector](./vector-search.md) kNN query (`ORDER BY <dist> LIMIT k`). Higher = better recall, slower queries. Does not affect range (`WHERE <dist> < r`) queries. |
| `sdb_rerank_factor` | `4` | For a quantized (`quant` other than `none`) [vector](./vector-search.md#quantization) index, the candidate pool re-scored with exact distances is `sdb_rerank_factor * k`. Higher = better recall, slower queries; `0` disables reranking. Ignored for unquantized indexes. |

```sql
SET sdb_nprobe = 32;             -- scan more clusters for this session
SET sdb_rerank_factor = 8;       -- widen the exact-rerank pool for a quantized index
SET sdb_scored_terms_limit = 4096;
SET sdb_levenshtein_max_terms = 256; -- widen fuzzy expansion
-- … run queries …
RESET sdb_nprobe;                -- back to the default
```

## Introspection

List the inverted indexes with the `duckdb_indexes()` table function:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_004" />

The standard `pg_indexes` view also lists them (alongside the primary-key index), with the `CREATE INDEX` statement in its `indexdef` column:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_005" />

<DocCallout type="attention">

`pg_indexes_size()` is not yet meaningful for inverted indexes — it returns `0`. Use `duckdb_indexes()` / `pg_indexes` for index metadata.

</DocCallout>

### Per-index statistics in `sdb_metrics` {#sdb-metrics}

The `sdb_metrics` system table exposes live runtime metrics. Most rows are process-wide gauges (connection counts, active/pending maintenance tasks) and carry a `NULL` `relation_id`. In addition, **each inverted index contributes a row per statistic**, tagged with the index's oid in the `relation_id` column — describing its physical state and the health of its background maintenance.

| Column | Description |
| :--- | :--- |
| `metric` | Metric name |
| `value` | Current value (`UBIGINT`) |
| `description` | Human-readable description |
| `relation_id` | Oid of the inverted index the row describes (an index is a relation in `pg_class`), or `NULL` for a process-wide gauge |

The per-index metrics, modelled on the statistics an ArangoSearch / IResearch data store reports:

| Metric | Meaning |
| :--- | :--- |
| `num_docs` | Documents in committed segments, **including** deleted-but-not-yet-compacted |
| `num_live_docs` | Live (non-deleted) documents; `num_docs − num_live_docs` is the deleted backlog awaiting compaction |
| `num_buffered_docs` | Documents written but not yet committed — still in memory, not searchable (see the [refresh model](#visibility-and-the-refresh-model)) |
| `num_segments` | On-disk segments |
| `num_files` | Files backing the index |
| `index_size` | On-disk index size, in bytes |
| `num_failed_commits` | Commit (refresh) operations that failed |
| `num_failed_cleanups` | Cleanup operations that failed |
| `num_failed_consolidations` | Consolidation (compaction) operations that failed |
| `avg_commit_time_ms` | Average duration of the last few commits, in ms |
| `avg_cleanup_time_ms` | Average duration of the last few cleanups, in ms |
| `avg_consolidation_time_ms` | Average duration of the last few consolidations, in ms |

The `avg_*` values are a rolling average over a small window of the most recent successful operations. Rows cover the inverted indexes in the **current database**. Join `pg_class` to resolve the index name, or `pg_index` to reach the base table:

```sql
-- Per-index statistics, with index names
SELECT c.relname AS index, m.metric, m.value
FROM sdb_metrics m
JOIN pg_class c ON c.oid = m.relation_id
WHERE m.relation_id IS NOT NULL
ORDER BY c.relname, m.metric;

-- Statistics for the indexes on one table
SELECT m.metric, m.value
FROM sdb_metrics m
JOIN pg_index i ON i.indexrelid = m.relation_id
JOIN pg_class t ON t.oid = i.indrelid
WHERE t.relname = 'my_table'
ORDER BY m.metric;
```

A persistently high `num_buffered_docs`, a growing `num_failed_*`, or a climbing `avg_commit_time_ms` are signs that maintenance is falling behind the write rate — see [Background intervals](#background-intervals) and [Performance](#performance) for the knobs.

## See also

- [Inverted Index](./index.md) · [Ranking](./ranking.md)
- [`VACUUM`](../../statements/vacuum/index.md) — full statement reference
- [`CREATE INDEX … USING inverted`](../../statements/create_index/inverted.md) — index `WITH` options
