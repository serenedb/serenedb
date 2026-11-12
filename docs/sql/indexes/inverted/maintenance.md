---
title: Maintenance & Introspection
sidebar_position: 12
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

A search reads the snapshot of its transaction like any other read, full-text matches with `@@`, scores and vector search included: it finds the rows committed before that snapshot was taken and the transaction's own writes. An inverted index over a view makes its rows searchable when `CREATE INDEX` or a [`REINDEX`](./views.md#refreshing-the-index) pass finishes. Segments are *compacted* in the background to keep queries fast. This page covers that lifecycle and how to inspect an index.

```mermaid
flowchart LR
    w["INSERT / UPDATE / DELETE"] -->|commit| idx["visible to later snapshots, @@ included"]
    idx -->|compaction| merged["fewer, larger segments"]
```

## Visibility

A committed row is found by every snapshot taken after its commit, through the table and through each inverted index on it; a `REPEATABLE READ` transaction that took its snapshot earlier does not see it (see [Isolation Level](../../statements/transactions.md#isolation-level)). A transaction indexes its rows when it commits, so a search right after writes costs the same as any other search.

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_001" />

## Background intervals

`compaction_interval` controls the background compaction of a table (set at `CREATE TABLE ... WITH`) and of an inverted index on a view (set at `CREATE INDEX ... WITH`); `0` disables it. See [Background compaction](#background-compaction).

View-backed indexes have a second interval, `reindex_interval`, which re-scans the view's *source* for new, changed and removed data -- see [Refreshing the index](./views.md#refreshing-the-index).

An inverted index on a table has no storage or background tasks of its own: the table's options run compaction for every index on it. Set them on the table, in `CREATE TABLE ... WITH` or with [`ALTER TABLE ... SET`](../../statements/alter_table/index.md#set--reset-table-options), whether or not it has indexes. `CREATE INDEX ... WITH` and `ALTER INDEX ... SET` reject them for an index on a table. They also reject the options that only `CREATE TABLE ... WITH` sets on a table: `row_group_size`, `block_size`, [`segment_memory_max`](#write-memory) and `optimize_top_k`. `segment_docs_max` doesn't apply to such an index and is rejected too.

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_007" />

## Background compaction

Compaction is on by default, for tables and inverted indexes on views alike: every `compaction_interval` milliseconds a background task merges segments of similar size into larger ones, so a table loaded in many small batches does not keep one segment per batch. Three more `WITH` options shape each merge:

| Option | Default | Controls |
|---|---|---|
| `compaction_max_segments` | `10` | Segments merged by one background compaction |
| `compaction_max_segments_bytes` | `5368709120` (5 GB) | Size budget of one background compaction |
| `compaction_floor_segment_bytes` | `2097152` (2 MB) | Segments smaller than this count as equal-sized when choosing what to merge |

The session settings of the same names hold the defaults a new table or index takes.

The same background task settles a table that received no writes since its previous run: it writes the rows the table holds in memory to segments and merges them, so a table left alone ends up in a few large segments.

A table deletes the files of the segments it no longer uses as soon as no running query or statement reads them, whether or not compaction is enabled: those a merge, a rebuild or a write of in-memory rows replaced, and those a rolled-back transaction wrote. While a [backup](../../../operations/backup.md) runs, they stay until it ends.

While a table receives writes, compaction keeps the bytes it rewrites low: a merge never takes a segment larger than all the others it merges together, so small segments merge with each other until they reach the size of the large ones, and a segment is rewritten on its own to drop its deleted rows only once a fifth of its rows are deleted. A steady stream of writes therefore does not keep rewriting large segments. A table left alone merges its segments without these limits and drops all of its deleted rows.

- **Disable** background compaction with `compaction_interval = 0`, in `WITH` at creation or later with [`ALTER TABLE ... SET`](../../statements/alter_table/index.md#set--reset-table-options) for a table and `ALTER INDEX ... SET` for an index on a view. Segments are then merged only when you ask for it, the table is not settled in the background, and the rows it holds in memory reach its segments when they grow to [`segment_memory_max`](#write-memory), at a checkpoint or with `VACUUM (FLUSH_TABLE)`.
- **Enable** it again, or change how often it runs, the same way with a non-zero interval. `RESET (compaction_interval)` returns to the session default. The change applies to the running background task at once.
- **Compact manually** at any time, whether background compaction is enabled or not, with `VACUUM (COMPACT_INDEX)` or `VACUUM (COMPACT_TABLE)` -- see [below](#manual-maintenance-with-vacuum).

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_006" />

## Manual maintenance with `VACUUM`

Each operation comes in a family scoped to a single index, a table, a schema, a database, or instance-wide (`*_ALL`, which takes no argument). At most one maintenance option may appear per `VACUUM` statement.

| Operation | Forms | Effect |
|---|---|---|
| Flush | `FLUSH_INDEX` · `FLUSH_TABLE` · `FLUSH_SCHEMA` · `FLUSH_DATABASE` · `FLUSH_ALL` | Write the rows a table holds in memory to its on-disk segments |
| Compact | `COMPACT_INDEX` · `COMPACT_TABLE` · `COMPACT_SCHEMA` · `COMPACT_DATABASE` · `COMPACT_ALL` | Merge segments to reclaim space and speed queries |

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_002" />

The [relevance scores](./ranking.md) and planner statistics of a table and its indexes follow the rows as they are written and compacted, so they never need recomputing.

## Refreshing view-backed indexes

A [view- or external-data-backed index](./views.md) holds a snapshot of its source and does not track later changes on its own. `REINDEX INDEX <name>` refreshes it in one pass, incrementally for Iceberg tables and file globs, and the `reindex_interval` index option repeats that pass in the background. See [Refreshing the index](./views.md#refreshing-the-index).

## Schema changes on an indexed table

An inverted index pins every column it reads, including columns reached only through an [indexed expression](./modeling.md#indexing-expressions) -- for example a struct sub-field such as `(s['id']::INTEGER)` pins the whole `s` column:

- **`RENAME` column, table or index** -- allowed; the index follows the rename.
- **`ADD COLUMN`** -- allowed; the index is unaffected.
- **`ALTER COLUMN ... TYPE`** on a pinned column -- rejected with `Cannot change the type of column "⟨column⟩": index "⟨index⟩" depends on it`; the index stores values of the old type. Drop the index first, change the type, then recreate it.
- **`ADD` / `DROP` / `RENAME` of a struct field** on a pinned column -- rejected the same way, even when the indexed expression targets a *different* sub-field (the whole column is pinned). Drop the index first.
- **`DROP COLUMN`** on a pinned column -- allowed; it drops every index that covers the column.
- **`DROP COLUMN`** on any other column -- allowed, including one that comes before a pinned column.

See [Rewrites and concurrent changes](../../statements/alter_table/index.md#rewrites-and-concurrent-changes) in the `ALTER TABLE` reference for how these statements wait for writers.

## Performance

Tuning is mostly about the background cadence and segment layout; use only the options that exist:

- **Compaction cadence** -- `compaction_interval` governs how aggressively segments merge; the files of the segments a merge replaces are deleted once nothing reads them.
- **Row-group size** -- `row_group_size` sets how many rows the columnstore of a table, or of an index on a view, groups together, for norm columns too; norms share this one setting. It must be a multiple of the vector size (2048), and it is also the unit a scan hands to one worker: a table whose segments hold few row groups cannot spread a scan over more threads than it has row groups, so lower it when rows are expensive to materialise and the table is small. It is fixed when the table, or the index on a view, is created.
- **Block size** -- `block_size` sets the size in bytes of the blocks a table's columns are compressed in: a power of two from 16384 to 262144 (the default). It is fixed at `CREATE TABLE`, and its default comes from the database's `BLOCK_SIZE` and then the `default_block_size` setting.
- **Build then index** -- for a bulk load, create the table, load the data, then create the index; this produces a more compact index than loading into an already-indexed table.
- **Whole-relation aggregates** -- `count(*)` over a table or an index relation (`FROM my_index`) without a `WHERE` clause is answered from the document counts, without reading any document. `min` and `max` of numeric and temporal `INCLUDE`d columns and of keyword columns are answered the same way while the relation holds no deleted documents; deleted documents are dropped by compaction, and until then `min` and `max` read the documents. `EXPLAIN` shows no index scan for an aggregate answered this way.
- **Top-K** -- set [`optimize_top_k`](./ranking.md#top-k-queries-and-wand-pruning) to accelerate `ORDER BY <scorer> ... LIMIT k`.
- **Write memory** -- [`segment_memory_max`](#write-memory) sets the segment size a write produces and what the write holds in memory.

## Write memory {#write-memory}

A table holds its committed rows in memory, indexed and searchable, together with the deletes and in-place updates of the rows on its segments, until they reach `segment_memory_max`, set in the table's `CREATE TABLE ... WITH (segment_memory_max = ...)` clause and defaulting to 256 MB. It then frees memory in the background: it writes its rows to disk as one segment when they take most of it, and otherwise rewrites the segment holding the most deletes and updates, without its deleted rows. A write also holds up to `compaction_floor_segment_bytes` of its own rows, 2 MB by default, before it indexes them: below that size a segment of their own would be one compaction merges right away; a write with more rows writes them to disk as segments of its own. A write that commits while its table already holds `segment_memory_max` in memory, because the background write is still running, writes its rows to disk as a segment of its own too, so the memory a table holds stays near its limit. `segment_memory_max` is below 4 GiB. Estimate the peak for one writing transaction as:

| Write | Peak memory |
| :--- | :--- |
| Serial `INSERT` / `UPDATE` | ≈ `segment_memory_max` + `compaction_floor_segment_bytes` |
| Parallel `INSERT` / `UPDATE` | ≈ `segment_memory_max` x threads |

Lower `segment_memory_max` to cap that peak, raise it for larger segments and less compaction work.

```sql
CREATE TABLE docs (id BIGINT, body TEXT)
  WITH (segment_memory_max = 134217728);  -- 128 MB
```

<DocCallout type="tip">

`DELETE` sits outside this bound, but costs little: until the removal is applied, a removed row takes at most a few bytes and a run of consecutive rows almost none, so even a million-row `DELETE` stays within a few megabytes.

</DocCallout>

## Durability {#durability}

A commit is durable once the server's write-ahead log holds it; one log serves every database. Once the log written since the last snapshot reaches [`max_wal_size`](../../../configuration/overview.md) (1 GB by default), or the size of the last snapshot if that is larger, the server writes a new snapshot of the catalog and of every table, with the rows, deletes and updates they hold in memory, and removes the log files the snapshot covers, while writes go on. `CHECKPOINT` takes a snapshot at once. After a crash, the server reads the snapshot and replays the log written after it. It also lists every table's directory and removes the files that no committed transaction refers to, such as those of writes the crash cut short; after a clean shutdown it starts without listing them.

A server left idle settles on its own: once every table has written the rows it held in memory to segments (see [Background compaction](#background-compaction)) and nothing commits, the server removes the log files it no longer needs, so the log stays small and a restart replays little.

A query sees every row its snapshot sees, including the rows a table still holds in memory, and reads them there, searches included: a query never writes. After a crash, the server replays those rows from the log.

## Session settings {#session-settings}

Beyond the per-index `WITH` options, a few **`sdb_`-prefixed session settings** tune search at query time. Set them per connection with `SET` (and restore with `RESET`); they affect only the current session:

| Setting | Default | Effect |
| :--- | :--- | :--- |
| `sdb_disable_top_k_optimization` | `false` | When `true`, the optimizer does **not** pull `ORDER BY <scorer> DESC LIMIT k` into the index scan, so [WAND top-K pruning](./ranking.md#top-k-queries-and-wand-pruning) never engages. Useful to A/B the optimization or work around a plan regression. |
| `sdb_levenshtein_max_terms` | `50` | Maximum number of dictionary terms a fuzzy predicate ([`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein)) expands to, per index segment. The terms closest to the query survive; the rest neither match nor contribute to scoring. Raise it for wide expansions, or set `0` to match every term within the edit distance. A predicate on a column that a `ts_dict_*` query enumerates is exempt, since there the terms are the result; other predicates in the same query keep the cap. |
| `sdb_ivf_search_nprobe` | `-1` | Number of IVF cluster lists each segment scans per [vector](./vector-search.md) kNN query (`ORDER BY <dist> LIMIT k`). Higher = better recall, slower queries. `-1` lets the engine choose per segment, `1.3 · log10(max(LIMIT, 10)) · √lists` with `lists` the segment's rows over the index's posting size, which keeps recall near 0.95 at any `LIMIT` and segment size. Does not affect range (`WHERE <dist> < r`) queries. |
| `sdb_ivf_max_search_fanout` | `-1` | Upper bound on the beam of the descent through a multi-level IVF centroid tree (large segments, or a small `sdb_ivf_posting_size`), where each level keeps the beam's worth of its nearest centroids and scores only their children. The beam follows `sdb_ivf_search_nprobe` as the query resolves it; a narrower one scores fewer centroids at some recall cost. Applied after `sdb_ivf_min_search_fanout`, so setting both to one value fixes the beam. `-1` leaves it unbounded. Does not affect range queries. |
| `sdb_ivf_min_search_fanout` | `-1` | Lower bound on the same beam. A wider one reaches clusters a narrow descent misses, and one as wide as the segment's list count makes the descent exact. `-1` leaves it unbounded. Does not affect range queries. |
| `sdb_hnsw_ef_search` | `-1` | Beam width of each segment's search for an [HNSW](./vector-search.md#hnsw) kNN query. Higher = better recall, slower queries. A value is used as written, floored only at the share of the query's `LIMIT` one segment is expected to hold. `-1` lets the engine choose: the index's `ef_construction` or the `LIMIT`, whichever is larger, which keeps recall high at any `LIMIT` without tuning. |
| `sdb_hnsw_filter_mode` | `auto` | How an [HNSW](./vector-search.md#filtered-search) index answers a kNN query with a `WHERE`: `auto` picks by the predicate's estimated selectivity and, where codes are long or the rows near the query are mostly rejected, walks as `twohop`, `scan` scores every admitted row, and `walk`, `prune`, `twohop` and `bridge` force a walk shape. For measurement -- `auto` is the one to leave alone. |
| `sdb_hnsw_column_filter` | `auto` | How an [HNSW](./vector-search.md#filtered-search) walk answers a predicate the columnstore can evaluate: `fold` evaluates one on columns the index does not index (`INCLUDE`d ones) over the whole segment before the walk, and answers one on indexed columns from the index; `read` reads the columns of each row the walk reaches, for indexed columns of a table too; `auto` reads while the walk is expected to reach few enough rows for that to cost less than the fold, or than the index for an indexed column, pricing a read by the columns' encodings. For measurement -- `auto` is the one to leave alone. |
| `sdb_ann_rescore_width` | `-1` | For a quantized (`quant` other than `none`) [vector](./vector-search.md#rescoring) index, how far a candidate's code score may be off and still have it re-read at full precision: candidates from every segment are re-scored while their code score plus this many times the code's estimated error could still beat the `k`-th best exact score. `0` answers from the codes alone; `-1` uses a width per quantizer, listed under [Re-scoring](./vector-search.md#rescoring). Ignored for unquantized indexes. |
| `sdb_ann_force_exact` | `false` | Skip the ANN walk and return the exact answer, for a nearest-`k` query and a distance range alike. Every row with a vector is considered; an `hnsw` index's 8-bit codes (`sq8`, `usq8`) rule out most rows before their stored vectors are read. Useful to measure recall. `false` leaves the choice to the engine, which may still answer a selective filter with an exact scan. |
| `sdb_scan_split` | `auto` | How an index scan shares a segment between worker threads. Each worker scans the row groups of a segment in order. `tail` gives every worker a segment of its own; only when no segment is left unclaimed does an idle worker join the segment with the most row groups left, so a segment is split across workers only at the end of the scan, when there are fewer segments than workers, or when one segment holds most of the data. `always` puts every worker on the same segment until all of its row groups are claimed; `never` scans each segment whole on one worker. `auto` is `always` when the query has an `ORDER BY <column> LIMIT` scan order and `tail` otherwise. Meant for benchmarking and tests. |
| `sdb_scan_order` | `auto` | The order an index scan claims its segments in. `largest_first` and `smallest_first` order them by live document count; `order` is best-first by the `ORDER BY` column's row-group statistics when the query has a scan order, so the `TOP_N` bound tightens early. `auto` is `order` under a scan order and `largest_first` otherwise. |
| `sdb_scan_no_split_row_groups` | `1` | A segment with at most this many row groups is always one unit of an index scan and is never split across workers. |
| `sdb_compact_target_segments` | `1` | How many segments `VACUUM (COMPACT_*)` leaves in a table. `0` or `1` merges every segment into one; a larger `N` splits the segments into `N` disjoint groups and merges each into one segment, so no single merge holds the whole table. A table with `N` segments or fewer is left as it is. |

```sql
SET sdb_ivf_search_nprobe = 32;  -- scan more IVF clusters for this session
SET sdb_hnsw_ef_search = 256;    -- widen the HNSW beam
SET sdb_ann_rescore_width = 3;   -- re-score more candidates of a quantized index
SET sdb_levenshtein_max_terms = 256; -- widen fuzzy expansion
-- ... run queries ...
RESET sdb_ivf_search_nprobe;     -- back to the default
```

## Introspection

List the inverted indexes with the `sdb_indexes()` table function:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_004" />

The standard `pg_indexes` view also lists them (alongside the primary-key index), with the `CREATE INDEX` statement in its `indexdef` column:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_005" />

<DocCallout type="info">

An inverted index on a table has no storage of its own, so `pg_relation_size()` and `pg_indexes_size()` count it as `0`; its data is part of `pg_relation_size()` of the table. `pg_relation_size()` of an inverted index on a view returns its size on disk, and `pg_indexes_size()` of a view returns `0`.

</DocCallout>

### Statistics in `sdb_metrics` {#sdb-metrics}

The `sdb_metrics` system table exposes live runtime metrics. Most rows are process-wide gauges (connection counts, active/pending maintenance tasks) and carry a `NULL` `relation_id`. In addition, **each table and each inverted index on a view contributes a row per statistic**, tagged with its oid in the `relation_id` column -- describing its physical state and the health of its background maintenance. An inverted index on a table has no storage of its own and contributes no rows: the table's rows cover its indexes.

| Column | Description |
| :--- | :--- |
| `metric` | Metric name |
| `value` | Current value (`UBIGINT`) |
| `description` | Human-readable description |
| `relation_id` | Oid of the table or inverted index the row describes (a relation in `pg_class`), or `NULL` for a process-wide gauge |

The metrics of each table and inverted index:

| Metric | Meaning |
| :--- | :--- |
| `num_docs` | Documents in committed segments, **including** deleted-but-not-yet-compacted |
| `num_live_docs` | Live (non-deleted) documents; `num_docs - num_live_docs` is the deleted backlog awaiting compaction |
| `num_buffered_docs` | Committed documents held in memory, not yet written to a segment |
| `num_segments` | On-disk segments |
| `num_files` | Files backing the index |
| `index_size` | On-disk index size, in bytes |
| `num_failed_cleanups` | Cleanup operations that failed |
| `num_failed_consolidations` | Consolidation (compaction) operations that failed |
| `avg_cleanup_time_ms` | Average duration of the last few cleanups, in ms |
| `avg_consolidation_time_ms` | Average duration of the last few consolidations, in ms |

The `avg_*` values are a rolling average over a small window of the most recent successful operations. Rows cover the tables and inverted indexes in the **current database**. Join `pg_class` to resolve the name. To group the rows by relation, join `pg_index` as well: an index's rows reach its view through `indrelid`, while a table's rows carry the table's own oid. Here `my_logs` is a table with the inverted index `logs_idx`, and the rows just inserted into `my_events` are still held in memory:

<SqlLogicTest id="sql/indexes/inverted/maintenance/sdb_metrics" />

A growing `num_failed_*` or a climbing `avg_consolidation_time_ms` are signs that maintenance is falling behind the write rate -- see [Background intervals](#background-intervals) and [Performance](#performance) for the knobs.

## See also

- [Inverted Index](./index.md) · [Ranking](./ranking.md)
- [`VACUUM`](../../statements/vacuum/index.md) -- full statement reference
- [`CREATE INDEX ... USING inverted`](../../statements/create_index/inverted.md) -- index `WITH` options
