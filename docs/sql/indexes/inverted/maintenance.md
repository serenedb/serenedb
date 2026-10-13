---
title: Maintenance & Introspection
sidebar_position: 12
split: headings
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

- **Automatically** -- a background thread refreshes each index on an interval (`refresh_interval`, default 1000 ms).
- **Explicitly** -- run `VACUUM (REFRESH_TABLE)` to publish pending writes immediately. This is what you want right after a bulk load, before querying:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_001" />

## Background intervals

Three `WITH` options control the background lifecycle of an inverted index (set at `CREATE INDEX`) and of a search table (set at `CREATE TABLE ... WITH (storage = 'search', ...)`); `0` disables each:

| Option | Default | Controls |
|---|---|---|
| `refresh_interval` | `1000` | Milliseconds between automatic refreshes |
| `compaction_interval` | `1000` | Milliseconds between automatic segment compactions |
| `cleanup_interval_step` | `1` | Commit ticks between cleanup passes |

View-backed indexes have a fourth interval, `reindex_interval`, which re-scans the view's *source* for new, changed and removed data -- see [Refreshing the index](./views.md#refreshing-the-index).

An inverted index on a search table has no storage or background tasks of its own: the table's options run refresh and compaction for every index on it. Set them on the table, in `CREATE TABLE ... WITH` or with [`ALTER TABLE ... SET`](../../statements/alter_table/index.md#set--reset-storage-options), whether or not it has indexes. `CREATE INDEX ... WITH` and `ALTER INDEX ... SET` reject them for an index on a search table, and `segment_docs_max` too.

## Background compaction

Compaction is on by default, for inverted indexes and search tables alike: every `compaction_interval` milliseconds a background task merges segments into larger ones, so a table loaded in many small batches does not keep one segment per batch. Four more `WITH` options shape each merge:

| Option | Default | Controls |
|---|---|---|
| `compaction_policy` | `tier` | How segments are picked: `tier` or `adjacent`, see below |
| `compaction_max_segments` | `10` | Segments merged by one background compaction |
| `compaction_max_segments_bytes` | `5368709120` (5 GB) | Size budget of one background compaction |
| `compaction_floor_segment_bytes` | `2097152` (2 MB) | Segments smaller than this count as equal-sized when choosing what to merge |

The session settings of the same names hold the defaults a new index or search table takes.

`compaction_policy = 'tier'` merges segments of similar size, wherever they are in the table. `compaction_policy = 'adjacent'` merges runs of neighbouring segments in the order their rows were written, so every segment holds a contiguous range of rows and a filter on insertion order skips whole segments:

- A run merges only when it is balanced: together its segments are at least five times the size of the largest one, with `compaction_floor_segment_bytes` added to the size of every segment. Small segments therefore merge in groups of five or more, and each row is rewritten a few times however large the table grows.
- Among the balanced runs of at most `compaction_max_segments` segments and `compaction_max_segments_bytes` bytes, the one that rewrites the fewest bytes for each segment it removes goes first; on a tie, the newest.
- When merges fall behind and more than ten neighbouring segments wait, the balance rule relaxes step by step, down to twice the largest segment at fifty.
- A segment with more than a quarter of its rows deleted is rewritten on its own when nothing else is to merge.

A segment holds at most 2,147,418,110 rows. A merge whose segments hold more live rows than that together is refused and logged, and its segments stay as they are; this applies to background compaction, `VACUUM` and [`sdb_compact`](#sdb-compact) alike.

- **Disable** background compaction with `compaction_interval = 0`, in `WITH` at creation or later with `ALTER INDEX ... SET` for an index and [`ALTER TABLE ... SET`](../../statements/alter_table/index.md#set--reset-storage-options) for a search table. Segments are then merged only when you ask for it.
- **Enable** it again, or change how often it runs, the same way with a non-zero interval. `RESET (compaction_interval)` returns to the session default. The change applies to the running background task at once.
- **Compact manually** at any time, whether background compaction is enabled or not, with `VACUUM (COMPACT_INDEX)` or `VACUUM (COMPACT_TABLE)` -- see [below](#manual-maintenance-with-vacuum).

`CREATE INDEX` on a search table stops the merges of the table that are in progress, because the index build rewrites their segments anyway. Segments that existed when the build started are not merged until it finishes; segments written during the build merge as usual.

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_006" />

## Manual maintenance with `VACUUM`

Each operation comes in a family scoped to a single index, a table, a schema, a database, or instance-wide (`*_ALL`, which takes no argument). At most one maintenance option may appear per `VACUUM` statement.

| Operation | Forms | Effect |
|---|---|---|
| Refresh | `REFRESH_INDEX` · `REFRESH_TABLE` · `REFRESH_SCHEMA` · `REFRESH_DATABASE` · `REFRESH_ALL` | Publish pending writes to readers |
| Compact | `COMPACT_INDEX` · `COMPACT_TABLE` · `COMPACT_SCHEMA` · `COMPACT_DATABASE` · `COMPACT_ALL` | Merge segments to reclaim space and speed queries |
| Optimize | `OPTIMIZE_INDEX` · `OPTIMIZE_TABLE` · `OPTIMIZE_SCHEMA` · `OPTIMIZE_DATABASE` · `OPTIMIZE_ALL` | Merge segments into the shape that serves queries best: about one segment per query thread, contiguous, balanced and without deleted rows (see [VACUUM](../../statements/vacuum/index.md)) |
| Recompute statistics | `RECOMPUTE_STATS_COLUMN` · `RECOMPUTE_STATS_TABLE` · `RECOMPUTE_STATS_SCHEMA` · `RECOMPUTE_STATS_DATABASE` · `RECOMPUTE_STATS_ALL` | Rebuild the term statistics used by relevance scoring and planning |

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_002" />

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_003" />

Recompute statistics after large changes in data distribution so that [relevance scores](./ranking.md) and planning stay accurate.

## Merging on demand and explaining merges {#sdb-compact}

`CALL sdb_compact(relation [, mode := ...])` merges segments of a search table or an inverted index right away and returns one row per merge. `EXPLAIN CALL sdb_compact(...)` shows which merges it would run without running them, and `EXPLAIN ANALYZE CALL sdb_compact(...)` runs them and adds how long each merge step took.

| `mode` | Merges |
|---|---|
| `background` (default) | What one round of background compaction would merge, following the table's `compaction_*` options, even when background compaction is disabled |
| `compact` | What `VACUUM (COMPACT_TABLE)` or `VACUUM (COMPACT_INDEX)` would merge: `target_segments` groups for a search table (default: the `sdb_compact_target_segments` setting), one segment for an index |
| `segments` | Exactly the segments named in `segments := ['...', ...]`, as `sdb_segments` lists them |
| `optimize` | The shape that serves queries best: about as many segments as the server has query threads, each a contiguous range of rows, balanced by live documents and without deleted documents. A small relation gets fewer, so that every segment holds at least four row groups (`row_group_size`) of rows; `target_segments` sets the count instead. A segment with deletes is rewritten even when the relation already has no more segments than the target, and so is a segment whose columns are not encoded the way the current codec settings would encode them (see `verdict` in `sdb_segments` below); a second run finds nothing to do. Merges only combine segments, so a segment already larger than its share stays as it is |

Each result row describes one merge:

| Column | Meaning |
|---|---|
| `merge` | Number of the merge within the call |
| `outcome` | `merged`; `deferred` (merged, visible after the next commit); `nothing`; `busy` (a segment was being merged by another task); `failed`; `cancelled` (stopped before it finished, for example by a `TRUNCATE` or a `CREATE INDEX` on the table; its segments stay as they were) |
| `inputs`, `input_docs`, `input_live_docs`, `input_bytes` | The merged segments, their documents, live documents and bytes |
| `output`, `output_docs`, `output_bytes` | The new segment |
| `total_ms` | Wall-clock time of the merge |
| `prepare_ms`, `norms_ms`, `columns_ms`, `columns_commit_ms`, `ann_ms`, `postings_ms`, `commit_ms` | Time spent in each merge step, added up over the threads that ran it |
| `error` | Why a merge failed |

`EXPLAIN` prints the plan in a `SEGMENT_REWRITE` box, with one group of lines per merge:

| Line | Meaning |
|---|---|
| `Segments`, `Merges` | Segments of the relation and merges planned |
| `Merge N` | How many segments the merge combines into one, and why: `tier policy` or `adjacent policy` for `background`, `compact to N segments` for `compact`, `optimize to N segments` for `optimize`, `listed segments` for `segments`. An `optimize` that rewrites a single segment says `re-encoding saves N%` or `a declared column codec changed` |
| `Merge N Inputs` | The segments merged: the first eight, then `+N more` |
| `Merge N Docs` | Documents and live documents of the inputs, and the range of their row ids |
| `Merge N Bytes` | Size of the inputs and the estimated size of the new segment |
| `Merge N Columns` | Columns stored in the inputs, internal ones included: how many, their size and the largest one |
| `Merge N Postings` | Indexed fields whose postings the merge combines |
| `Merge N ANN` | Vector columns whose ANN index the merge builds |
| `Not Merged` | Segments left out, with the reason: `not selected by the tier policy` (or `adjacent policy`), `at or above the merge size cap` or `alone in its group without deletes` (for `optimize` followed by how much re-encoding would save) |
| `Notes` | Why less is merged: background compaction is disabled, the relation already has at most the target number of segments, or the only listed segment has no deletes |

`EXPLAIN ANALYZE` runs the merges and adds `Actual Outcome` (how many merges ended with each outcome), `Actual Written` (bytes of the new segments), one `Actual <step>` line per merge step and `Actual Total`.

A merge runs as separate pieces of work: the norms of each indexed field, one after another, then every column and the postings of all indexed fields side by side, then the column file's footer, the vector indexes one at a time, and finally the index files. `CALL sdb_compact` and `VACUUM (COMPACT_*)` run these pieces on as many threads as the session may use (the `threads` setting), so a merge of a wide table finishes sooner on more threads. With several threads the step times of a merge add up to more than its `total_ms`.

Background compaction runs on the server's background threads (`--background_threads`, see [Command Line Interface](../../../configuration/cli.mdx)) and never on the threads that run queries. Merges may use all of these threads but one, the *merge threads*:

- **One queue.** The background merges of all search tables and indexes wait in one queue. The merges that rewrite the fewest bytes for each segment they remove start first, and a merge gains priority the longer it waits.
- **Small merges.** A merge that rewrites less than 64 MB keeps one merge thread for itself, so it never waits behind large merges.
- **Large merges.** The limits below keep threads free for merges of other tables:
  - large merges together take at most all merge threads but one;
  - a single large merge takes about half of the background threads;
  - one table or index runs at most half as many large merges at a time as there are merge threads.
- **Within a table.** A table's merges do not wait for each other, so a long merge does not hold back merges of the table's newer segments.
- **Failures.** A merge that fails leaves its segments out of the next merges for a while, and that while grows with each failure in a row.
- **Drops.** Dropping a table, an index or a database discards its queued merges and stops its running ones within a moment, so its files are removed without waiting for those merges to finish.

`sdb_compact` needs the same privilege as `VACUUM` on the table (`MAINTAIN`). Its merges take effect at once: they are not part of the surrounding transaction, and `ROLLBACK` does not undo them. Cancelling the call or reaching `statement_timeout` stops it: the merge in progress is abandoned and its segments stay as they were, while merges that finished before remain. Like `VACUUM (COMPACT_*)`, a call that merges anything pauses the relation's background merges until it ends: those in progress stop and show as `cancelled`. `EXPLAIN` runs nothing and pauses nothing.

`sdb_segments(relation)` lists the segments of a search table or an inverted index: `segment`, `docs`, `live_docs`, `deleted_docs`, `bytes`, and `rowid_min` / `rowid_max`, the range of row ids a segment holds.

`sdb_segments(relation, columns := true)` lists the stored columns of every segment instead, internal ones included (`rowid` is the row id), one row each:

| Column | Meaning |
|---|---|
| `segment`, `column`, `type`, `rows`, `bytes` | The segment, the column, its type, its rows and the bytes its data takes |
| `codecs` | The codecs of the column's own blocks, in the order they first appear. For a list these hold where each list ends, for a struct there are none (`NULL`): the values live in the nested parts, which are not listed |
| `reencoded_bytes` | The estimated size of the column if it were encoded again with the current codec settings, as a merge would. A few row groups spread over the column are encoded on the side and compared with the bytes they take now; the estimate is kept as long as the segment is in use. `NULL` for nested columns, which are not checked |
| `verdict` | `re-encode` when re-encoding would save more than 10% of the column's bytes, or when the codec declared with `ALTER COLUMN ... SET COMPRESSION` is one the column's blocks do not use; `copy` otherwise; `NULL` when not checked |

The estimates cost a little encoding work the first time a segment is looked at. `sdb_compact(mode := 'optimize')` and `VACUUM (OPTIMIZE_*)` use the same verdicts to decide whether a segment that would otherwise stay as it is gets rewritten:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_009" />

Two system tables show the compaction of the current database's search tables and indexes, whichever task ran a merge (`origin`: `background`, `vacuum` or `sql`):

- `sdb_merges` lists the merges that run or wait in the background queue: `merge_id`, `relation_id`, `origin`, `state` (`queued` or `running`), `queued`, `started`, `elapsed_ms`, the inputs, and `estimated_bytes`, what the merge will write, estimated from the live share of its inputs. Background merges also show `lane` (`small` or `big`), `score` (bytes rewritten per segment removed; the lowest starts first) and, once running, `threads` (the threads the merge may use). A queued merge keeps its `merge_id` when it runs, and it becomes the `id` of the merge's `sdb_compact_history` row; a merge dropped from the queue, for example by a `VACUUM` on its table, leaves no history.
- `sdb_compact_history` keeps the segments the server wrote recently, one row each: the last 256 finished merges, and the last 8192 segments written by refreshes. `type` tells them apart:
  - `merge` rows have the columns of a `sdb_compact` row plus `id`, `relation_id`, `origin`, `started` and `finished`;
  - `flush` rows describe a segment a refresh published at `finished`: `id`, `relation_id`, `output`, `output_docs` and `output_bytes`. The columns that only describe a merge are `NULL`.

  Both kinds also show `output_live_docs`, the documents of the new segment still live when it was published, and `rowid_min` / `rowid_max`, the range of row ids it holds. The flush rows of a load show the order and sizes in which its data arrived.

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_007" />

### Replaying a load with `sdb_merge_simulate` {#sdb-merge-simulate}

`sdb_merge_simulate(trace [, ...])` replays the segments a load wrote and returns how background compaction would merge them, without reading or changing any table. The replay runs the server's own merge policy and merge queue on a simulated clock: a merge waits for a free merge thread, as it would in the background, and takes as long as rewriting its bytes needs at `merge_bytes_per_second` on each thread it holds. Cancelling the statement or reaching `statement_timeout` stops the replay.

The trace is a subquery with one row per written segment, its columns matched by name. The flush rows of `sdb_compact_history` fit as they are, so a load on this server is its own trace:

| Column | Meaning |
|---|---|
| `finished` | When the segment was written (required) |
| `output_docs`, `output_bytes` | Documents and bytes of the segment (required) |
| `output_live_docs` | Documents still live; all of them when the column is missing |
| `relation_id` | The table or index the segment belongs to; the segments of all relations share the merge threads |
| `rowid_min`, `rowid_max` | The range of row ids the segment holds |
| `type` | Rows whose `type` is not `flush` are skipped, so all of `sdb_compact_history` can be passed |

| Parameter | Default | Meaning |
|---|---|---|
| `policy` | The table default | The merge policy, `tier` or `adjacent`, as `compaction_policy` above |
| `threads` | The server's merge threads | Merge threads: on a server, `--background_threads` minus one |
| `merge_bytes_per_second` | `16777216` (16 MB) | Bytes one thread merges per second |
| `compaction_max_segments`, `compaction_max_segments_bytes`, `compaction_floor_segment_bytes` | The table defaults | The policy's options, as in `CREATE TABLE ... WITH` |

The result has one row per event, in time order: every replayed segment (`type` `flush`) and every simulated merge (`type` `merge`):

| Column | Meaning |
|---|---|
| `event` | Number of the event, from 1 in time order |
| `time_s` | Seconds since the first segment of the trace |
| `relation_id`, `type`, `segment` | The relation, the event and the name of the new segment |
| `inputs`, `input_bytes` | The merged segments and their bytes (merges only) |
| `docs`, `live_docs`, `bytes`, `rowid_min`, `rowid_max` | The new segment; a merge drops deleted documents and keeps the bytes of the live ones |
| `queued_s`, `started_s`, `threads`, `score`, `lane` | When a merge was queued and started, how many threads it had, and its score and lane as in `sdb_merges` |
| `segments`, `segment_bytes` | Segments and bytes of the relation after the event |
| `overlap` | How many of the relation's segments hold an average row id: `1` when their row-id ranges do not overlap |
| `merges_queued`, `merges_running` | Merges waiting and running after the event, all relations together |

Write amplification, how many times each written byte is rewritten by merges, is the merged bytes over the written ones. Here a load writes an 8 MB segment every second for 100 seconds and two threads merge:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_008" />

## Refreshing view-backed indexes

A [view- or external-data-backed index](./views.md) holds a snapshot of its source and does not track later changes on its own. `REINDEX INDEX <name>` refreshes it in one pass, incrementally for Iceberg tables and file globs, and the `reindex_interval` index option repeats that pass in the background. See [Refreshing the index](./views.md#refreshing-the-index).

## Schema changes on an indexed table

An inverted index pins every column it reads, including columns reached only through an [indexed expression](./modeling.md#indexing-expressions) -- for example a struct sub-field such as `(s['id']::INTEGER)` pins the whole `s` column.

- **`RENAME` column, table or index** -- allowed; the index keys columns by id and follows the rename automatically.
- **`ADD COLUMN`** -- allowed; it is index-neutral (existing rows are backfilled).
- **`ALTER COLUMN ... TYPE`** on a pinned column -- rejected; the index stores values of the old type. Drop the index first, change the type, then recreate it.
- **`ADD` / `DROP` / `RENAME` of a struct field** on a pinned column -- rejected for the same reason, even when the indexed expression targets a *different* sub-field (the whole column is pinned). Drop the index first.
- **`DROP COLUMN`** on a pinned column -- allowed; it cascade-drops every index that covers the column.
- **`DROP COLUMN`** on any other column -- allowed, including one that comes before a pinned column; the index keeps its columns by stable id.

## Performance

Tuning is mostly about the background cadence and segment layout; use only the options that exist:

- **Refresh vs. compaction cadence** -- lower `refresh_interval` for fresher results, raise it (or `0`) to reduce overhead on write-heavy tables; `compaction_interval` / `cleanup_interval_step` govern how aggressively segments merge.
- **Row-group size** -- `row_group_size` controls the columnstore batch size for stored (`INCLUDE`d) columns and for norm columns alike; norms share this one setting. It must be a multiple of the vector size (2048), and it is also the unit a scan hands to one worker: an index whose segments hold few row groups cannot spread a scan over more threads than it has row groups, so lower it when rows are expensive to materialise and the index is small. It is fixed at `CREATE INDEX` and applies to segments written afterwards.
- **Build then index** -- for a bulk load, create the table, load the data, then create the index; this produces a more compact index than loading into an already-indexed table.
- **Top-K** -- set [`optimize_top_k`](./ranking.md#top-k-queries-and-wand-pruning) to accelerate `ORDER BY <scorer> ... LIMIT k`.
- **Write memory** -- [`segment_memory_max`](#write-memory) bounds what a write holds in memory, and sets the segment size it produces.

## Write memory {#write-memory}

Memory held while writing to a search table is bounded by `segment_memory_max`, set in the table's `CREATE TABLE ... WITH (segment_memory_max = ...)` clause and defaulting to 256 MB. Estimate the peak for one writing transaction as:

| Write | Peak memory |
| :--- | :--- |
| Serial `INSERT` / `UPDATE` | ≈ 1.5 x `segment_memory_max` |
| Parallel `INSERT` | ≈ `segment_memory_max` x threads |

Lower it to cap what a write can hold, raise it for larger segments and less compaction work.

```sql
CREATE TABLE docs (id BIGINT, body TEXT)
  WITH (storage = 'search', segment_memory_max = 134217728);  -- 128 MB
```

<DocCallout type="tip">

`DELETE` sits outside this bound, but costs little: each removed row holds only its rowid and storage position until the removal is applied, so even a million-row `DELETE` stays within a few tens of megabytes.

</DocCallout>

## Session settings {#session-settings}

Beyond the per-index `WITH` options, a few **`sdb_`-prefixed session settings** tune search at query time. Set them per connection with `SET` (and restore with `RESET`); they affect only the current session:

| Setting | Default | Effect |
| :--- | :--- | :--- |
| `sdb_disable_top_k_optimization` | `false` | When `true`, the optimizer does **not** pull `ORDER BY <scorer> DESC LIMIT k` into the index scan, so [WAND top-K pruning](./ranking.md#top-k-queries-and-wand-pruning) never engages. Useful to A/B the optimization or work around a plan regression. |
| `sdb_levenshtein_max_terms` | `50` | Maximum number of dictionary terms a fuzzy predicate ([`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein)) expands to, per index segment. The terms closest to the query survive; the rest neither match nor contribute to scoring. Raise it for wide expansions, or set `0` to match every term within the edit distance. A predicate on a column that a `ts_dict_*` query enumerates is exempt, since there the terms are the result; other predicates in the same query keep the cap. |
| `sdb_ivf_search_nprobe` | `8` | Number of IVF cluster lists scanned per [vector](./vector-search.md) kNN query (`ORDER BY <dist> LIMIT k`). Higher = better recall, slower queries. Does not affect range (`WHERE <dist> < r`) queries. |
| `sdb_rerank_factor` | `4` | For a quantized (`quant` other than `none`) [vector](./vector-search.md#quantization) index, the candidate pool re-scored with exact distances is `sdb_rerank_factor * k`. Higher = better recall, slower queries; `0` disables reranking. Ignored for unquantized indexes. |
| `sdb_hnsw_ef_search` | `64` | Search beam width of an [HNSW](./vector-search.md#hnsw) vector index. Higher = better recall, slower queries. It is also the result ceiling: a value below the query's `LIMIT` returns fewer rows. |
| `sdb_scan_split` | `auto` | How an index scan shares a segment between worker threads. Each worker scans the row groups of a segment in order. `tail` gives every worker a segment of its own; only when no segment is left unclaimed does an idle worker join the segment with the most row groups left, so a segment is split across workers only at the end of the scan, when there are fewer segments than workers, or when one segment holds most of the data. `always` puts every worker on the same segment until all of its row groups are claimed; `never` scans each segment whole on one worker. `auto` is `always` when the query has an `ORDER BY <column> LIMIT` scan order and `tail` otherwise. Meant for benchmarking and tests. |
| `sdb_scan_order` | `auto` | The order an index scan claims its segments in. `largest_first` and `smallest_first` order them by live document count; `order` is best-first by the `ORDER BY` column's row-group statistics when the query has a scan order, so the `TOP_N` bound tightens early. `auto` is `order` under a scan order and `largest_first` otherwise. |
| `sdb_scan_no_split_row_groups` | `1` | A segment with at most this many row groups is always one unit of an index scan and is never split across workers. |
| `sdb_compact_target_segments` | `1` | How many segments `VACUUM (COMPACT_*)` leaves in a search table. `0` or `1` merges every segment into one; a larger `N` splits the segments, in the order their rows were written, into `N` runs of about as many live rows each and merges each run into one segment, so no single merge holds the whole table. A segment left alone in its run is kept as it is unless it has deleted rows. A table with `N` segments or fewer is left as it is. |

```sql
SET sdb_ivf_search_nprobe = 32;  -- scan more clusters for this session
SET sdb_rerank_factor = 8;       -- widen the exact-rerank pool for a quantized index
SET sdb_levenshtein_max_terms = 256; -- widen fuzzy expansion
-- ... run queries ...
RESET sdb_ivf_search_nprobe;     -- back to the default
```

## Introspection

List the inverted indexes with the `sdb_indexes()` table function:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_004" />

The standard `pg_indexes` view also lists them (alongside the primary-key index), with the `CREATE INDEX` statement in its `indexdef` column:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_005" />

<DocCallout type="attention">

`pg_indexes_size()` is not yet meaningful for inverted indexes -- it returns `0`. Use `sdb_indexes()` / `pg_indexes` for index metadata.

</DocCallout>

### Per-index statistics in `sdb_metrics` {#sdb-metrics}

The `sdb_metrics` system table exposes live runtime metrics. Most rows are process-wide gauges (connection counts, active/pending maintenance tasks) and carry a `NULL` `relation_id`. In addition, **each inverted index contributes a row per statistic**, tagged with the index's oid in the `relation_id` column -- describing its physical state and the health of its background maintenance.

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
| `num_live_docs` | Live (non-deleted) documents; `num_docs - num_live_docs` is the deleted backlog awaiting compaction |
| `num_buffered_docs` | Documents written but not yet committed -- still in memory, not searchable (see the [refresh model](#visibility-and-the-refresh-model)) |
| `num_segments` | On-disk segments |
| `num_files` | Files backing the index |
| `index_size` | On-disk index size, in bytes |
| `num_failed_commits` | Commit (refresh) operations that failed |
| `num_failed_cleanups` | Cleanup operations that failed |
| `num_failed_consolidations` | Consolidation (compaction) operations that failed |
| `avg_commit_time_ms` | Average duration of the last few commits, in ms |
| `avg_cleanup_time_ms` | Average duration of the last few cleanups, in ms |
| `avg_consolidation_time_ms` | Average duration of the last few consolidations, in ms |
| `merges_queued` | Background merges waiting in the queue |
| `merges_running` | Merges running now, background or on demand |
| `merge_pending_bytes` | Bytes the queued and running merges will write, estimated; with `num_segments`, how far compaction lags behind the writes |

The `avg_*` values are a rolling average over a small window of the most recent successful operations.

Among the process-wide rows, `compaction_pending` counts the background merges waiting in the queue, `compaction_threads` the background threads that merges hold, and `compaction_written_bytes` the bytes merges have written since the server started. Rows cover the inverted indexes in the **current database**. Join `pg_class` to resolve the index name, or `pg_index` to reach the base table:

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

A persistently high `num_buffered_docs`, a growing `num_failed_*`, or a climbing `avg_commit_time_ms` are signs that maintenance is falling behind the write rate -- see [Background intervals](#background-intervals) and [Performance](#performance) for the knobs.

## See also

- [Inverted Index](./index.md) · [Ranking](./ranking.md)
- [`VACUUM`](../../statements/vacuum/index.md) -- full statement reference
- [`CREATE INDEX ... USING inverted`](../../statements/create_index/inverted.md) -- index `WITH` options
