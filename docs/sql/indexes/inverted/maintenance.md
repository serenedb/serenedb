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
| `cleanup_interval_step` | `1` | Automatic refreshes between cleanup passes, which delete the files no segment uses anymore |

View-backed indexes have a fourth interval, `reindex_interval`, which re-scans the view's *source* for new, changed and removed data -- see [Refreshing the index](./views.md#refreshing-the-index).

An inverted index on a search table has no storage or background tasks of its own: the table's options run refresh and compaction for every index on it. Set them on the table, in `CREATE TABLE ... WITH` or with [`ALTER TABLE ... SET`](../../statements/alter_table/index.md#set--reset-storage-options), whether or not it has indexes. `CREATE INDEX ... WITH` and `ALTER INDEX ... SET` reject them for an index on a search table. They also reject the options that only `CREATE TABLE ... WITH` sets on a search table: `row_group_size`, [`segment_memory_max`](#write-memory) and `optimize_top_k`. `segment_docs_max` doesn't apply to such an index and is rejected too.

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_007" />

## Background compaction

Compaction is on by default, for inverted indexes and search tables alike: every `compaction_interval` milliseconds a background task merges segments into larger ones, so a table loaded in many small batches does not keep one segment per batch. Three more `WITH` options shape each merge:

| Option | Default | Controls |
|---|---|---|
| `compaction_max_segments` | `10` | Segments merged by one background compaction |
| `compaction_max_segments_bytes` | `5368709120` (5 GB) | Size budget of one background compaction |
| `compaction_floor_segment_bytes` | `2097152` (2 MB) | Segments smaller than this count as equal-sized when choosing what to merge |

The session settings of the same names hold the defaults a new index or search table takes.

Background compaction merges runs of neighbouring segments in the order their rows were written, so every segment holds a contiguous range of rows and a filter on insertion order skips whole segments:

- A run merges only when it is balanced: together its segments are at least six times the size of the largest one (or `compaction_max_segments` times, when that is smaller), where a segment smaller than `compaction_floor_segment_bytes` counts as that size. Small segments therefore merge in groups of six or more, and each row is rewritten a few times however large the table grows.
- Among the balanced runs of at most `compaction_max_segments` segments and `compaction_max_segments_bytes` bytes, the one that rewrites the fewest bytes for each segment it removes goes first; on a tie, the newest.
- When merges fall behind and more than ten neighbouring segments wait, the balance rule relaxes step by step, down to twice the largest segment at fifty.
- A segment with more than a quarter of its rows deleted is rewritten on its own when nothing else is to merge.

A segment holds at most 2,147,418,110 rows. A merge whose segments hold more live rows than that together is refused and logged, and its segments stay as they are; this applies to background compaction, `VACUUM` and [`sdb_compact`](#sdb-compact) alike.

A merge of a search table or an index with a sort key (`SORTED BY`) takes at most 1024 segments, so its background compaction merges at most 1024 at a time, whatever `compaction_max_segments` says. `VACUUM` merges a larger group [in steps](../../statements/vacuum/index.md#compact).

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
| Clean up | `CLEANUP_INDEX` · `CLEANUP_TABLE` · `CLEANUP_SCHEMA` · `CLEANUP_DATABASE` · `CLEANUP_ALL` | Delete index files that no snapshot uses anymore, such as the segments a merge replaced |
| Recompute statistics | `RECOMPUTE_STATS_COLUMN` · `RECOMPUTE_STATS_TABLE` · `RECOMPUTE_STATS_SCHEMA` · `RECOMPUTE_STATS_DATABASE` · `RECOMPUTE_STATS_ALL` | Rebuild the term statistics used by relevance scoring and planning |

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_002" />

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_003" />

Recompute statistics after large changes in data distribution so that [relevance scores](./ranking.md) and planning stay accurate.

A refresh and a compaction of a search table delete the files they make unused when they finish, and so does every merge of `COMPACT_*`, `OPTIMIZE_*` and [`sdb_compact`](#sdb-compact). Otherwise the files of an inverted index with storage of its own are deleted by the background task after every `cleanup_interval_step` automatic refreshes, or by `CLEANUP_*`. Files that a running query still reads stay until a later cleanup.

## Merging on demand and explaining merges {#sdb-compact}

`CALL sdb_compact(relation [, mode := ...])` merges segments of a search table or an inverted index right away and returns one row per merge. Like `VACUUM (COMPACT_*)`, it first publishes the pending writes. `EXPLAIN CALL sdb_compact(...)` shows which merges it would run without running them, and `EXPLAIN ANALYZE CALL sdb_compact(...)` runs them and adds how long each merge step took.

| `mode` | Merges |
|---|---|
| `background` (default) | What one round of background compaction would merge, following the table's `compaction_*` options, even when background compaction is disabled |
| `compact` | What `VACUUM (COMPACT_TABLE)` or `VACUUM (COMPACT_INDEX)` would merge: `target_segments` groups for a search table (default: the `sdb_compact_target_segments` setting), one segment for an index |
| `segments` | Exactly the segments named in `segments := ['...', ...]`, as `sdb_segments` lists them |
| `optimize` | The shape that serves queries best: about as many segments as the server has query threads, each a contiguous range of rows, balanced by live documents and without deleted documents. A small relation gets fewer, so that every segment holds at least four row groups (`row_group_size`) of rows. `target_segments` sets the count instead, whatever the relation's size; a relation with fewer segments keeps their number. A segment with deletes is rewritten even when the relation already has no more segments than the target, and so is a segment whose columns are not encoded the way the current codec settings would encode them (see `verdict` in `sdb_segments` below); a second run finds nothing to do. Merges only combine segments: a segment is never split, even when it holds more than its share |

Each result row describes one merge:

| Column | Meaning |
|---|---|
| `merge` | Number of the merge within the call |
| `outcome` | `merged`; `deferred` (merged, visible after the next commit); `nothing`; `busy` (a segment was being merged by another task); `failed`; `cancelled` (stopped before it finished, for example by a `TRUNCATE` or a `CREATE INDEX` on the table; its segments stay as they were) |
| `inputs`, `input_docs`, `input_live_docs`, `input_bytes` | The merged segments, their documents, live documents and bytes |
| `output`, `output_docs`, `output_bytes` | The new segment |
| `total_ms` | Wall-clock time of the merge |
| `prepare_ms`, `norms_ms`, `columns_ms`, `columns_commit_ms`, `ann_ms`, `postings_ms`, `commit_ms` | Time of each merge step in milliseconds: `prepare_ms` plans the merge and puts the rows of a relation with a sort key in key order, `norms_ms` merges the norms, `columns_ms` the stored columns, `columns_commit_ms` finishes the stored columns, `ann_ms` builds the vector indexes, `postings_ms` merges the postings and `commit_ms` writes the new segment. `0` when the merge did not run, as for `nothing` |
| `error` | Why a merge failed |

`EXPLAIN` prints the plan in an `SDB_COMPACT` box, with one group of lines per merge and the number of merges planned as the estimated rows (`~N rows`):

| Line | Meaning |
|---|---|
| `Table`, `Mode` | The relation and the mode of the call: `background`, `compact`, `optimize` or `segments` |
| `Segments`, `Merges` | Segments of the relation and merges planned |
| `Merge N` | How many segments the merge combines into one, and why: `adjacent policy` for `background`, `compact to N segments` for `compact`, `optimize to N segments` for `optimize`, `listed segments` for `segments`. An `optimize` that rewrites a single segment says `re-encoding saves N%` or `a declared column codec changed`. The first step of a merge in key order adds `first step` |
| `Merge N Inputs` | The segments merged: the first eight, then `+N more` |
| `Merge N Docs` | Documents and live documents of the inputs |
| `Merge N Bytes` | Size of the inputs and the estimated size of the new segment |

On a search table or an index with a sort key (`SORTED BY`), a merge takes at most 1024 segments. When a `compact`, `optimize` or `segments` call would merge more segments into one, it merges them in the [steps](../../statements/vacuum/index.md#compact) `VACUUM` takes, one result row each: the first step merges just enough neighbouring segments, those with the fewest live rows, to leave at most 1024, and the next step merges what is left. In `segments` mode, the segment a step writes takes the place of its inputs in the list. `EXPLAIN` shows the first step.

`EXPLAIN ANALYZE` runs the merges and shows the same lines in a `TABLE_SCAN` box, as for any table function, followed by `Actual Outcome` (how many merges ended with each outcome), `Actual Written` (bytes of the new segments), one `Actual <step>` line per merge step, summed over the merges, and `Actual Total`.

`CALL sdb_compact` and `VACUUM (COMPACT_*)` merge on as many threads as the session may use (the `threads` setting): the columns of a merge, large columns in ranges of row groups, and the terms of large indexed fields are merged side by side, so a merge of a wide table finishes sooner on more threads. A step time adds up the threads that ran the step, so with several threads the step times (`prepare_ms` to `commit_ms`) and the `Actual <step>` lines of `EXPLAIN ANALYZE` add up to more than the merges took.

A merge copies a column's encoded blocks unchanged when re-encoding them would not save much (the `verdict` of `sdb_segments` below), and re-encodes them otherwise, so one merged column can mix copied and re-encoded rows. Copying costs little more than reading the blocks, so merges of segments that earlier merges already encoded well take a fraction of the time.

Background compaction runs on its own *merge threads*, a quarter of the server's logical cores minus one (at least one), and never on the threads that run queries. The background merges of all search tables and indexes share one queue: cheap merges start first and a waiting merge gains priority with every merge that starts before it, a small merge never waits behind large ones, and a table's large merges leave threads for the merges of other tables. A merge that fails leaves its segments out of the next rounds of background compaction: one round after the first failure, twice as many after each further failure in a row, at most 300. Dropping a table, an index or a database discards its queued merges and stops its running ones within a moment.

`sdb_compact`'s merges take effect at once: they are not part of the surrounding transaction, and `ROLLBACK` does not undo them. Cancelling the call or reaching `statement_timeout` stops it: the merge in progress is abandoned and its segments stay as they were, while merges that finished before remain. Like `VACUUM (COMPACT_*)`, a call that merges anything pauses the relation's background merges until it ends: those in progress stop and show as `cancelled`. `EXPLAIN` runs nothing and pauses nothing.

`sdb_segments(relation)` lists the segments of a search table or an inverted index in the order their rows were written: `segment`, `docs`, `live_docs`, `deleted_docs` and `bytes`.

`sdb_segments(relation, columns := true)` lists the stored columns of every segment instead, internal ones included (`rowid` is the row id an inverted index on a DuckDB table stores), one row each:

| Column | Meaning |
|---|---|
| `segment`, `column`, `type`, `rows`, `bytes` | The segment, the column, its type, its rows and the bytes its data takes, nested parts included |
| `codecs` | The codecs of the column's own blocks, in the order they first appear. For a list these blocks say which stored list each row has; its elements and where each list ends are nested parts. A struct or a variant has no blocks of its own (`NULL`): its values are all in nested parts. Nested parts are not listed here, [`pragma_storage_info`](./columnstore.md#inspecting-storage) lists them |
| `reencoded_bytes` | The estimated size of the column if it were encoded again with the current codec settings, as a merge would. A few row groups spread over the column are encoded on the side and compared with the bytes they take now; the estimate is kept as long as the segment is in use. `NULL` for nested columns, which are not checked |
| `verdict` | `re-encode` when re-encoding would save more than 10% of the column's bytes, or when the codec declared with `ALTER COLUMN ... SET COMPRESSION` is one the column's blocks do not use; `copy` otherwise; `NULL` when not checked |

The estimates cost a little encoding work the first time a segment is looked at. `sdb_compact(mode := 'optimize')` and `VACUUM (OPTIMIZE_*)` use the same verdicts to decide whether a segment that would otherwise stay as it is gets rewritten:

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_012" />

`sdb_compact` and `sdb_segments` need the same privilege as `VACUUM` on the table (`MAINTAIN`); for an inverted index, on the table or view it indexes.

Two system tables show the compaction of the current database's search tables and indexes, whichever task ran a merge (`origin`: `background`, `vacuum` or `sql`):

- `sdb_merges` lists the merges that run or wait in the background queue: `merge_id`, `relation_id`, `origin`, `state` (`queued` or `running`), `queued`, `started`, `elapsed_ms`, the inputs, and `estimated_bytes`, what the merge will write, estimated from the live share of its inputs. A queued merge keeps its `merge_id` when it runs, and it becomes the `id` of the merge's `sdb_compact_history` row; a merge dropped from the queue, for example by a `VACUUM` on its table, leaves no history.
- `sdb_compact_history` keeps the last [`sdb_compact_history_size`](../../../configuration/overview.md#logging-metrics-and-profiling) finished merges of all search tables and indexes together, 256 by default, one row each, with `id`, `relation_id`, `origin`, `started`, `finished` and the columns of a `sdb_compact` row other than `merge`.

<SqlLogicTest id="sql/indexes/inverted/maintenance/example_010" />

## Refreshing view-backed indexes

A [view- or external-data-backed index](./views.md) holds a snapshot of its source and does not track later changes on its own. `REINDEX INDEX <name>` refreshes it in one pass, incrementally for Iceberg tables and file globs, and the `reindex_interval` index option repeats that pass in the background. See [Refreshing the index](./views.md#refreshing-the-index).

## Schema changes on an indexed table

An inverted index pins every column it reads, including columns reached only through an [indexed expression](./modeling.md#indexing-expressions) -- for example a struct sub-field such as `(s['id']::INTEGER)` pins the whole `s` column. On a regular table:

- **`RENAME` column, table or index** -- allowed; the index follows the rename.
- **`ADD COLUMN`** -- allowed; the index is unaffected.
- **`ALTER COLUMN ... TYPE`** on a pinned column -- rejected with `Cannot change the type of this column: an index depends on it!`; the index stores values of the old type. Drop the index first, change the type, then recreate it.
- **`ADD` / `DROP` / `RENAME` of a struct field** on a pinned column -- rejected for the same reason, even when the indexed expression targets a *different* sub-field (the whole column is pinned). Drop the index first.
- **`DROP COLUMN`** on a pinned column -- allowed; it drops every index that covers the column.
- **`DROP COLUMN`** on any other column -- allowed, including one that comes before a pinned column.

A search table's columns are fixed, whether or not it has indexes: `ADD COLUMN`, `DROP COLUMN` and `ALTER COLUMN ... TYPE` are rejected, while renaming the table or a column is allowed. See [Search tables](../../statements/alter_table/index.md#search-tables) in the `ALTER TABLE` reference.

## Performance

Tuning is mostly about the background cadence and segment layout; use only the options that exist:

- **Refresh vs. compaction cadence** -- lower `refresh_interval` for fresher results, raise it (or `0`) to reduce overhead on write-heavy tables; `compaction_interval` governs how aggressively segments merge, and `cleanup_interval_step` how soon the files they replace are deleted.
- **Row-group size** -- `row_group_size` controls the columnstore batch size for stored (`INCLUDE`d) columns and for norm columns alike; norms share this one setting. It must be a multiple of the vector size (2048), and it is also the unit a scan hands to one worker: an index whose segments hold few row groups cannot spread a scan over more threads than it has row groups, so lower it when rows are expensive to materialise and the index is small. It is fixed at `CREATE INDEX` and applies to segments written afterwards.
- **Build then index** -- for a bulk load, create the table, load the data, then create the index; this produces a more compact index than loading into an already-indexed table.
- **Whole-relation aggregates** -- `count(*)` over a search table or an index relation (`FROM my_index`) without a `WHERE` clause is answered from the document counts, without reading any document. `min` and `max` of numeric and temporal `INCLUDE`d columns and of keyword columns are answered the same way while the relation holds no deleted documents; deleted documents are dropped by compaction, and until then `min` and `max` read the documents. `EXPLAIN` shows no index scan for an aggregate answered this way.
- **Top-K** -- set [`optimize_top_k`](./ranking.md#top-k-queries-and-wand-pruning) to accelerate `ORDER BY <scorer> ... LIMIT k`.
- **Write memory** -- [`segment_memory_max`](#write-memory) sets the segment size a write produces and, together with `checkpoint_threshold` for a serial write, what the write holds in memory.

## Write memory {#write-memory}

Memory held while writing to a search table depends on `segment_memory_max`, set in the table's `CREATE TABLE ... WITH (segment_memory_max = ...)` clause and defaulting to 256 MB, and for a serial write also on [`checkpoint_threshold`](../../../configuration/overview.md), 16 MB by default: a serial write buffers up to that much of its rows before it writes them to the index. Estimate the peak for one writing transaction as:

| Write | Peak memory |
| :--- | :--- |
| Serial `INSERT` / `UPDATE` | ≈ `segment_memory_max` + [`checkpoint_threshold`](../../../configuration/overview.md) |
| Parallel `INSERT` | ≈ `segment_memory_max` x threads |

Lower `segment_memory_max` to cap the segment part of that peak, raise it for larger segments and less compaction work. `checkpoint_threshold` is set for the whole server, and also decides how often the database checkpoints.

A table with a sort key (`SORTED BY`) holds the rows of a segment unsorted and uncompressed until it sorts and writes them, so under the same `segment_memory_max` it writes smaller segments than a table without one. A larger value gives it fewer segments and less compaction work, but a parallel load then holds about `segment_memory_max` x threads in memory and does not finish sooner, because the larger staging memory costs the load about as much time as the compaction saves.

```sql
CREATE TABLE docs (id BIGINT, body TEXT)
  WITH (storage = 'search', segment_memory_max = 134217728);  -- 128 MB
```

<DocCallout type="tip">

`DELETE` sits outside this bound, but costs little: until the removal is applied, a removed row takes at most a few bytes and a run of consecutive rows almost none, so even a million-row `DELETE` stays within a few megabytes.

</DocCallout>

## Durability {#durability}

A commit to a search table is durable once it is in the write-ahead log of the table's database; a refresh then writes the changes into the table's own index files. A checkpoint leaves search tables alone: it keeps the write-ahead log it retires, as `data.db.wal.retained.<n>` beside the database file, while a search table still has changes in it that its own files do not durably hold, and a later checkpoint deletes the kept logs no table needs anymore. A table's changes are durable in its files once a refresh has written them and a cleanup pass has run after it: by default every automatic refresh runs one (`cleanup_interval_step = 1`), and so do `VACUUM (REFRESH_TABLE)` and `VACUUM (COMPACT_TABLE)`. After a crash, the server replays the kept logs and the current one into the tables that need them.

Kept logs pile up while automatic refresh (`refresh_interval = 0`) or periodic cleanup (`cleanup_interval_step = 0`) is disabled, until `VACUUM (REFRESH_TABLE)` or `VACUUM (COMPACT_TABLE)` saves the table or the server restarts; with only cleanup disabled, a few background compactions save it too.

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
| `sdb_hnsw_column_filter` | `auto` | How an [HNSW](./vector-search.md#filtered-search) walk answers a predicate the columnstore can evaluate: `fold` evaluates one on columns the index does not index (`INCLUDE`d ones) over the whole segment before the walk, and answers one on indexed columns from the index; `read` reads the columns of each row the walk reaches, for indexed columns of a search table too; `auto` reads while the walk is expected to reach few enough rows for that to cost less than the fold, or than the index for an indexed column, pricing a read by the columns' encodings. For measurement -- `auto` is the one to leave alone. |
| `sdb_ann_rescore_width` | `-1` | For a quantized (`quant` other than `none`) [vector](./vector-search.md#rescoring) index, how far a candidate's code score may be off and still have it re-read at full precision: candidates from every segment are re-scored while their code score plus this many times the code's estimated error could still beat the `k`-th best exact score. `0` answers from the codes alone; `-1` uses a width per quantizer, listed under [Re-scoring](./vector-search.md#rescoring). Ignored for unquantized indexes. |
| `sdb_ann_force_exact` | `false` | Skip the ANN walk and return the exact answer, for a nearest-`k` query and a distance range alike. Every row with a vector is considered; an `hnsw` index's 8-bit codes (`sq8`, `usq8`) rule out most rows before their stored vectors are read. Useful to measure recall. `false` leaves the choice to the engine, which may still answer a selective filter with an exact scan. |
| `sdb_scan_split` | `auto` | How an index scan shares a segment between worker threads. Each worker scans the row groups of a segment in order. `tail` gives every worker a segment of its own; only when no segment is left unclaimed does an idle worker join the segment with the most row groups left, so a segment is split across workers only at the end of the scan, when there are fewer segments than workers, or when one segment holds most of the data. `always` puts every worker on the same segment until all of its row groups are claimed; `never` scans each segment whole on one worker. `auto` is `always` when the query has an `ORDER BY <column> LIMIT` scan order and `tail` otherwise. Meant for benchmarking and tests. |
| `sdb_scan_order` | `auto` | The order an index scan claims its segments in. `largest_first` and `smallest_first` order them by live document count; `order` is best-first by the `ORDER BY` column's row-group statistics when the query has a scan order, so the `TOP_N` bound tightens early. `auto` is `order` under a scan order and `largest_first` otherwise. |
| `sdb_scan_no_split_row_groups` | `1` | A segment with at most this many row groups is always one unit of an index scan and is never split across workers. |
| `sdb_compact_target_segments` | `1` | How many segments `VACUUM (COMPACT_*)` and `sdb_compact(mode := 'compact')` leave in a search table. `0` or `1` merges every segment into one; a larger `N` splits the segments, in the order their rows were written, into `N` runs of about as many live rows each and merges each run into one segment, so no single merge holds the whole table. A segment left alone in its run is kept as it is unless it has deleted rows. A table with `N` segments or fewer is left as it is. |

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

`pg_relation_size()` of an inverted index returns its size on disk, counting the refreshed rows only, and `pg_indexes_size()` of a regular table adds the inverted indexes on it to its other indexes. An index on a search table has no storage of its own, so both count it as `0`; its data is part of `pg_relation_size()` of the table. `pg_indexes_size()` of a view returns `0`.

</DocCallout>

### Statistics in `sdb_metrics` {#sdb-metrics}

The `sdb_metrics` system table exposes live runtime metrics. Most rows are process-wide gauges (connection counts, active/pending maintenance tasks) and carry a `NULL` `relation_id`. In addition, **each search table and each inverted index on a regular table or a view contributes a row per statistic**, tagged with its oid in the `relation_id` column -- describing its physical state and the health of its background maintenance. An inverted index on a search table has no storage of its own and contributes no rows: the search table's rows cover its indexes.

| Column | Description |
| :--- | :--- |
| `metric` | Metric name |
| `value` | Current value (`UBIGINT`) |
| `description` | Human-readable description |
| `relation_id` | Oid of the search table or inverted index the row describes (a relation in `pg_class`), or `NULL` for a process-wide gauge |

The metrics of each search table and inverted index:

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

The `avg_*` values are a rolling average over a small window of the most recent successful operations.

Among the process-wide rows, `compaction_pending` counts the background merges waiting in the queue, `compaction_threads` the background threads that merges hold, and `compaction_written_bytes` the bytes merges have written since the server started. Rows cover the search tables and inverted indexes in the **current database**. Join `pg_class` to resolve the name. To group the rows by table, join `pg_index` as well: an index's rows reach its table through `indrelid`, while a search table's rows carry the table's own oid. Here `my_logs` is a regular table with the inverted index `logs_idx`, and `my_events` a search table:

<SqlLogicTest id="sql/indexes/inverted/maintenance/sdb_metrics" />

A persistently high `num_buffered_docs`, a growing `num_failed_*`, or a climbing `avg_commit_time_ms` are signs that maintenance is falling behind the write rate -- see [Background intervals](#background-intervals) and [Performance](#performance) for the knobs.

## See also

- [Inverted Index](./index.md) · [Ranking](./ranking.md)
- [`VACUUM`](../../statements/vacuum/index.md) -- full statement reference
- [`CREATE INDEX ... USING inverted`](../../statements/create_index/inverted.md) -- index `WITH` options
