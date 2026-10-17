---
title: VACUUM
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

`VACUUM` serves two roles in SereneDB. On standard tables it is a PostgreSQL-compatible no-op, accepted so that existing tooling keeps working. It is also extended with options that maintain SereneDB's [inverted indexes](../create_index/index.md).

## Standard `VACUUM` and `ANALYZE`

On a standard table `VACUUM` does no work -- it is accepted purely for PostgreSQL compatibility.

The bare form:

<SqlLogicTest id="sql/statements/vacuum/index/example_001" />

`ANALYZE` is accepted as well:

<SqlLogicTest id="sql/statements/vacuum/index/example_002" />

Targeting a specific table, optionally with a column list, is also accepted and is likewise a no-op:

<SqlLogicTest id="sql/statements/vacuum/index/example_003" />

<SqlLogicTest id="sql/statements/vacuum/index/example_004" />

`VACUUM FULL` is not supported:

<SqlLogicTest id="sql/statements/vacuum/index/example_005" />

## Inverted Index Maintenance

SereneDB extends `VACUUM` with options that maintain [inverted indexes](../create_index/index.md). A statement takes **at most one** of these options, and an extension option cannot be combined with standard options such as `ANALYZE`. The options fall into four families.

### Refreshing -- `REFRESH_*`

Inverted indexes are eventually consistent: rows you `INSERT`, `UPDATE` or `DELETE` may not be visible to queries until the index is refreshed. `REFRESH_*` publishes pending writes to readers. After writing to an indexed table, refresh it so the new rows become searchable:

<SqlLogicTest id="sql/statements/vacuum/index/example_007" />

### Compacting -- `COMPACT_*` {#compact}

`COMPACT_*` merges index segments to reclaim space and keep queries fast. It does not change query results:

<SqlLogicTest id="sql/statements/vacuum/index/example_008" />

Inverted indexes and search tables also compact in the background by default (see [Background compaction](../../indexes/inverted/maintenance.md#background-compaction)); `COMPACT_*` merges on demand, including when background compaction is disabled. A search table is merged into a single segment. Set `sdb_compact_target_segments` to `N` to stop at `N` segments instead: the segments, in the order their rows were written, are split into `N` runs of about as many live rows each, and each run is merged into one segment. No single merge holds the whole table, and each new segment holds rows written next to each other. An inverted index on a DuckDB table or a view is merged into a single segment. A table or an index that already has no more segments than that is left as it is, even when it has deleted rows: `OPTIMIZE_*` rewrites those. Each merge spreads its columns and postings over as many threads as the session may use (the `threads` setting).

A merge of a search table or an index with a sort key ([`SORTED BY`](../create_table/index.md#sort-keys)) takes at most 1024 segments. When more segments are to become one, `COMPACT_*` and `OPTIMIZE_*` merge them in steps: the first step merges just enough neighbouring segments, choosing those with the fewest live rows, to leave at most 1024, and the next step merges what is left. Only the rows of the first step are written twice.

While `COMPACT_*` works on a table or an index, its background merges pause: those in progress stop, and none starts until `COMPACT_*` ends. Each merge becomes visible as soon as it finishes, so the disk holds at most one merge's extra copy at a time. Cancelling the statement or reaching `statement_timeout` stops the merge in progress and leaves its segments as they were; merges that finished before remain.

<SqlLogicTest id="sql/statements/vacuum/index/example_011" />

### Optimizing -- `OPTIMIZE_*`

`OPTIMIZE_*` brings a search table or an inverted index into the shape that serves queries best, without choosing a segment count:

<SqlLogicTest id="sql/statements/vacuum/index/example_012" />

- **About as many segments as query threads.** A column scan spreads one segment over many threads, but index work (term lookups, wildcard and prefix expansion, dictionary enumeration) uses one thread per segment, so the target is the number of threads the server runs queries on. A small table gets fewer, so that every segment holds at least four row groups (`row_group_size`) of rows.
- **Contiguous and balanced.** The segments, in the order their rows were written, are split into runs of about as many live rows each, and each run is merged into one segment, so a filter on insertion order skips whole segments.
- **No deleted rows.** A segment with deleted rows is rewritten even when the table already has no more segments than the target.
- **Current encodings.** A segment that would otherwise stay as it is gets rewritten when its columns are not encoded the way the table's codec settings would encode them now: when re-encoding them would save more than 10% of their bytes (data written by a refresh, which favours speed, or under older settings), or when `ALTER COLUMN ... SET COMPRESSION` declared a codec its stored data does not use. The saving is estimated by encoding a few sampled row groups of each column; [`sdb_segments(..., columns := true)`](../../indexes/inverted/maintenance.md#sdb-compact) shows the estimate per column.
- **Idempotent.** A second `OPTIMIZE_*` on an unchanged table merges nothing. Merges only combine segments: a segment is never split, even when it holds more than its share.

Background merges pause and resume, and cancelling works, as for `COMPACT_*`. [`sdb_compact(mode := 'optimize')`](../../indexes/inverted/maintenance.md#sdb-compact) shows what an optimize would merge, or runs it with a given segment count.

### Cleaning up -- `CLEANUP_*`

`CLEANUP_*` deletes the files of inverted indexes and search tables that no snapshot uses anymore, such as the segments a merge replaced. Files that a running query still reads stay until a later cleanup. [`sdb_cleanup`](../../indexes/inverted/maintenance.md#sdb-refresh) does the same for one relation and reports how many files and bytes it deleted.

### Recomputing statistics -- `RECOMPUTE_STATS_*`

`RECOMPUTE_STATS_*` recomputes the index statistics used for relevance scoring and planning:

<SqlLogicTest id="sql/statements/vacuum/index/example_009" />

### Scopes

Each option applies at the scope named by its suffix and takes the matching object name as its argument:

| Scope | `REFRESH` | `COMPACT` | `OPTIMIZE` | `CLEANUP` | `RECOMPUTE_STATS` | Argument |
|---|---|---|---|---|---|---|
| Index | `REFRESH_INDEX` | `COMPACT_INDEX` | `OPTIMIZE_INDEX` | `CLEANUP_INDEX` | -- | index name |
| Table | `REFRESH_TABLE` | `COMPACT_TABLE` | `OPTIMIZE_TABLE` | `CLEANUP_TABLE` | `RECOMPUTE_STATS_TABLE` | table name |
| Schema | `REFRESH_SCHEMA` | `COMPACT_SCHEMA` | `OPTIMIZE_SCHEMA` | `CLEANUP_SCHEMA` | `RECOMPUTE_STATS_SCHEMA` | `[database.]schema` |
| Database | `REFRESH_DATABASE` | `COMPACT_DATABASE` | `OPTIMIZE_DATABASE` | `CLEANUP_DATABASE` | `RECOMPUTE_STATS_DATABASE` | database name |
| Everything | `REFRESH_ALL` | `COMPACT_ALL` | `OPTIMIZE_ALL` | `CLEANUP_ALL` | `RECOMPUTE_STATS_ALL` | *(none)* |

`RECOMPUTE_STATS` additionally accepts a column scope, `RECOMPUTE_STATS_COLUMN`, whose argument is `[schema.]table.column`.

An inverted index on a search table keeps its terms in the table's own storage, so `REFRESH_INDEX`, `COMPACT_INDEX`, `OPTIMIZE_INDEX` and `CLEANUP_INDEX` on such an index do what `REFRESH_TABLE`, `COMPACT_TABLE`, `OPTIMIZE_TABLE` and `CLEANUP_TABLE` do on the table.

Object names may be qualified, for example `VACUUM (REFRESH_TABLE) mydb.public.articles`. The `*_ALL` scopes are instance-wide and take no argument.

### Combining options

A statement accepts at most one maintenance option, and a maintenance option cannot be mixed with standard `VACUUM` options such as `ANALYZE`:

<SqlLogicTest id="sql/statements/vacuum/index/example_010" />

The `vacuum_rebuild_indexes` setting that governs index rebuilds is fixed at startup and cannot be changed while the database is running:

<SqlLogicTest id="sql/statements/vacuum/index/example_006" />

## Reclaiming Space

Apart from `CLEANUP_*`, which deletes unused files of inverted indexes and search tables, the `VACUUM` statement does not reclaim space.
To reclaim space in the database file, use the `CHECKPOINT` statement or compact the database by creating a fresh copy with the [`COPY FROM DATABASE` statement](../copy/index.md#copy-from-database--to).

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

See the [scope table](#scopes) for the valid `( ... )` maintenance options and their arguments.
