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

SereneDB extends `VACUUM` with options that maintain [inverted indexes](../create_index/index.md). A statement takes **at most one** of these options, and an extension option cannot be combined with standard options such as `ANALYZE`. The options fall into three families.

### Refreshing -- `REFRESH_*`

Inverted indexes are eventually consistent: rows you `INSERT`, `UPDATE` or `DELETE` may not be visible to queries until the index is refreshed. `REFRESH_*` publishes pending writes to readers. After writing to an indexed table, refresh it so the new rows become searchable:

<SqlLogicTest id="sql/statements/vacuum/index/example_007" />

### Compacting -- `COMPACT_*`

`COMPACT_*` merges index segments to reclaim space and keep queries fast. It does not change query results:

<SqlLogicTest id="sql/statements/vacuum/index/example_008" />

Inverted indexes and search tables also compact in the background by default (see [Background compaction](../../indexes/inverted/maintenance.md#background-compaction)); `COMPACT_*` merges on demand, including when background compaction is disabled. A search table is merged into a single segment. Set `sdb_compact_target_segments` to `N` to stop at `N` segments instead: the segments, in the order their rows were written, are split into `N` runs of about as many live rows each, and each run is merged into one segment. No single merge holds the whole table, and each new segment holds rows written next to each other. Each merge spreads its columns and postings over as many threads as the session may use (the `threads` setting).

While `COMPACT_*` works on a table or an index, its background merges pause: those in progress stop, and none starts until `COMPACT_*` ends. Each merge becomes visible as soon as it finishes, so the disk holds at most one merge's extra copy at a time. Cancelling the statement or reaching `statement_timeout` stops the merge in progress and leaves its segments as they were; merges that finished before remain.

<SqlLogicTest id="sql/statements/vacuum/index/example_011" />

### Recomputing statistics -- `RECOMPUTE_STATS_*`

`RECOMPUTE_STATS_*` recomputes the index statistics used for relevance scoring and planning:

<SqlLogicTest id="sql/statements/vacuum/index/example_009" />

### Scopes

Each option applies at the scope named by its suffix and takes the matching object name as its argument:

| Scope | `REFRESH` | `COMPACT` | `RECOMPUTE_STATS` | Argument |
|---|---|---|---|---|
| Index | `REFRESH_INDEX` | `COMPACT_INDEX` | -- | index name |
| Table | `REFRESH_TABLE` | `COMPACT_TABLE` | `RECOMPUTE_STATS_TABLE` | table name |
| Schema | `REFRESH_SCHEMA` | `COMPACT_SCHEMA` | `RECOMPUTE_STATS_SCHEMA` | `[database.]schema` |
| Database | `REFRESH_DATABASE` | `COMPACT_DATABASE` | `RECOMPUTE_STATS_DATABASE` | database name |
| Everything | `REFRESH_ALL` | `COMPACT_ALL` | `RECOMPUTE_STATS_ALL` | *(none)* |

`RECOMPUTE_STATS` additionally accepts a column scope, `RECOMPUTE_STATS_COLUMN`, whose argument is `[schema.]table.column`.

Object names may be qualified, for example `VACUUM (REFRESH_TABLE) mydb.public.articles`. The `*_ALL` scopes are instance-wide and take no argument.

### Combining options

A statement accepts at most one maintenance option, and a maintenance option cannot be mixed with standard `VACUUM` options such as `ANALYZE`:

<SqlLogicTest id="sql/statements/vacuum/index/example_010" />

The `vacuum_rebuild_indexes` setting that governs index rebuilds is fixed at startup and cannot be changed while the database is running:

<SqlLogicTest id="sql/statements/vacuum/index/example_006" />

## Reclaiming Space

The `VACUUM` statement does not reclaim space.
To reclaim space, use the `CHECKPOINT` statement or compact the database by creating a fresh copy with the [`COPY FROM DATABASE` statement](../copy/index.md#copy-from-database--to).

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

See the [scope table](#scopes) for the valid `( ... )` maintenance options and their arguments.
