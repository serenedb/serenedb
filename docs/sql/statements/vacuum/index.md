---
title: VACUUM
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

`VACUUM` serves two roles in SereneDB. Without options it is a PostgreSQL-compatible no-op on SereneDB tables, accepted so that existing tooling keeps working. It is also extended with options that maintain tables and their [inverted indexes](../create_index/index.md).

## Standard `VACUUM` and `ANALYZE`

On a SereneDB table, `VACUUM` and `ANALYZE` without the options below do no work — they are accepted purely for PostgreSQL compatibility. On a table of an attached DuckDB database file, `VACUUM table` and `ANALYZE table` recompute the table's statistics.

The bare form:

<SqlLogicTest id="sql/statements/vacuum/index/example_001" />

`ANALYZE` is accepted as well:

<SqlLogicTest id="sql/statements/vacuum/index/example_002" />

Targeting a specific table, optionally with a column list, is also accepted and is likewise a no-op:

<SqlLogicTest id="sql/statements/vacuum/index/example_003" />

<SqlLogicTest id="sql/statements/vacuum/index/example_004" />

`VACUUM FULL` is not supported:

<SqlLogicTest id="sql/statements/vacuum/index/example_005" />

## Table and Index Maintenance

SereneDB extends `VACUUM` with options that maintain tables and their [inverted indexes](../create_index/index.md). A statement takes **at most one** of these options, and an extension option cannot be combined with standard options such as `ANALYZE`. The options fall into three families.

### Flushing — `FLUSH_*`

A committed `INSERT`, `UPDATE` or `DELETE` is visible to every snapshot taken after its commit, full-text search included, whether or not it was flushed. A table holds its most recent writes in memory and writes them to its on-disk segments later. `FLUSH_*` writes them now; `FLUSH_INDEX` flushes the table the index belongs to, and does nothing for an index on a view. It does not change query results:

<SqlLogicTest id="sql/statements/vacuum/index/example_007" />

### Compacting — `COMPACT_*`

`COMPACT_*` merges the segments of tables and of inverted indexes on views to reclaim space and keep queries fast; `COMPACT_INDEX` compacts the table an index belongs to, or the index itself when it is on a view. It does not change query results:

<SqlLogicTest id="sql/statements/vacuum/index/example_008" />

Tables and inverted indexes on views also compact in the background by default (see [Background compaction](../../indexes/inverted/maintenance.md#background-compaction)); `COMPACT_*` merges on demand, including when background compaction is disabled. A table is merged into a single segment. Set `sdb_compact_target_segments` to `N` to stop at `N` segments instead: the segments are split into `N` disjoint groups and each group is merged into one segment, so no single merge holds the whole table.

<SqlLogicTest id="sql/statements/vacuum/index/example_011" />

### Scopes

Each option applies at the scope named by its suffix and takes the matching object name as its argument:

| Scope | `FLUSH` | `COMPACT` | Argument |
|---|---|---|---|
| Index | `FLUSH_INDEX` | `COMPACT_INDEX` | index name |
| Table | `FLUSH_TABLE` | `COMPACT_TABLE` | table name |
| Schema | `FLUSH_SCHEMA` | `COMPACT_SCHEMA` | `[database.]schema` |
| Database | `FLUSH_DATABASE` | `COMPACT_DATABASE` | database name |
| Everything | `FLUSH_ALL` | `COMPACT_ALL` | *(none)* |

Object names may be qualified, for example `VACUUM (FLUSH_TABLE) mydb.public.articles`. The `*_ALL` scopes take no argument and cover every database of the server but not attached databases.

The statistics of a table and its indexes follow its segments as they are written and compacted, so there is no option to recompute them. To recompute the statistics of a table in an attached DuckDB database file, run [`ANALYZE`](../analyze.md) on it.

A table you lack the `MAINTAIN` privilege on is skipped with a warning such as `permission denied to compact "articles", skipping it`; an index is maintained under the privileges of its table or view. A read-only database, such as one attached with `READ_ONLY`, refuses the options that name an object.

### Combining options

A statement accepts at most one maintenance option, and a maintenance option cannot be mixed with standard `VACUUM` options such as `ANALYZE`:

<SqlLogicTest id="sql/statements/vacuum/index/example_010" />

The `vacuum_rebuild_indexes` setting, which governs index rebuilds in attached DuckDB database files, is fixed at startup and cannot be changed while the server is running:

<SqlLogicTest id="sql/statements/vacuum/index/example_006" />

## Reclaiming Space

`VACUUM` without options does not reclaim space. `COMPACT_*` rewrites a table's segments without the rows deleted from it. In an attached DuckDB database file, use the `CHECKPOINT` statement, or create a fresh copy with the [`COPY FROM DATABASE` statement](../copy/index.md#copy-from-database--to).

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

See the [scope table](#scopes) for the valid `( … )` maintenance options and their arguments.
