---
title: CHECKPOINT
draft: true
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CHECKPOINT` statement writes a snapshot of the catalog and of every table, including the rows, deletes and updates tables still hold in memory, then removes the log files the snapshot covers, so that a restart replays less. It does not write rows to segments: [`VACUUM (FLUSH_*)`](../vacuum/index.md) does. In an attached DuckDB database file, it writes the file's own write-ahead log into the database file.

## Examples

Checkpoint the server's databases:

<SqlLogicTest id="sql/statements/checkpoint/index/example_001" />


Checkpoint an attached DuckDB database file:

<SqlLogicTest id="sql/statements/checkpoint/index/example_002" />


`FORCE CHECKPOINT` is accepted as well:

<SqlLogicTest id="sql/statements/checkpoint/index/example_003" />


## Checkpointing In-Memory DuckDB Databases

An in-memory DuckDB database attached with `ATTACH ':memory:'` supports checkpointing. This has two key benefits:

-   Its tables support compression. This is disabled by default – you can turn it on using:

    <SqlLogicTest id="sql/statements/checkpoint/index/example_004" />


-   Checkpointing triggers vacuuming deleted rows, allowing space to be reclaimed after deletes/truncation.

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

The server writes a new snapshot on its own once the log written since the last one reaches [`max_wal_size`](../../../configuration/overview.md); see [Durability](../../indexes/inverted/maintenance.md#durability). This statement is for manual checkpoint actions.

## Behavior

The server's databases share one write-ahead log, so a checkpoint of any of them covers all of them. It runs while other transactions write, and `FORCE CHECKPOINT` behaves the same there.

In an attached DuckDB database file, the default `CHECKPOINT` command fails if other write transactions are running on the file. `FORCE CHECKPOINT` stops new transactions on it from starting, waits for the running write transactions to finish, and then checkpoints.

While a [backup](../../../operations/backup.md) is in progress, between `pg_backup_start` and `pg_backup_stop`, both `CHECKPOINT` and `FORCE CHECKPOINT` fail with `cannot checkpoint while a backup is in progress`. Run the checkpoint again after `pg_backup_stop`. A `pg_backup_start` issued while a checkpoint is running waits for it to finish.

Also see the related [`PRAGMA` option](../../../configuration/pragmas.md#force-checkpoint) for further behavior modification.

### Reclaiming Space

In the server's databases, a checkpoint does not drop deleted rows: compaction does, in the background or with [`VACUUM (COMPACT_*)`](../vacuum/index.md).

In an attached DuckDB database file, a checkpoint (automatic or otherwise) partially reclaims the space occupied by deleted rows. It does not remove all deleted rows, but merges row groups that have a significant amount of deletes together, which requires ~25% of rows to be deleted in adjacent row groups. An in-memory DuckDB database reclaims space the same way: checkpointing vacuums deleted rows and frees the memory they held (see [Checkpointing In-Memory DuckDB Databases](#checkpointing-in-memory-duckdb-databases) above).
