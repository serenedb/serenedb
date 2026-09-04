---
title: CHECKPOINT
draft: true
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `CHECKPOINT` statement synchronizes data in the write-ahead log (WAL) to the database data file.

## Examples

Synchronize data in the default database:

<SqlLogicTest id="sql/statements/checkpoint/index/example_001" />


Synchronize data in the specified database:

<SqlLogicTest id="sql/statements/checkpoint/index/example_002" />


Synchronize data and prevent new transactions from starting:

<SqlLogicTest id="sql/statements/checkpoint/index/example_003" />


## Checkpointing In-Memory Tables

In-memory tables support checkpointing. This has two key benefits:

-   In-memory tables also support compression. This is disabled by default – you can turn it on using:

    <SqlLogicTest id="sql/statements/checkpoint/index/example_004" />


-   Checkpointing triggers vacuuming deleted rows, allowing space to be reclaimed after deletes/truncation.

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

Checkpoint operations happen automatically based on the WAL size (see [Configuration](../../../configuration/overview.md)). This
statement is for manual checkpoint actions.

## Behavior

The default `CHECKPOINT` command will fail if there are any running transactions. Including `FORCE` will abort any
transactions and execute the checkpoint operation.

Also see the related [`PRAGMA` option](../../../configuration/pragmas.md#force-checkpoint) for further behavior modification.

### Reclaiming Space

When performing a checkpoint (automatic or otherwise), the space occupied by deleted rows is partially reclaimed. Note that this does not remove all deleted rows, but rather merges row groups that have a significant amount of deletes together. In the current implementation this requires ~25% of rows to be deleted in adjacent row groups.

An in-memory database reclaims space the same way: checkpointing vacuums deleted rows and frees the memory they held (see [Checkpointing In-Memory Tables](#checkpointing-in-memory-tables) above).

<DocCallout type="attention">
The [`VACUUM` statement](../vacuum/index.md) does _not_ trigger vacuuming deletes and hence does not reclaim space.
</DocCallout>
