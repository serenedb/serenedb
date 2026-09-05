---
title: DROP
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `DROP` statement removes a catalog entry added previously with the `CREATE` command.

## Examples

Delete the table with the name `tbl`:

<SqlLogicTest id="sql/statements/drop/index/example_001" />

Drop the view with the name `view1`; do not throw an error if the view does not exist:

<SqlLogicTest id="sql/statements/drop/index/example_002" />

Drop function `fn`:

<SqlLogicTest id="sql/statements/drop/index/example_003" />

Drop index `idx`:

<SqlLogicTest id="sql/statements/drop/index/example_004" />

Drop schema `sch`:

<SqlLogicTest id="sql/statements/drop/index/example_005" />

Drop sequence `seq`:

<SqlLogicTest id="sql/statements/drop/index/example_006" />

Drop macro `mcr`:

<SqlLogicTest id="sql/statements/drop/index/example_007" />

Drop macro table `mt`:

<SqlLogicTest id="sql/statements/drop/index/example_008" />

Drop type `typ`:

<SqlLogicTest id="sql/statements/drop/index/example_009" />

Drop text search dictionary `dict`:

<SqlLogicTest id="sql/statements/drop/index/example_014" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## Dependencies of Dropped Objects

SereneDB performs limited dependency tracking for some object types.
By default or if the `RESTRICT` clause is provided, the entry will not be dropped if there are any other objects that depend on it.
If the `CASCADE` clause is provided then all the objects that are dependent on the object will be dropped as well.

<SqlLogicTest id="sql/statements/drop/index/example_010" />

The `CASCADE` modifier drops both myschema and `myschema.t1`:

<SqlLogicTest id="sql/statements/drop/index/example_011" />

The following dependencies are tracked and thus will raise an error if the user tries to drop the depending object without the `CASCADE` modifier.

| Depending object type | Dependent object type |
| --------------------- | --------------------- |
| `SCHEMA`              | `FUNCTION`            |
| `SCHEMA`              | `MACRO TABLE`         |
| `SCHEMA`              | `MACRO`               |
| `SCHEMA`              | `SCHEMA`              |
| `SCHEMA`              | `SEQUENCE`            |
| `SCHEMA`              | `TABLE`               |
| `SCHEMA`              | `TYPE`                |
| `SCHEMA`              | `VIEW`                |
| `TABLE`               | `VIEW`                |
| `TEXT SEARCH DICTIONARY` | `INDEX`            |

### Dependencies on Views

Views that reference a table are tracked as dependents of that table. If a view references a table and the table is dropped with `RESTRICT` (the default), then the drop is rejected and the view stays valid:

<SqlLogicTest id="sql/statements/drop/view_dependency/example_012" />

## Limitations on Reclaiming Disk Space

Running `DROP TABLE` should free the memory used by the table, but not always disk space.
Even if disk space does not decrease, the free blocks will be marked as `free`.
For example, if we have a 2 GB file and we drop a 1 GB table, the file might still be 2 GB, but it should have 1 GB of free blocks in it.
To check this, use the following `PRAGMA` and check the number of `free_blocks` in the output:

<SqlLogicTest id="sql/statements/drop/index/example_013" />

To reclaim space after dropping a table, use the `CHECKPOINT` statement or compact the database by creating a fresh copy with the [`COPY FROM DATABASE` statement](../copy/index.md#copy-from-database--to).
