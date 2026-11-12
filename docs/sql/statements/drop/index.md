---
title: DROP
split: headings
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

### Dependencies across Databases

A view or macro may read objects of another database, and a table may take a column default (`nextval`) or a column type from one. Those references are tracked the same way: dropping the referenced object with `RESTRICT` is rejected, and so is `DROP DATABASE` while an object of another database depends on one of its objects. `CASCADE` also drops a dependent view or macro in the other database; a dependent table stays, and a column default that used a dropped sequence is removed:

<SqlLogicTest id="sql/statements/drop/cross_database_dependency/example_013" />

`DROP DATABASE ... CASCADE` drops the dependents in other databases together with the database:

<SqlLogicTest id="sql/statements/drop/cross_database_dependency/example_015" />

The same holds between concurrent transactions: when one transaction drops an object and another adds a reference to it from another database, whichever commits second fails at `COMMIT` and changes nothing.

## Reclaiming Disk Space

`DROP TABLE`, `DROP INDEX` and `DROP DATABASE` remove the files of the dropped objects once the dropping transaction commits and no running query reads them; while a [backup](../../../operations/backup.md) runs, the files stay until `pg_backup_stop`. `PRAGMA database_size` reports, for each database, the size of its tables and indexes in `database_size`:

<SqlLogicTest id="sql/statements/drop/index/example_013" />

In an attached DuckDB database file, `DROP TABLE` marks the table's blocks as free without shrinking the file, and `PRAGMA database_size` counts them in `free_blocks`. To reclaim that space, use the `CHECKPOINT` statement or create a fresh copy with the [`COPY FROM DATABASE` statement](../copy/index.md#copy-from-database--to).
