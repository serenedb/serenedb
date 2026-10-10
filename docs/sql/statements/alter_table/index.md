---
title: ALTER TABLE
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";


The `ALTER TABLE` statement changes the schema of an existing table in the catalog.

<!--

## Examples

<SqlLogicTest id="sql/statements/alter_table/index/example_001" />

Add a new column with name `k` to the table `integers`, it will be filled with the default value `NULL`:

<SqlLogicTest id="sql/statements/alter_table/index/example_002" />

Add a new column with name `l` to the table integers, it will be filled with the default value 10:

<SqlLogicTest id="sql/statements/alter_table/index/example_003" />

Drop the column `k` from the table integers:

<SqlLogicTest id="sql/statements/alter_table/index/example_004" />

Change the type of the column `i` to the type `VARCHAR` using a standard cast:

<SqlLogicTest id="sql/statements/alter_table/index/example_005" />

Change the type of the column `i` to the type `VARCHAR`, using the specified expression to convert the data for each row:

<SqlLogicTest id="sql/statements/alter_table/index/example_006" />

Set the default value of a column:

<SqlLogicTest id="sql/statements/alter_table/index/example_007" />

Drop the default value of a column:

<SqlLogicTest id="sql/statements/alter_table/index/example_008" />

Make a column not nullable:

<SqlLogicTest id="sql/statements/alter_table/index/example_009" />

Drop the not-`NULL` constraint:

<SqlLogicTest id="sql/statements/alter_table/index/example_010" />

Rename a table:

<SqlLogicTest id="sql/statements/alter_table/index/example_011" />

Rename a column of a table:

<SqlLogicTest id="sql/statements/alter_table/index/example_012" />

Add a primary key to a column of a table:

<SqlLogicTest id="sql/statements/alter_table/index/example_013" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

-->

## `RENAME TABLE`

<SqlLogicTest id="sql/statements/alter_table/index/example_014" />

The `RENAME TO` clause renames an entire table, changing its name in the schema. Indexes follow the rename, except in an [attached DuckDB file](../attach/duckdb.md), where a table that has an index can't be renamed. A table that a view, a function or another table's `DEFAULT`, `CHECK` or generated column uses can't be renamed: the error lists those dependents, which have to be dropped first and recreated afterwards.

<DocCallout type="tip">
    `ALTER TABLE` changes the schema of an existing table.
</DocCallout>

<!--
All the changes made by `ALTER TABLE` fully respect the transactional semantics, i.e., they will not be visible to other transactions until committed, and can be fully reverted through a rollback.
-->

## `RENAME COLUMN`

To rename a column of a table, use the `RENAME` or `RENAME COLUMN` clauses:

<SqlLogicTest id="sql/statements/alter_table/rename_column/example_015" />

<SqlLogicTest id="sql/statements/alter_table/rename_column_short/example_016" />

The `RENAME [COLUMN]` clause renames a single column within a table. Constraints and indexes that use the column are updated automatically; in an [attached DuckDB file](../attach/duckdb.md), a table that has an index can't rename its columns. A column that a view or a table function reads can't be renamed, and neither can its type be changed; the error lists the dependents. A column that no dependent reads can be renamed or retyped freely.

## `ADD COLUMN`

To add a column of a table, use the `ADD` or `ADD COLUMN` clauses.

E.g., to add a new column with name `k` to the table `integers`, it will be filled with the default value `NULL`:

<SqlLogicTest id="sql/statements/alter_table/index/example_017" />

Or:

<SqlLogicTest id="sql/statements/alter_table/index/example_018" />

Add a new column with name `l` to the table integers, it will be filled with the default value 10:

<SqlLogicTest id="sql/statements/alter_table/index/example_019" />

The `ADD [COLUMN]` clause can be used to add a new column of a specified type to a table. The new column will be filled with the specified default value, or `NULL` if none is specified. Besides `DEFAULT`, the new column can declare `UNIQUE` or `PRIMARY KEY`, and the constraint is enforced from the start. A `PRIMARY KEY` is also checked against the rows already in the table, which all get the default value: adding `id INT DEFAULT 5 PRIMARY KEY` to a table of two rows fails because the key contains duplicates. An inline `CHECK` constraint and a named constraint (`CONSTRAINT ⟨name⟩ UNIQUE`) are not supported; add them with [`ADD CONSTRAINT`](#add-constraint), and `NOT NULL` with `ALTER COLUMN ... SET NOT NULL`, in a separate `ALTER TABLE` step.

## `DROP COLUMN`

To drop a column of a table, use the `DROP` or `DROP COLUMN` clause:

E.g., to drop the column `k` from the table `integers`:

<SqlLogicTest id="sql/statements/alter_table/index/example_020" />

Or:

<SqlLogicTest id="sql/statements/alter_table/index/example_021" />

The `DROP [COLUMN]` clause can be used to remove a column from a table. As in PostgreSQL, every index created with `CREATE INDEX` that uses the column (as a key, in an indexed expression or in its `WHERE` predicate) is dropped along with it. A column cannot be removed while an index created as part of a `PRIMARY KEY` or `UNIQUE` constraint relies on it, or while a remaining plain index (not an [inverted index](../../indexes/inverted/maintenance.md#schema-changes-on-an-indexed-table)) uses a column that comes after it. Columns that are part of multi-column check constraints cannot be dropped either.
In those cases SereneDB returns a `Catalog Error` reporting that an index or constraint depends on the column.

## `[SET [DATA]] TYPE`

Change the type of the column `i` to the type `VARCHAR` using a standard cast:

<SqlLogicTest id="sql/statements/alter_table/index/example_022" />

<DocCallout type="pin">
Instead of `ALTER ⟨column_name⟩ TYPE ⟨type⟩`, you can also use the equivalent
`ALTER ⟨column_name⟩ SET TYPE ⟨type⟩` and the
`ALTER ⟨column_name⟩ SET DATA TYPE ⟨type⟩` clauses.
</DocCallout>

Change the type of the column `i` to the type `VARCHAR`, using the specified expression to convert the data for each row:

<SqlLogicTest id="sql/statements/alter_table/index/example_023" />

The `[SET [DATA]] TYPE` clause changes the type of a column in a table. Any data present in the column is converted according to the provided expression in the `USING` clause, or, if the `USING` clause is absent, cast to the new data type. Note that columns can only have their type changed if they do not have any indexes that rely on them and are not part of any `CHECK` constraints.

### Handling Structs

There are two options to change the sub-schema of a [`STRUCT`](../../data_types/struct.md)-typed column.

#### `ALTER TABLE` with `struct_insert`

You can add fields to a `STRUCT` column with `ALTER TABLE`: give the new struct type in the `TYPE` clause and use `struct_insert` in the `USING` clause to transform the existing values.
For example:

<SqlLogicTest id="sql/statements/alter_table/struct_insert/example_024" />

#### `ALTER TABLE` with `ADD COLUMN` / `DROP COLUMN` / `RENAME COLUMN`

SereneDB `ALTER TABLE` supports the
[`ADD COLUMN`, `DROP COLUMN` and `RENAME COLUMN` clauses](../../data_types/struct.md#updating-the-schema)
to update the sub-schema of a `STRUCT`.

## `SET` / `DROP DEFAULT`

The `SET DEFAULT` clause changes the default value of a column:

<SqlLogicTest id="sql/statements/alter_table/index/example_025" />

The `DROP DEFAULT` clause removes the default value of a column, resetting it to `NULL`:

<SqlLogicTest id="sql/statements/alter_table/index/example_026" />

## `ADD PRIMARY KEY`

The `ADD PRIMARY KEY` clause promotes one or more existing columns to the table's primary key. The chosen columns are made implicitly `NOT NULL`, and the constraint is enforced from that point on:

<SqlLogicTest id="sql/statements/alter_table/index/example_027" />

A primary key can also span multiple columns:

<SqlLogicTest id="sql/statements/alter_table/index/example_028" />

The statement fails if the table already has a primary key, if an index depends on the table or if the existing data would violate the new constraint (duplicate or `NULL` values in the key columns).

## `SET` / `RESET` (Table Options)

<DocCallout type="tip">
The `SET` and `RESET` table-option clauses are not yet supported in SereneDB.
</DocCallout>

Attempting to set table options returns an error:

<SqlLogicTest id="sql/statements/alter_table/index/example_029" />

Attempting to reset table options returns an error:

<SqlLogicTest id="sql/statements/alter_table/index/example_030" />

Attempting to set or reset multiple options in a single statement returns an error:

<SqlLogicTest id="sql/statements/alter_table/index/example_031" />

## `DROP CONSTRAINT`

The `DROP CONSTRAINT` clause removes a named `CHECK` constraint from a table:

<SqlLogicTest id="sql/statements/alter_table/index/example_034" />

`DROP CONSTRAINT` (and `RENAME CONSTRAINT`) operate on named `CHECK` constraints. Index-backed constraints — those created by `PRIMARY KEY` and `UNIQUE` — cannot be dropped this way.

## `ADD CONSTRAINT`

The `ADD CONSTRAINT` clause adds a `CHECK`, `UNIQUE` or `PRIMARY KEY` constraint to an existing table:

<SqlLogicTest id="sql/statements/alter_table/index/example_035" />

`FOREIGN KEY` constraints cannot be added with `ADD CONSTRAINT`.

## `SET` / `RESET` storage options

For a table created with `WITH (storage = 'search')`, `SET (option = value, …)` changes the background maintenance options it was created with, and `RESET (option, …)` returns them to the current session defaults. These options can be changed: `refresh_interval`, `compaction_interval`, `cleanup_interval_step`, `compaction_max_segments`, `compaction_max_segments_bytes` and `compaction_floor_segment_bytes` (see [Background compaction](../../indexes/inverted/maintenance.md#background-compaction)). A change reaches the table's background tasks at once and is undone if its transaction rolls back. `row_group_size`, `segment_memory_max` and `optimize_top_k` are fixed at `CREATE TABLE`. The current values are listed in `pg_class.reloptions`.

<SqlLogicTest id="sql/statements/alter_table/index/example_036" />

`SET` and `RESET` of storage options are supported only for search tables.

## `ENABLE` / `DISABLE TRIGGER`

`DISABLE TRIGGER name` stops a trigger of the table from firing, and `ENABLE TRIGGER name` lets it fire again. With `ALL` or `USER` instead of a name the change applies to every trigger of the table. Like in PostgreSQL, a trigger can also be enabled for a [replication role](../../../configuration/overview.md): `ENABLE REPLICA TRIGGER` makes it fire only in sessions where `session_replication_role` is `replica`, such as the apply worker of a [subscription](../create_subscription/index.md), and `ENABLE ALWAYS TRIGGER` makes it fire in every session. A plain `ENABLE TRIGGER` trigger fires when the role is `origin` (the default) or `local`. `pg_trigger.tgenabled` shows the state as `O`, `D`, `R` or `A`. The change is transactional.

<SqlLogicTest id="sql/statements/alter_table/index/example_037" />

Foreign keys are not triggers in SereneDB, so `DISABLE TRIGGER ALL` does not turn off foreign key checks; a session with `session_replication_role` set to `replica` skips them.

## Search tables

A search table's columns are fixed: `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`, `DROP CONSTRAINT` and adding a `PRIMARY KEY` or `UNIQUE` constraint are rejected. Renaming the table or a column, `ALTER COLUMN SET DEFAULT` / `DROP DEFAULT`, `SET NOT NULL` / `DROP NOT NULL`, adding a `CHECK` constraint and `COMMENT ON COLUMN` change only the table's definition. A search table does not check `NOT NULL` and `CHECK` constraints when rows are written, whether they were declared at `CREATE TABLE` or added later.

## Limitations

`ALTER COLUMN` fails if values of conflicting types have occurred in the table at any point, even if they have been deleted:

<SqlLogicTest id="sql/statements/alter_table/type_conflict/example_032" />

Currently, this is expected behavior.
As a workaround, you can create a copy of the table:

<SqlLogicTest id="sql/statements/alter_table/copy_workaround/example_033" />
