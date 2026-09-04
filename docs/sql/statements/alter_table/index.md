---
title: ALTER TABLE
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

The `RENAME TO` clause renames an entire table, changing its name in the schema. Note that any views that rely on the table are **not** automatically updated.

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

The `RENAME [COLUMN]` clause renames a single column within a table. Any constraints that rely on this name (e.g., `CHECK` constraints) are automatically updated. However, note that any views that rely on this column name are **not** automatically updated.

## `ADD COLUMN`

To add a column of a table, use the `ADD` or `ADD COLUMN` clauses.

E.g., to add a new column with name `k` to the table `integers`, it will be filled with the default value `NULL`:

<SqlLogicTest id="sql/statements/alter_table/index/example_017" />

Or:

<SqlLogicTest id="sql/statements/alter_table/index/example_018" />

Add a new column with name `l` to the table integers, it will be filled with the default value 10:

<SqlLogicTest id="sql/statements/alter_table/index/example_019" />

The `ADD [COLUMN]` clause can be used to add a new column of a specified type to a table. The new column will be filled with the specified default value, or `NULL` if none is specified.

## `DROP COLUMN`

To drop a column of a table, use the `DROP` or `DROP COLUMN` clause:

E.g., to drop the column `k` from the table `integers`:

<SqlLogicTest id="sql/statements/alter_table/index/example_020" />

Or:

<SqlLogicTest id="sql/statements/alter_table/index/example_021" />

The `DROP [COLUMN]` clause can be used to remove a column from a table. Note that columns can only be removed if they do not have any indexes that rely on them. This includes any indexes created as part of a `PRIMARY KEY` or `UNIQUE` constraint. Columns that are part of multi-column check constraints cannot be dropped either.
If you attempt to drop a column with an index on it, SereneDB returns a `Catalog Error` reporting that the column is referenced by that index or constraint.

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

## Limitations

`ALTER COLUMN` fails if values of conflicting types have occurred in the table at any point, even if they have been deleted:

<SqlLogicTest id="sql/statements/alter_table/type_conflict/example_032" />

Currently, this is expected behavior.
As a workaround, you can create a copy of the table:

<SqlLogicTest id="sql/statements/alter_table/copy_workaround/example_033" />
