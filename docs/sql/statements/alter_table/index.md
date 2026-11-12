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

The `RENAME TO` clause renames an entire table, changing its name in the schema. Indexes follow the rename, except in an [attached DuckDB file](../attach/duckdb.md), where a table that has an index can't be renamed. Views, functions and foreign keys that refer to the table follow the new name. A table that a trigger body names, its own triggers' included, can't be renamed: the error lists those triggers, which have to be dropped first and recreated afterwards.

<DocCallout type="tip">
    `ALTER TABLE` changes the schema of an existing table.
</DocCallout>

<!--
All the changes made by `ALTER TABLE` fully respect the transactional semantics, i.e., they will not be visible to other transactions until committed, and can be fully reverted through a rollback.
-->

## `SET SCHEMA`

<SqlLogicTest id="sql/statements/alter_table/index/example_038" />

The `SET SCHEMA` clause moves a table into another schema, keeping its rows, together with its indexes, its triggers and the sequences it owns, such as those of its `SERIAL` columns. `ALTER TABLE ... SET SCHEMA` also moves a view or a sequence; given an index, it fails with `cannot change schema of index`. The target schema must exist, must not be a system schema such as `pg_catalog`, and must not already hold a relation of that name, and moving a table needs the `CREATE` privilege on the target schema. Moving a table into the schema it is already in changes nothing. Views, functions, column defaults and foreign keys that refer to the moved table, view or sequence follow it into the new schema; a trigger body that names it blocks the move, the same as for `RENAME TO`. A table whose `CHECK` constraint, `DEFAULT` or generated column calls a function of its own schema by its name alone can't be moved: the statement fails with `Cannot move table "<table>" to schema "<schema>": its expressions call function "<name>", which would resolve to a different function there`. The same holds for a sequence of the table's schema that the table doesn't own, named by its name alone as in `nextval('order_ids')`: the statement fails with `Cannot move table "<table>" to schema "<schema>": its expressions use sequence "<name>", which would resolve to a different sequence there`. [`ALTER VIEW`](../alter_view.md) and [`ALTER SEQUENCE`](../create_sequence/index.md#moving-a-sequence) move views and sequences, and [`ALTER TYPE`](../create_type/index.md), [`ALTER FUNCTION`](../create_macro/index.md#moving-to-another-schema) and [`ALTER TEXT SEARCH DICTIONARY`](../create_text_search_dictionary/index.md) move types, functions and dictionaries; a sequence that a table owns moves only with its table, and an index always stays in its table's schema.

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

A [serial column](../../data_types/index.md#general-purpose-data-types) gets a sequence of its own, every row already in the table gets a number from it, and new rows continue from there:

<SqlLogicTest id="sql/statements/alter_table/index/add_column_serial" />

With `IF NOT EXISTS`, adding a serial column that already exists changes nothing and creates no sequence. A serial column can declare `PRIMARY KEY` or `UNIQUE` in the same statement, also on a table that already has rows.

## `DROP COLUMN`

To drop a column of a table, use the `DROP` or `DROP COLUMN` clause:

E.g., to drop the column `k` from the table `integers`:

<SqlLogicTest id="sql/statements/alter_table/index/example_020" />

Or:

<SqlLogicTest id="sql/statements/alter_table/index/example_021" />

The `DROP [COLUMN]` clause can be used to remove a column from a table. As in PostgreSQL, every index created with `CREATE INDEX` that uses the column (as a key, in an indexed expression or in its `WHERE` predicate) is dropped along with it, and so are the `CHECK` and `FOREIGN KEY` constraints of the table that use it (see [Schema changes on an indexed table](../../indexes/inverted/maintenance.md#schema-changes-on-an-indexed-table)). A column of a `PRIMARY KEY` or `UNIQUE` constraint cannot be removed: the statement fails with `Cannot drop column ⟨name⟩ because there is a PRIMARY KEY constraint that depends on it`, or `UNIQUE` for a unique constraint. In an [attached DuckDB file](../attach/duckdb.md), a column also cannot be removed while an index uses a column that comes after it.

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

The `[SET [DATA]] TYPE` clause changes the type of a column in a table. Any data present in the column is converted according to the provided expression in the `USING` clause, or, if the `USING` clause is absent, cast to the new data type. Note that a column can only have its type changed if no index and no `CHECK`, `UNIQUE`, `PRIMARY KEY` or `FOREIGN KEY` constraint uses it.

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

The statement fails if the table already has a primary key (`table "⟨name⟩" can have only one primary key`), or if the existing rows would violate the new constraint: duplicates fail with `could not create unique index "⟨name⟩_pkey"`, and `NULL` values with `column "⟨column⟩" of relation "⟨name⟩" contains null values`. A table with indexes takes a primary key like any other.

## `SET` / `RESET` (Table Options)

`SET (option = value, …)` changes the background maintenance options of a table, and `RESET (option, …)` returns them to the current session defaults. These options can be changed: `compaction_interval`, `compaction_max_segments`, `compaction_max_segments_bytes` and `compaction_floor_segment_bytes` (see [Background compaction](../../indexes/inverted/maintenance.md#background-compaction)). A change reaches the table's background tasks at once and is undone if its transaction rolls back. `row_group_size`, `block_size`, `segment_memory_max` and `optimize_top_k` are fixed at [`CREATE TABLE`](../create_table/index.md#table-options): setting or resetting one fails with `option "⟨name⟩" cannot be changed with ALTER TABLE`. `pg_class.reloptions` lists the options whose values differ from the defaults.

<SqlLogicTest id="sql/statements/alter_table/index/example_036" />

An option that doesn't exist is an error:

<SqlLogicTest id="sql/statements/alter_table/index/example_029" />

<SqlLogicTest id="sql/statements/alter_table/index/example_030" />

A statement that sets or resets several options fails as a whole when one of them doesn't exist:

<SqlLogicTest id="sql/statements/alter_table/index/example_031" />

## `DROP CONSTRAINT`

The `DROP CONSTRAINT` clause removes a named constraint from a table, a `CHECK`, `UNIQUE`, `PRIMARY KEY` or `FOREIGN KEY` constraint alike:

<SqlLogicTest id="sql/statements/alter_table/index/example_034" />

A `PRIMARY KEY` or `UNIQUE` constraint that a foreign key references can't be dropped: the statement fails with `cannot drop constraint ⟨name⟩ on table ⟨table⟩ because other objects depend on it`. `DROP CONSTRAINT ... CASCADE` drops the referencing foreign keys too. `RENAME CONSTRAINT` renames a constraint.

## `ADD CONSTRAINT`

The `ADD CONSTRAINT` clause adds a `CHECK`, `UNIQUE`, `PRIMARY KEY` or `FOREIGN KEY` constraint to an existing table:

<SqlLogicTest id="sql/statements/alter_table/index/example_035" />

The rows already in the table are checked against the new constraint: a duplicate key fails the statement with `could not create unique index "⟨name⟩"`, and a foreign key value missing from the referenced table with the error an `INSERT` of that row would raise.

## `ENABLE` / `DISABLE TRIGGER`

`DISABLE TRIGGER name` stops a trigger of the table from firing, and `ENABLE TRIGGER name` lets it fire again. With `ALL` or `USER` instead of a name the change applies to every trigger of the table. Like in PostgreSQL, a trigger can also be enabled for a [replication role](../../../configuration/overview.md): `ENABLE REPLICA TRIGGER` makes it fire only in sessions where `session_replication_role` is `replica`, such as the apply worker of a [subscription](../create_subscription/index.md), and `ENABLE ALWAYS TRIGGER` makes it fire in every session. A plain `ENABLE TRIGGER` trigger fires when the role is `origin` (the default) or `local`. `pg_trigger.tgenabled` shows the state as `O`, `D`, `R` or `A`. The change is transactional.

<SqlLogicTest id="sql/statements/alter_table/index/example_037" />

Foreign keys are not triggers in SereneDB, so `DISABLE TRIGGER ALL` does not turn off foreign key checks; a session with `session_replication_role` set to `replica` skips them.

## Rewrites and concurrent changes

Renaming the table or a column, `ALTER COLUMN SET DEFAULT` / `DROP DEFAULT`, `DROP NOT NULL`, `DROP CONSTRAINT` and `RENAME CONSTRAINT` of a `CHECK` or `NOT NULL` constraint, and `COMMENT ON COLUMN` change only the table's definition.

`ADD COLUMN` without a default, or with `DEFAULT NULL`, gives the existing rows `NULL`. With any other default, the existing rows get the default, evaluated per row, and the statement rewrites the table. A generated column can't be added.

`DROP COLUMN` drops the indexes on the column and the `CHECK` and `FOREIGN KEY` constraints that use it. A column of a `PRIMARY KEY` or `UNIQUE` constraint can't be dropped.

`ALTER COLUMN TYPE` rewrites the table, converting each value with the `USING` expression, or with a cast when there is none. It is refused for a column of a `PRIMARY KEY`, `UNIQUE` or `FOREIGN KEY` constraint, a column an index covers, a column a `CHECK` constraint or a generated column uses, and a generated column. If the column is `NOT NULL`, every converted value must be non-`NULL`.

Every row written is checked against the `NOT NULL` and `CHECK` constraints. `SET NOT NULL` and adding a `CHECK` constraint fail if a row of the table violates the new constraint.

`ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`, `SET NOT NULL`, adding a `CHECK`, `UNIQUE`, `PRIMARY KEY` or `FOREIGN KEY` constraint and dropping a `UNIQUE`, `PRIMARY KEY` or `FOREIGN KEY` constraint wait for the transactions that wrote to the table before them to end. They are refused in a transaction that has already written to the table. While such an `ALTER TABLE` is uncommitted, and in transactions whose snapshot predates its commit, `INSERT` and `UPDATE` of the table fail with `table "<name>" was altered by a concurrent transaction`; retry the transaction. An `ALTER TABLE` holds the table until its transaction ends, like a change of its indexes; see [Concurrent changes](../create_index/inverted.md#concurrent-changes). An `ALTER TABLE` that rewrites the table waits for the table's running compactions and holds off new ones the way `CREATE INDEX` does.
