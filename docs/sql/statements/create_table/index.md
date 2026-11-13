---
title: CREATE TABLE
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE TABLE` statement creates a table in the catalog.

## Examples

Create a table with two integer columns (`i` and `j`):

<SqlLogicTest id="sql/statements/create_table/index/example_001" />

Create a table with a primary key:

<SqlLogicTest id="sql/statements/create_table/index/example_002" />

Create a table with a composite primary key:

<SqlLogicTest id="sql/statements/create_table/index/example_003" />

Create a table with various different types, constraints and default values:

<SqlLogicTest id="sql/statements/create_table/index/example_004" />

Create table with `CREATE TABLE ... AS SELECT` (CTAS):

<SqlLogicTest id="sql/statements/create_table/index/example_005" />

Create a table from a CSV file (automatically detecting column names and types):

<SqlLogicTest id="sql/statements/create_table/index/example_006" />

We can use the `FROM`-first syntax to omit `SELECT *`:

<SqlLogicTest id="sql/statements/create_table/index/example_007" />

Copy the schema of `t2` to `t1`:

<SqlLogicTest id="sql/statements/create_table/index/example_008" />

Note that only the column names and types are copied to `t1`, other pieces of information (indexes, constraints, default values, etc.) are not copied.

## Temporary Tables

Temporary tables are session scoped, meaning that only the specific connection that created them can access them and once the connection to SereneDB is closed they will be automatically dropped.

They can be created using the `CREATE TEMP TABLE` or the `CREATE TEMPORARY TABLE` statement (see diagram below) and are part of the `pg_temp` schema. While discouraged, their names can overlap with the names of the regular database tables. In these cases, temporary tables take priority in name resolution and full qualification is required to refer to a regular table, e.g., `public.t1`.

Temporary tables reside in memory rather than on disk, and their rows stay in memory even when memory becomes constrained.

Create a temporary table from a CSV file (automatically detecting column names and types):

<SqlLogicTest id="sql/statements/create_table/index/example_009" />

The `temp_directory` [setting](../../../configuration/overview.md) names the directory where queries spill intermediate results that do not fit in memory; it does not apply to temporary tables:

<SqlLogicTest id="sql/statements/create_table/index/example_010" />

## `CREATE OR REPLACE`

The `CREATE OR REPLACE` syntax allows a new table to be created or for an existing table to be overwritten by the new table. This is shorthand for dropping the existing table and then creating the new one.

Create a table with two integer columns (i and j) even if t1 already exists:

<SqlLogicTest id="sql/statements/create_table/index/example_011" />

## `IF NOT EXISTS`

The `IF NOT EXISTS` syntax will only proceed with the creation of the table if it does not already exist. If the table already exists, no action will be taken and the existing table will remain in the database.

Create a table with two integer columns (`i` and `j`) only if `t1` does not exist yet:

<SqlLogicTest id="sql/statements/create_table/index/example_012" />

## `CREATE TABLE ... AS SELECT` (CTAS)

SereneDB supports the `CREATE TABLE ... AS SELECT` syntax, also known as “CTAS”:

<SqlLogicTest id="sql/statements/create_table/index/example_013" />

This syntax can be used in combination with the [CSV reader](../../../data_import_and_export/csv/overview.md), the shorthand to read directly from CSV files without specifying a function, the [`FROM`-first syntax](../../query_syntax/from_and_join/index.md), and HTTP(S) support, yielding concise SQL commands such as the following:

<SqlLogicTest id="sql/statements/create_table/index/example_014" />

The CTAS construct also works with the `OR REPLACE` modifier, yielding `CREATE OR REPLACE TABLE ... AS` statements:

<SqlLogicTest id="sql/statements/create_table/index/example_015" />

### Copying the Schema

You can create a copy of the table's schema (column names and types only) as follows:

<SqlLogicTest id="sql/statements/create_table/index/example_016" />

Or:

<SqlLogicTest id="sql/statements/create_table/index/example_017" />

It is not possible to create tables using CTAS statements with constraints (primary keys, check constraints, etc.).

## Table Options

`CREATE TABLE ... WITH (option = value, ...)` sets how the table stores and maintains its rows and the [inverted indexes](../../indexes/inverted/index.md) on it:

| Option | Default | Description |
| :--- | :--- | :--- |
| `compaction_interval` | `1000` | Background compaction interval in milliseconds; `0` disables it |
| `compaction_max_segments` | `10` | Segments merged by one background compaction |
| `compaction_max_segments_bytes` | `5368709120` (5 GB) | Size budget of one background compaction |
| `compaction_floor_segment_bytes` | `2097152` (2 MB) | Segments smaller than this count as equal-sized when choosing what to merge |
| `segment_memory_max` | `268435456` (256 MB) | Committed rows, deletes and in-place updates the table holds in memory before writing them to disk; below 4 GiB |
| `row_group_size` | `122880` | Rows in a row group of the table's columns; a multiple of 2048 |
| `block_size` | `262144` | Size in bytes of the blocks the table's columns are compressed in; a power of two from 16384 to 262144 |
| `optimize_top_k` | — | Scorer expression enabling top-K (WAND) pruning, e.g. `'bm25(1.2, 0.75)'` |

The settings of the same names give the defaults of a new table, and the `default_block_size` setting that of `block_size`; `row_group_size` and `block_size` take the database's `ROW_GROUP_SIZE` and `BLOCK_SIZE` first (see [`CREATE DATABASE`](../create_database/index.md#storage-options)). The four `compaction_*` options can be changed later with [`ALTER TABLE ... SET` and `RESET`](../alter_table/index.md#set--reset-table-options); the others are fixed when the table is created. Any other option fails with `unrecognized parameter "<name>"`. See [Maintenance](../../indexes/inverted/maintenance.md) for what the options trade.

## Key Lookups

A filter that compares a single-column `PRIMARY KEY`, `UNIQUE` or `FOREIGN KEY` column with a value (`=`) or a list of values (`IN`) reads only the matching rows, whatever the column's type, instead of scanning the table. `EXPLAIN` shows it as an `Index Filter` on the column.

## Check Constraints

A `CHECK` constraint is an expression that must be satisfied by the values of every row in the table.

<SqlLogicTest id="sql/statements/create_table/index/example_018" />


<SqlLogicTest id="sql/statements/create_table/index/example_019" />


<SqlLogicTest id="sql/statements/create_table/index/example_020" />


`CHECK` constraints can also be added as part of the `CONSTRAINTS` clause:

<SqlLogicTest id="sql/statements/create_table/index/example_021" />


## Foreign Key Constraints

A `FOREIGN KEY` is a column (or set of columns) that references another table's primary key. Foreign keys check referential integrity, i.e., the referred primary key must exist in the other table upon insertion.

<SqlLogicTest id="sql/statements/create_table/index/example_022" />

Example:

<SqlLogicTest id="sql/statements/create_table/index/example_023" />


Foreign keys can be defined on composite primary keys:

<SqlLogicTest id="sql/statements/create_table/index/example_024" />

Example:

<SqlLogicTest id="sql/statements/create_table/index/example_025" />


A foreign key may reference either a `PRIMARY KEY` or a `UNIQUE` column of the referenced table:

<SqlLogicTest id="sql/statements/create_table/index/example_026" />


### Limitations

Foreign keys have the following limitations.

Referential actions are not supported: a `FOREIGN KEY` cannot use `ON DELETE` or `ON UPDATE` with `CASCADE`, `SET NULL` or `SET DEFAULT`.

<SqlLogicTest id="sql/statements/create_table/index/example_030" />

## Generated Columns

The `[type] [GENERATED ALWAYS] AS (expr) [VIRTUAL|STORED]` syntax will create a generated column. The data in this kind of column is generated from its expression, which can reference other (regular or generated) columns of the table. Since they are produced by calculations, these columns cannot be inserted into directly.

SereneDB can infer the type of the generated column based on the expression's return type. This allows you to leave out the type when declaring a generated column. It is possible to explicitly set a type, but insertions into the referenced columns might fail if the type cannot be cast to the type of the generated column.

Generated columns come in two varieties: `VIRTUAL` and `STORED`.
The data of virtual generated columns is not stored on disk, instead it is computed from the expression every time the column is referenced (through a select statement).

The data of stored generated columns is stored on disk and is computed every time the data of their dependencies change (through an `INSERT` / `UPDATE` / `DROP` statement).

Both `VIRTUAL` and `STORED` are accepted; `VIRTUAL` is the default when the last field is left blank.

The simplest syntax for a generated column:

The type is derived from the expression, and the variant defaults to `VIRTUAL`:

<SqlLogicTest id="sql/statements/create_table/index/example_027" />

Fully specifying the same generated column for completeness:

<SqlLogicTest id="sql/statements/create_table/index/example_028" />

A `STORED` generated column is also accepted; its value is likewise computed from the expression:

<SqlLogicTest id="sql/statements/create_table/index/example_029" />

### Compatibility with PostgreSQL

SereneDB follows PostgreSQL's generated-column behavior in most respects: `VIRTUAL` is the default (as in PostgreSQL 18), a generated column cannot be written to directly or given a `DEFAULT`, its expression may not contain a subquery, and a generated column can be indexed — including in an [inverted index](../../indexes/inverted/modeling.md).

Two expressions that PostgreSQL rejects are accepted by SereneDB:

-   the expression may **reference other generated columns**, not just regular columns of the same row;
-   the expression is **not required to be immutable** — volatile functions such as `random()` are accepted.

## Functions in Column Expressions

A user-defined function called in a `DEFAULT` value, a `CHECK` constraint or a generated column is looked up through the `search_path` of the statement that defines it, `CREATE TABLE` or `ALTER TABLE`. A function from a schema other than the table's own is kept with its schema: `CHECK (is_positive(v))` on a table in `public`, written while the function's schema `app` is on the `search_path`, is kept and shown as `CHECK (app.is_positive(v))`; a function from another database is kept with its database too. Writes, later `ALTER TABLE` statements of the table and a restart call that function whatever the session's `search_path` is. A built-in function or a function of the table's own schema stays the one the expression was written with: a temporary function or a function of a schema on the session's `search_path` with the same name doesn't replace it in writes, generated columns or indexes, and neither does a function with a built-in's name created later in the table's schema. A function of the table's schema named like a built-in is kept with its schema, since built-ins are looked up first. The `USING` expression of `ALTER COLUMN TYPE` and the `DEFAULT` of a field added to a struct column (`ADD COLUMN s.f ... DEFAULT ...`) are looked up the same way. A temporary function, such as one created with `CREATE TEMPORARY MACRO`, cannot be used in these expressions or in an index of a non-temporary table: the statement fails with `Cannot use temporary function "<name>" in a non-temporary relation`. A sequence named in `nextval`, `currval` or `setval`, also as `'<name>'::regclass`, is looked up the same way and kept with its schema when it isn't the one the table's schema holds: `DEFAULT nextval('ids')`, written while the sequence's schema `app` is on the `search_path`, is kept and shown as `nextval('app.ids')`. A temporary sequence can't be used in the expressions of a non-temporary table: the statement fails with `Cannot use temporary sequence "<name>" in a non-temporary relation`. A user-defined type named in such an expression, as in `DEFAULT 'a'::mood`, is not looked up through the `search_path`: it has to be in the table's schema or in `public`, or be written with its schema, as in `'a'::app.mood`; otherwise the statement fails with `Type with name mood does not exist!`. Column types are looked up through the `search_path` as usual.

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
