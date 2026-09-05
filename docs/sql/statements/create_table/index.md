---
title: CREATE TABLE
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

Temporary tables are session scoped, meaning that only the specific connection that created them can access them and once the connection to SereneDB is closed they will be automatically dropped (similar to PostgreSQL, for example).

They can be created using the `CREATE TEMP TABLE` or the `CREATE TEMPORARY TABLE` statement (see diagram below) and are part of the `temp.main` schema. While discouraged, their names can overlap with the names of the regular database tables. In these cases, temporary tables take priority in name resolution and full qualification is required to refer to a regular table e.g., `memory.main.t1`.

Temporary tables reside in memory rather than on disk even when connecting to a persistent SereneDB, but if the `temp_directory` [configuration](../../../configuration/overview.md) is set, data will be spilled to disk if memory becomes constrained.

Create a temporary table from a CSV file (automatically detecting column names and types):

<SqlLogicTest id="sql/statements/create_table/index/example_009" />

Allow temporary tables to off-load excess memory to disk:

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

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
