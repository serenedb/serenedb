---
title: INSERT
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `INSERT` statement inserts new data into a table.

## Examples

Insert the values 1, 2, 3 into `tbl`:

<SqlLogicTest id="sql/statements/insert/index/example_001" />

Insert the result of a query into a table:

<SqlLogicTest id="sql/statements/insert/index/example_002" />

Insert values into the `i` column, inserting the default value into other columns:

<SqlLogicTest id="sql/statements/insert/index/example_003" />

Explicitly insert the default value into a column:

<SqlLogicTest id="sql/statements/insert/index/example_004" />

Assuming `tbl` has a primary key/unique constraint, do nothing on conflict:

<SqlLogicTest id="sql/statements/insert/index/example_005" />

Or update the table with the new values instead:

<SqlLogicTest id="sql/statements/insert/index/example_006" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

`INSERT INTO` inserts new rows into a table. One can insert one or more rows specified by value expressions, or zero or more rows resulting from a query.

## Insert Column Order

It's possible to provide an optional insert column order, this can either be `BY POSITION` (the default) or `BY NAME`.
Each column not present in the explicit or implicit column list will be filled with a default value, either its declared default value or `NULL` if there is none.

If the expression for any column is not of the correct data type, automatic type conversion will be attempted.

### `INSERT INTO ... [BY POSITION]`

The order that values are inserted into the columns of the table is determined by the order that the columns were declared in.
That is, the values supplied by the `VALUES` clause or query are associated with the column list left-to-right.
This is the default option, that can be explicitly specified using the `BY POSITION` option.
For example:

<SqlLogicTest id="sql/statements/insert/by_position/example_007" />

Specifying `BY POSITION` is optional and is equivalent to the default behavior:

<SqlLogicTest id="sql/statements/insert/by_position_explicit/example_008" />

To use a different order, column names can be provided as part of the target, for example:

<SqlLogicTest id="sql/statements/insert/column_order/example_009" />

Adding `BY POSITION` results in the same behavior:

<SqlLogicTest id="sql/statements/insert/column_order_by_position/example_010" />

This will insert `5` into `b` and `42` into `a`.

### `INSERT INTO ... BY NAME`

Using the `BY NAME` modifier, the names of the column list of the `SELECT` statement are matched against the column names of the table to determine the order that values should be inserted into the table. This allows inserting even in cases when the order of the columns in the table differs from the order of the values in the `SELECT` statement or certain columns are missing.

For example:

<SqlLogicTest id="sql/statements/insert/by_name/example_011" />

It's important to note that when using `INSERT INTO ... BY NAME`, the column names specified in the `SELECT` statement must match the column names in the table. If a column name is misspelled or does not exist in the table, an error will occur. Columns that are missing from the `SELECT` statement will be filled with the default value.

## `ON CONFLICT` Clause

An `ON CONFLICT` clause can be used to perform a certain action on conflicts that arise from `UNIQUE` or `PRIMARY KEY` constraints.
An example for such a conflict is shown in the following example:

<SqlLogicTest id="sql/statements/insert/conflict_error/example_012" />

This raises a constraint error. The table will contain the row that was first inserted:

<SqlLogicTest id="sql/statements/insert/conflict_error_result/example_013" />

These error messages can be avoided by explicitly handling conflicts.
SereneDB supports two such clauses: [`ON CONFLICT DO NOTHING`](#do-nothing-clause) and [`ON CONFLICT DO UPDATE SET ...`](#do-update-clause-upsert).

### `DO NOTHING` Clause

The `DO NOTHING` clause causes the error(s) to be ignored, and the values are not inserted or updated.
For example:

<SqlLogicTest id="sql/statements/insert/do_nothing/example_014" />

These statements finish successfully and leave the table with the row `<i: 1, j: 42>`.

#### `INSERT OR IGNORE INTO`

The `INSERT OR IGNORE INTO ...` statement is a shorter syntax alternative to `INSERT INTO ... ON CONFLICT DO NOTHING`.
For example, the following statements are equivalent:

<SqlLogicTest id="sql/statements/insert/do_nothing_equivalent/example_015" />

### `DO UPDATE` Clause (Upsert)

The `DO UPDATE` clause causes the `INSERT` to turn into an `UPDATE` on the conflicting row(s) instead.
The `SET` expressions that follow determine how these rows are updated. The expressions can use the special virtual table `EXCLUDED`, which contains the conflicting values for the row.
Optionally you can provide an additional `WHERE` clause that can exclude certain rows from the update.
The conflicts that don't meet this condition are ignored instead.

Because we need a way to refer to both the **to-be-inserted** tuple and the **existing** tuple, we introduce the special `EXCLUDED` qualifier.
When the `EXCLUDED` qualifier is provided, the reference refers to the **to-be-inserted** tuple, otherwise, it refers to the **existing** tuple.
This special qualifier can be used within the `WHERE` clauses and `SET` expressions of the `ON CONFLICT` clause.

<SqlLogicTest id="sql/statements/insert/do_update_clause/example_016" />

#### Examples

An example using `DO UPDATE` is the following:

<SqlLogicTest id="sql/statements/insert/do_update_example/example_017" />

Rearranging columns and using `BY NAME` is also possible:

<SqlLogicTest id="sql/statements/insert/do_update_by_name/example_018" />

#### `INSERT OR REPLACE INTO`

The `INSERT OR REPLACE INTO ...` statement is a shorter syntax alternative to `INSERT INTO ... DO UPDATE SET c1 = EXCLUDED.c1, c2 = EXCLUDED.c2, ...`.
That is, it updates every column of the **existing** row to the new values of the **to-be-inserted** row.
For example, given the following input table:

<SqlLogicTest id="sql/statements/insert/insert_or_replace/example_019" />

These statements are equivalent:

<SqlLogicTest id="sql/statements/insert/insert_or_replace_equivalent/example_020" />

#### Limitations

When the `ON CONFLICT ... DO UPDATE` clause is used and a conflict occurs, only the columns named in the `SET` clause are updated. Columns that are unaffected by the conflict keep their existing values, so their `NOT NULL` constraints continue to hold. For example, the following upsert updates `val1` while preserving the existing `val2`:

<SqlLogicTest id="sql/statements/insert/index/example_021" />

#### Composite Primary Key

When multiple columns need to be part of the uniqueness constraint, use a single `PRIMARY KEY` clause including all relevant columns:

<SqlLogicTest id="sql/statements/insert/index/example_022" />

### Defining a Conflict Target

A conflict target may be provided as `ON CONFLICT (conflict_target)`. This is a group of columns that an index or uniqueness/key constraint is defined on. If the conflict target is omitted, the `PRIMARY KEY` constraint(s) on the table are targeted.

Specifying a conflict target is optional unless using a [`DO UPDATE`](#do-update-clause-upsert) and there are multiple unique/primary key constraints on the table.

<SqlLogicTest id="sql/statements/insert/conflict_target/example_023" />

Targeting the `PRIMARY KEY` column `i` resolves the conflict and updates the row:

<SqlLogicTest id="sql/statements/insert/conflict_target_i/example_024" />

The conflict target can also be backed by a `UNIQUE` constraint. Targeting the `UNIQUE` column `j` likewise resolves the conflict and updates the row:

<SqlLogicTest id="sql/statements/insert/conflict_target_j/example_025" />

When a conflict target is provided, you can further filter this with a `WHERE` clause, that should be met by all conflicts.

<SqlLogicTest id="sql/statements/insert/conflict_target_where/example_026" />

## `RETURNING` Clause

The `RETURNING` clause may be used to return the contents of the rows that were inserted. This can be useful if some columns are calculated upon insert. For example, if the table contains an automatically incrementing primary key, then the `RETURNING` clause will include the automatically created primary key. This is also useful in the case of generated columns.

Some or all columns can be explicitly chosen to be returned and they may optionally be renamed using aliases. Arbitrary non-aggregating expressions may also be returned instead of simply returning a column. All columns can be returned using the `*` expression, and columns or expressions can be returned in addition to all columns returned by the `*`.

For example:

<SqlLogicTest id="sql/statements/insert/index/example_027" />

A more complex example that includes an expression in the `RETURNING` clause:

<SqlLogicTest id="sql/statements/insert/index/example_028" />

The next example shows a situation where the `RETURNING` clause is more helpful. First, a table is created with a primary key column. Then a sequence is created to allow for that primary key to be incremented as new rows are inserted. When we insert into the table, we do not already know the values generated by the sequence, so it is valuable to return them. For additional information, see the [`CREATE SEQUENCE` page](../../statements/create_sequence/index.md).

<SqlLogicTest id="sql/statements/insert/index/example_029" />
