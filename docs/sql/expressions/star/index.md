---
title: Star Expression
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

The `*` expression can be used in a `SELECT` statement to select all columns that are projected in the `FROM` clause.

<SqlLogicTest id="sql/expressions/star/index/example_001" />

### `TABLE.*` and `STRUCT.*`

The `*` expression can be prepended by a table name to select only columns from that table.

<SqlLogicTest id="sql/expressions/star/index/example_002" />

Similarly, the `*` expression can also be used to retrieve all keys from a struct as separate columns.
This is particularly useful when a prior operation creates a struct of unknown shape, or if a query must handle any potential struct keys.
See the [`STRUCT` data type](../../../sql/data_types/struct.md) and [`STRUCT` functions](../../../sql/functions/struct.md) pages for more details on working with structs.

For example:

<SqlLogicTest id="sql/expressions/star/index/example_003" />

### `EXCLUDE` Clause

`EXCLUDE` allows you to exclude specific columns from the `*` expression.

<SqlLogicTest id="sql/expressions/star/index/example_004" />

### `REPLACE` Clause

`REPLACE` allows you to replace specific columns by alternative expressions.

<SqlLogicTest id="sql/expressions/star/index/example_005" />

### `RENAME` Clause

`RENAME` allows you to replace specific columns.

<SqlLogicTest id="sql/expressions/star/index/example_006" />

### Column Filtering via Pattern Matching Operators

The [pattern matching operators](../../../sql/functions/pattern_matching/index.md) `LIKE` and `GLOB` allow you to select columns by matching their names to patterns.

<SqlLogicTest id="sql/expressions/star/index/example_007" />

<SqlLogicTest id="sql/expressions/star/index/example_008" />

The pattern applied to a star expression must be a constant, and only `LIKE` and `GLOB` are supported. The `SIMILAR TO` operator is not supported on a star expression and raises an error:

<SqlLogicTest id="sql/expressions/star/index/example_009" />

The `NOT` variant behaves the same way and also raises an error:

<SqlLogicTest id="sql/expressions/star/index/example_010" />

## `COLUMNS` Expression

The `COLUMNS` expression is similar to the regular star expression, but additionally allows you to execute the same expression on the resulting columns.

<SqlLogicTest id="sql/expressions/star/index/example_011" />

<SqlLogicTest id="sql/expressions/star/index/example_012" />

`COLUMNS` expressions can also be combined, as long as they contain the same star expression:

<SqlLogicTest id="sql/expressions/star/index/example_013" />

### `COLUMNS` Expression in a `WHERE` Clause

`COLUMNS` expressions can also be used in `WHERE` clauses. The conditions are applied to all columns and are combined using the logical `AND` operator.

<SqlLogicTest id="sql/expressions/star/index/example_014" />

To combine conditions using the logical `OR` operator, you can `UNPACK` the `COLUMNS` expression into the variadic `greatest` function.

<SqlLogicTest id="sql/expressions/star/index/example_015" />

### `COLUMNS` Expression in `DISTINCT ON`

`COLUMNS` expressions can be used in [`DISTINCT ON`](../../../sql/query_syntax/select/index.md#distinct-on-clause) clauses to specify distinct columns by pattern:

<SqlLogicTest id="sql/expressions/star/index/example_016" />

### Regular Expressions in a `COLUMNS` Expression

`COLUMNS` expressions don't currently support the pattern matching operators, but they do support regular expression matching by simply passing a string constant in place of the star:

<SqlLogicTest id="sql/expressions/star/index/example_017" />

### Renaming Columns with Regular Expressions in a `COLUMNS` Expression

The matches of capture groups in regular expressions can be used to rename matching columns.
The capture groups are one-indexed; `\0` is the original column name.

For example, to select the first three letters of column names, run:

<SqlLogicTest id="sql/expressions/star/index/example_018" />

To remove a colon (`:`) character in the middle of a column name, run:

<SqlLogicTest id="sql/expressions/star/regex_rename_colon/example_019" />

To add the original column name to the expression alias, run:

<SqlLogicTest id="sql/expressions/star/index/example_020" />

### `COLUMNS` Lambda Function

`COLUMNS` also supports passing in a lambda function. The lambda function will be evaluated for all columns present in the `FROM` clause, and only columns that match the lambda function will be returned. This allows the execution of arbitrary expressions in order to select and rename columns.

<SqlLogicTest id="sql/expressions/star/index/example_021" />

### `COLUMNS` List

`COLUMNS` also supports passing in a list of column names.

<SqlLogicTest id="sql/expressions/star/index/example_022" />

## Unpacking a `COLUMNS` Expression

By wrapping a `COLUMNS` expression in `UNPACK`, the columns expand into a parent expression, much like the [iterable unpacking behavior in Python](https://peps.python.org/pep-3132/).

Without `UNPACK`, operations on the `COLUMNS` expression are applied to each column separately:

<SqlLogicTest id="sql/expressions/star/index/example_023" />

With `UNPACK`, the `COLUMNS` expression is expanded into its parent expression, `coalesce` in the example above, which results in a single column:

<SqlLogicTest id="sql/expressions/star/index/example_024" />

The `UNPACK` keyword may be replaced by `*`, [matching Python syntax](https://peps.python.org/pep-3132/), when it is applied directly to the `COLUMNS` expression without any intermediate operations.

<SqlLogicTest id="sql/expressions/star/index/example_025" />

<DocCallout type="attention">
In the following example, where the `COLUMNS` expression has an intermediate operation (`+ 1`), replacing `UNPACK` by `*` results in an error:

<SqlLogicTest id="sql/expressions/star/index/example_026" />
</DocCallout>
