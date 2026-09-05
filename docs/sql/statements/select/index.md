---
title: SELECT
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `SELECT` statement retrieves rows from the database.

## Examples

Select all columns from the table `tbl`:

<SqlLogicTest id="sql/statements/select/index/example_001" />

Select the rows from `tbl`:

<SqlLogicTest id="sql/statements/select/index/example_002" />

Perform an aggregate grouped by the column `i`:

<SqlLogicTest id="sql/statements/select/index/example_003" />

Select only the top 3 rows from the `tbl`:

<SqlLogicTest id="sql/statements/select/index/example_004" />

Join two tables together using the `USING` clause:

<SqlLogicTest id="sql/statements/select/index/example_005" />

Use column indexes to select the first and third column from the table `tbl`:

<SqlLogicTest id="sql/statements/select/index/example_006" />

Select all unique cities from the addresses table:

<SqlLogicTest id="sql/statements/select/index/example_007" />

Return a `STRUCT` by using a row variable:

<SqlLogicTest id="sql/statements/select/index/example_008" />

## Syntax

The `SELECT` statement retrieves rows from the database. The canonical order of a `SELECT` statement is as follows, with less common clauses being indented:

<SqlLogicTest id="sql/statements/select/index/example_009" hideResult />

Optionally, the `SELECT` statement can be prefixed with a [`WITH` clause](../../query_syntax/with/index.md).

As the `SELECT` statement is so complex, we have split up the syntax diagrams into several parts. The full syntax diagram can be found at the bottom of the page.

### `SELECT` Clause

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />

The [`SELECT` clause](../../query_syntax/select/index.md) specifies the list of columns that will be returned by the query. While it appears first in the clause, _logically_ the expressions here are executed only at the end. The `SELECT` clause can contain arbitrary expressions that transform the output, as well as aggregates and window functions. The `DISTINCT` keyword ensures that only unique tuples are returned.

<DocCallout type="tip">
Column names are case-insensitive. See the [Rules for Case Sensitivity](../../../compatibility/keywords_and_identifiers.md#rules-for-case-sensitivity) for more details.
</DocCallout>

### `FROM` Clause

<RailroadDiagram source={RailroadSource} production="rrdiagram4" />

The [`FROM` clause](../../query_syntax/from_and_join/index.md) specifies the _source_ of the data on which the remainder of the query should operate. Logically, the `FROM` clause is where the query starts execution. The `FROM` clause can contain a single table, a combination of multiple tables that are joined together, or another `SELECT` query inside a subquery node.

### `SAMPLE` Clause

<RailroadDiagram source={RailroadSource} production="rrdiagram10" />

The [`SAMPLE` clause](../../query_syntax/sample/index.md) allows you to run the query on a sample from the base table. This can significantly speed up processing of queries, at the expense of accuracy in the result. Samples can also be used to quickly see a snapshot of the data when exploring a dataset. The `SAMPLE` clause is applied right after anything in the `FROM` clause (i.e., after any joins, but before the where clause or any aggregates). See the [Samples](../../samples/index.md) page for more information.

### `WHERE` Clause

<RailroadDiagram source={RailroadSource} production="rrdiagram5" />

The [`WHERE` clause](../../query_syntax/where/index.md) specifies any filters to apply to the data. This allows you to select only a subset of the data in which you are interested. Logically the `WHERE` clause is applied immediately after the `FROM` clause.

### `GROUP BY` and `HAVING` Clauses

<RailroadDiagram source={RailroadSource} production="rrdiagram6" />

The [`GROUP BY` clause](../../query_syntax/groupby/index.md) specifies which grouping columns should be used to perform any aggregations in the `SELECT` clause. If the `GROUP BY` clause is specified, the query is always an aggregate query, even if no aggregations are present in the `SELECT` clause.

### `WINDOW` Clause

<RailroadDiagram source={RailroadSource} production="rrdiagram7" />

The [`WINDOW` clause](../../query_syntax/window/index.md) allows you to specify named windows that can be used within window functions. These are useful when you have multiple window functions, as they allow you to avoid repeating the same window clause.

### `QUALIFY` Clause

<RailroadDiagram source={RailroadSource} production="rrdiagram11" />

The [`QUALIFY` clause](../../query_syntax/qualify/index.md) is used to filter the result of [`WINDOW` functions](../../functions/window_functions/index.md).

### `ORDER BY`, `LIMIT` and `OFFSET` Clauses

<RailroadDiagram source={RailroadSource} production="rrdiagram8" />

[`ORDER BY`](../../query_syntax/orderby/index.md), [`LIMIT` and `OFFSET`](../../query_syntax/limit/index.md) are output modifiers.
Logically they are applied at the very end of the query.
The `ORDER BY` clause sorts the rows on the sorting criteria in either ascending or descending order.
The `LIMIT` clause restricts the amount of rows fetched, while the `OFFSET` clause indicates at which position to start reading the values.

### `VALUES` List

<RailroadDiagram source={RailroadSource} production="rrdiagram9" />

[A `VALUES` list](../../query_syntax/values/index.md) is a set of values that is supplied instead of a `SELECT` statement.

### Row IDs

For each table, the [`rowid` pseudocolumn](https://docs.oracle.com/cd/B19306_01/server.102/b14200/pseudocolumns008.htm) returns the row identifiers based on the physical storage.

<SqlLogicTest id="sql/statements/select/index/example_010" />

In the current storage, these identifiers are contiguous unsigned integers (0, 1, ...) if no rows were deleted.
Deletions introduce gaps in the rowids which may be reclaimed later:

<SqlLogicTest id="sql/statements/select/index/example_011" />

The `rowid` values are stable within a transaction.

<DocCallout type="bestPractice">
It is strongly advised to avoid using rowids as identifiers.
</DocCallout>

> If there is a user-defined column named `rowid`, it shadows the `rowid` pseudocolumn.

### Common Table Expressions

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

## Full Syntax Diagram

Below is the full syntax diagram of the `SELECT` statement:

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
