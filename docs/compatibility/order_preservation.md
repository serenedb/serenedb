---
title: Order Preservation
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

With the `preserve_insertion_order` [configuration option](../configuration/overview.md) set to `true`, SereneDB preserves the order of rows for many operations, similarly to data frame libraries such as Pandas. The option is `false` by default, which lets parallel operations return rows in any order unless the query has an `ORDER BY`.

## Example

Take the following table for example:

<SqlLogicTest id="sql/dialect/order_preservation/basic_table/example_001" />

Let's take the following query that returns the rows where `x` is an odd number:

<SqlLogicTest id="sql/dialect/order_preservation/odd_rows/example_002" />

Here the row `(1, 'a')` comes before `(3, 'c')`, the order the table stores them in. A table does not guarantee that order, whatever the option says; see [Insertion Order](#insertion-order).

## Clauses

With `preserve_insertion_order` set to `true`, the following clauses keep the order of the rows they read:

-   `COPY` (see [Insertion Order](#insertion-order))
-   `FROM` with a single table
-   `LIMIT`
-   `OFFSET`
-   `SELECT`
-   `UNION ALL`
-   `WHERE`
-   Window functions with an empty `OVER` clause
-   Common table expressions and table subqueries as long as they only contain the aforementioned components

<DocCallout type="tip">

`row_number() OVER ()` allows turning the original row order into an explicit column that can be referenced in the operations that don't preserve row order by default.
On materialized tables, the `rowid` pseudo-column can be used to the same effect.

</DocCallout>

The following operations **do not** guarantee that the row order is preserved:

-   `FROM` with multiple tables and/or subqueries
-   `JOIN`
-   `UNION`
-   `USING SAMPLE`
-   Whole-table aggregation (the input order, that is, the order in which rows are fed into [order-sensitive aggregate functions](../sql/functions/aggregates/index.md#order-by-clause-in-aggregate-functions) is not guaranteed unless explicitly specified in the aggregate function)
-   `GROUP BY` (neither in- nor output order are guaranteed)
-   `ORDER BY` (specifically, `ORDER BY` may not use a [stable algorithm](https://en.wikipedia.org/wiki/Stable_algorithm))
-   Scalar subqueries

## Insertion Order

With `preserve_insertion_order` set to `true`, the following components preserve insertion order:

-   [CSV reader](../data_import_and_export/csv/overview.md#order-preservation) (`read_csv` function)
-   [JSON reader](../data_import_and_export/json/overview.md) (`read_json` function)
-   [Parquet reader](../data_import_and_export/parquet/overview.md) (`read_parquet` function)

Tables do not honor `preserve_insertion_order`, also when it is `true`: a table returns its rows in the order it stores them, which is not the order they were inserted in once a write such as `INSERT ... SELECT` or `COPY` runs on several threads. Use `ORDER BY` when the order of a table's rows matters.

Preservation of insertion order is controlled by the `preserve_insertion_order` [configuration option](../configuration/overview.md).
This setting is `false` by default.
To change this setting, use:

<SqlLogicTest id="sql/dialect/order_preservation/example_003" />
