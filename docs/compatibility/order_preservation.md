---
title: Order Preservation
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

For many operations, SereneDB preserves the order of rows, similarly to data frame libraries such as Pandas.

## Example

Take the following table for example:

<SqlLogicTest id="sql/dialect/order_preservation/basic_table/example_001" />

Let's take the following query that returns the rows where `x` is an odd number:

<SqlLogicTest id="sql/dialect/order_preservation/odd_rows/example_002" />

Because the row `(1, 'a')` occurs before `(3, 'c')` in the original table, it is guaranteed to come before that row in this table too.

## Clauses

The following clauses guarantee that the original row order is preserved:

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

By default, the following components preserve insertion order:

-   [CSV reader](../data_import_and_export/csv/overview.md#order-preservation) (`read_csv` function)
-   [JSON reader](../data_import_and_export/json/overview.md) (`read_json` function)
-   [Parquet reader](../data_import_and_export/parquet/overview.md) (`read_parquet` function)

Preservation of insertion order is controlled by the `preserve_insertion_order` [configuration option](../configuration/overview.md).
This setting is `true` by default, indicating that the order should be preserved.
To change this setting, use:

<SqlLogicTest id="sql/dialect/order_preservation/example_003" />
