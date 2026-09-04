---
title: SQL Extensions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

On top of standard PostgreSQL syntax, SereneDB offers several SQL extensions and syntactic sugar that make queries more concise and readable.

<DocCallout type="tip">

Several of these features were first introduced by SereneDB, while some are inspired by other systems.
Many of the features originally introduced by SereneDB (e.g., [`GROUP BY ALL`](../sql/query_syntax/groupby/index.md#group-by-all)) have been since adapted by other systems.

</DocCallout>

## Clauses

-   Creating tables and inserting data:
    -   [`CREATE OR REPLACE TABLE`](../sql/statements/create_table/index.md#create-or-replace): avoid `DROP TABLE IF EXISTS` statements in scripts.
    -   [`CREATE TABLE ... AS SELECT` (CTAS)](../sql/statements/create_table/index.md#create-table--as-select-ctas): create a new table from the output of a table without manually defining a schema.
    -   [`INSERT INTO ... BY NAME`](../sql/statements/insert/index.md#insert-into--by-name): this variant of the `INSERT` statement allows using column names instead of positions.
    -   [`INSERT OR IGNORE INTO ...`](../sql/statements/insert/index.md#insert-or-ignore-into): insert the rows that do not result in a conflict due to `UNIQUE` or `PRIMARY KEY` constraints.
    -   [`INSERT OR REPLACE INTO ...`](../sql/statements/insert/index.md#insert-or-replace-into): insert the rows that do not result in a conflict due to `UNIQUE` or `PRIMARY KEY` constraints. For those that result in a conflict, replace the columns of the existing row to the new values of the to-be-inserted row.
-   Describing tables and computing statistics:
    -   [`DESCRIBE`](../cookbook/meta/describe.md): provides a succinct summary of the schema of a table or query.
    -   [`SUMMARIZE`](../cookbook/meta/summarize.md): returns summary statistics for a table or query.
-   Making SQL clauses more compact and readable:
    -   [`FROM`-first syntax with an optional `SELECT` clause](../sql/query_syntax/from_and_join/index.md#from-first-syntax): SereneDB allows queries in the form of `FROM tbl` which selects all columns (performing a `SELECT *` statement).
    -   [`GROUP BY ALL`](../sql/query_syntax/groupby/index.md#group-by-all): omit the group-by columns by inferring them from the list of attributes in the `SELECT` clause.
    -   [`ORDER BY ALL`](../sql/query_syntax/orderby/index.md#order-by-all): shorthand to order on all columns (e.g., to ensure deterministic results).
    -   [`SELECT * EXCLUDE`](../sql/expressions/star/index.md#exclude-clause): the `EXCLUDE` option allows excluding specific columns from the `*` expression.
    -   [`SELECT * REPLACE`](../sql/expressions/star/index.md#replace-clause): the `REPLACE` option allows replacing specific columns with different expressions in a `*` expression.
    -   [`UNION BY NAME`](../sql/query_syntax/setops/index.md#union-all-by-name): perform the `UNION` operation along the names of columns (instead of relying on positions).
    -   [Prefix aliases in the `SELECT` and `FROM` clauses](../sql/query_syntax/select/index.md): write `x: 42` instead of `42 AS x` for improved readability.
    -   [Specifying a percentage of the table size for the `LIMIT` clause](../sql/query_syntax/limit/index.md): write `LIMIT 10%` to return 10% of the query results.
-   Transforming tables:
    -   [`PIVOT`](../sql/statements/pivot/index.md) to turn long tables to wide tables.
    -   [`UNPIVOT`](../sql/statements/unpivot/index.md) to turn wide tables to long tables.
-   Defining SQL-level variables:
    -   [`SET VARIABLE`](../sql/statements/set_variable/index.md#set-variable)
    -   [`RESET VARIABLE`](../sql/statements/set_variable/index.md#reset-variable)

## Query Features

-   Column aliases in `WHERE`, `GROUP BY`, and `HAVING`. (Note that column aliases cannot be used in the `ON` clause of [`JOIN` clauses](../sql/query_syntax/from_and_join/index.md#joins).)
-   [`COLUMNS()` expression](../sql/expressions/star/index.md#columns-expression) can be used to execute the same expression on multiple columns:
    -   with regular expressions
    -   with `EXCLUDE` and `REPLACE`
    -   with lambda functions
-   Reusable column aliases (also known as “lateral column aliases”), e.g.: `SELECT i + 1 AS j, j + 2 AS k FROM range(0, 3) t(i)`
-   Advanced aggregation features for analytical (OLAP) queries:
    -   [`FILTER` clause](../sql/query_syntax/filter/index.md)
    -   [`GROUPING SETS`, `GROUP BY CUBE`, `GROUP BY ROLLUP` clauses](../sql/query_syntax/grouping_sets/index.md)
-   [`count()` shorthand](../sql/functions/aggregates/index.md) for `count(*)`
-   [`IN` operator for lists and maps](../sql/expressions/in/index.md)
-   [Specifying column names for common table expressions (`WITH`)](../sql/query_syntax/with/index.md#basic-cte-examples)
-   [Specifying column names in the `JOIN` clause](../sql/query_syntax/from_and_join/index.md#shorthands-in-the-join-clause)
-   [Using `VALUES` in the `JOIN` clause](../sql/query_syntax/from_and_join/index.md#shorthands-in-the-join-clause)
-   [Using `VALUES` in the anchor part of common table expressions](../sql/query_syntax/with/index.md#using-values)
-   [`SWITCH` statements as syntactic sugar for the `CASE` expression](../sql/expressions/case/index.md#switch-expression)

## Literals and Identifiers

-   [Case-insensitivity while maintaining case of entities in the catalog](./keywords_and_identifiers.md#case-sensitivity-of-identifiers)
-   [Underscores as digit separators in numeric literals](../sql/data_types/literal_types.md#underscores-in-numeric-literals)

## Data Types

-   [`MAP` data type](../sql/data_types/map.md)
-   [`UNION` data type](../sql/data_types/union.md)

## Data Import

-   [Auto-detecting the headers and schema of CSV files](../data_import_and_export/csv/auto_detection.md)
-   Directly querying [CSV files](../data_import_and_export/csv/overview.md) and [Parquet files](../data_import_and_export/parquet/overview.md)
-   [Filename expansion (globbing)](../sql/functions/pattern_matching/index.md#globbing), e.g.: `FROM 'my-data/part-*.parquet'`

## Functions and Expressions

-   [Dot operator for function chaining](../sql/functions/index.md#function-chaining-via-the-dot-operator): `SELECT ('hello').upper()`
-   String formatters:
    the [`format()` function with the `fmt` syntax](../sql/functions/text.md#format-syntax) and
    the [`printf() function`](../sql/functions/text.md#printf-syntax)
-   List comprehensions
-   [List slicing](../sql/data_types/list.md) and indexing from the back (`[-1]`)
-   [String slicing](../sql/functions/text.md)
-   [`STRUCT.*` notation](../sql/data_types/struct.md)
-   [Creating `LIST` using square brackets](../sql/data_types/list.md#creating-lists)
-   [Simple `LIST` and `STRUCT` creation](../sql/data_types/list.md#creating-lists)
-   [Updating the schema of `STRUCT`s](../sql/data_types/struct.md#updating-the-schema)

## Join Types

-   [`ASOF` joins](../sql/query_syntax/from_and_join/index.md#as-of-joins)
-   [`LATERAL` joins](../sql/query_syntax/from_and_join/index.md#lateral-joins)
-   [`POSITIONAL` joins](../sql/query_syntax/from_and_join/index.md#positional-joins)

## Trailing Commas

SereneDB allows [trailing commas](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Trailing_commas),
both when listing entities (e.g., column and table names) and when constructing [`LIST` items](../sql/data_types/list.md#creating-lists).
For example, the following query works:

<SqlLogicTest id="sql/dialect/sql_extensions/example_001" />

## "Top-N in Group" Queries

Computing the "top-N rows in a group" ordered by some criteria is a common task in SQL that unfortunately often requires a complex query involving window functions and/or subqueries.

To aid in this, SereneDB provides the aggregate functions [`max(arg, n)`](../sql/functions/aggregates/index.md#maxarg-n), [`min(arg, n)`](../sql/functions/aggregates/index.md#minarg-n), [`arg_max(arg, val, n)`](../sql/functions/aggregates/index.md#arg_maxarg-val-n), [`arg_min(arg, val, n)`](../sql/functions/aggregates/index.md#arg_minarg-val-n), [`max_by(arg, val, n)`](../sql/functions/aggregates/index.md#arg_maxarg-val-n) and [`min_by(arg, val, n)`](../sql/functions/aggregates/index.md#arg_minarg-val-n) to efficiently return the "top" `n` rows in a group based on a specific column in either ascending or descending order.

For example, let's use the following table:

<SqlLogicTest id="sql/dialect/sql_extensions/example_002" />

We want to get a list of the top-3 `val` values in each group `grp`. The conventional way to do this is to use a window function in a subquery:

<SqlLogicTest id="sql/dialect/sql_extensions/example_003" />

But in SereneDB, we can do this much more concisely (and efficiently!):

<SqlLogicTest id="sql/dialect/sql_extensions/example_004" />
