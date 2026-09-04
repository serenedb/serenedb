---
title: List
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

A `LIST` column encodes lists of values. Fields in the column can have values with different lengths, but they must all have the same underlying type. `LIST`s are typically used to store arrays of numbers, but can contain any uniform data type, including other `LIST`s and `STRUCT`s.

`LIST`s are similar to PostgreSQL's `ARRAY` type. SereneDB uses the `LIST` terminology, but some [`array_` functions](../../sql/functions/list.md) are provided for PostgreSQL compatibility.

See the [data types overview](../../sql/data_types/overview.md) for a comparison between nested data types.

<DocCallout type="tip">
For storing fixed-length lists, SereneDB uses the [`ARRAY` type](../../sql/data_types/array.md).
</DocCallout>

## Creating Lists

Lists can be created using the [`list_value(expr, ...)`](../../sql/functions/list.md#list_valuearg-) function or the equivalent bracket notation `[expr, ...]`. The expressions can be constants or arbitrary expressions. To create a list from a table column, use the [`list`](../../sql/functions/aggregates/index.md#general-aggregate-functions) aggregate function.

List of integers:

<SqlLogicTest id="sql/data_types/list/example_001" />

List of strings with a `NULL` value:

<SqlLogicTest id="sql/data_types/list/example_002" />

List of lists with `NULL` values:

<SqlLogicTest id="sql/data_types/list/example_003" />

Create a list with the list_value function:

<SqlLogicTest id="sql/data_types/list/example_004" />

Create a table with an `INTEGER` list column and a `VARCHAR` list column:

<SqlLogicTest id="sql/data_types/list/example_005" />

## Retrieving from Lists

Retrieving one or more values from a list can be accomplished using brackets and slicing notation, or through [list functions](../../sql/functions/list.md) like `list_extract`. Multiple equivalent functions are provided as aliases for compatibility with systems that refer to lists as arrays. For example, the function `array_slice`.

<SqlLogicTest id="sql/data_types/list/example_012" />

## Comparison and Ordering

The `LIST` type can be compared using all the [comparison operators](../../sql/expressions/comparison_operators/index.md).
These comparisons can be used in [logical expressions](../../sql/expressions/logical_operators/index.md)
such as `WHERE` and `HAVING` clauses, and return [`BOOLEAN` values](../../sql/data_types/boolean.md).

The `LIST` ordering is defined positionally using the following rules, where `min_len = min(len(l1), len(l2))`.

-   **Equality.** `l1` and `l2` are equal, if for each `i` in `[1, min_len]`: `l1[i] = l2[i]`.
-   **Less Than**. For the first index `i` in `[1, min_len]` where `l1[i] != l2[i]`:
    If `l1[i] < l2[i]`, `l1` is less than `l2`.

`NULL` values are compared following PostgreSQL's semantics.
Lower nesting levels are used for tie-breaking.

Here are some queries returning `true` for the comparison.

<SqlLogicTest id="sql/data_types/list/example_006" />

<SqlLogicTest id="sql/data_types/list/example_007" />

<SqlLogicTest id="sql/data_types/list/example_008" />

<SqlLogicTest id="sql/data_types/list/example_011" />

These queries return `false`.

<SqlLogicTest id="sql/data_types/list/example_009" />

<SqlLogicTest id="sql/data_types/list/example_010" />

## Functions

See [List Functions](../../sql/functions/list.md).
