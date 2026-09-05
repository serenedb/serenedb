---
title: FILTER
sidebar_position: 14
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `FILTER` clause may optionally follow an aggregate function in a `SELECT` statement. This will filter the rows of data that are fed into the aggregate function in the same way that a `WHERE` clause filters rows, but localized to the specific aggregate function.

There are multiple types of situations where this is useful, including when evaluating multiple aggregates with different filters, and when creating a pivoted view of a dataset. `FILTER` provides a cleaner syntax for pivoting data when compared with the more traditional `CASE WHEN` approach discussed below.

Some aggregate functions also do not filter out `NULL` values, so using a `FILTER` clause will return valid results when at times the `CASE WHEN` approach will not. This occurs with the functions `first` and `last`, which are desirable in a non-aggregating pivot operation where the goal is to simply re-orient the data into columns rather than re-aggregate it. `FILTER` also improves `NULL` handling when using the `list` and `array_agg` functions, as the `CASE WHEN` approach will include `NULL` values in the list result, while the `FILTER` clause will remove them.

## Examples

Return the following:

-   The total number of rows
-   The number of rows where `i <= 5`
-   The number of rows where `i` is odd

<SqlLogicTest id="sql/query_syntax/filter/index/example_001" />

<DocCallout type="tip">
Simply counting rows that satisfy a condition can also be achieved without the `FILTER` clause, using the boolean `sum` aggregate function, e.g., `sum(i <= 5)`.
</DocCallout>

Different aggregate functions may be used, and multiple `WHERE` expressions are also permitted:

<SqlLogicTest id="sql/query_syntax/filter/index/example_002" />

The `FILTER` clause can also be used to pivot data from rows into columns. This is a static pivot, as columns must be defined prior to runtime in SQL. However, this kind of statement can be dynamically generated in a host programming language to leverage SereneDB's SQL engine for rapid, larger than memory pivoting.

First generate an example dataset:

<SqlLogicTest id="sql/query_syntax/filter/index/example_003" />

“Pivot” the data out by year (move each year out to a separate column):

<SqlLogicTest id="sql/query_syntax/filter/index/example_004" />

This syntax produces the same results as the `FILTER` clauses above:

<SqlLogicTest id="sql/query_syntax/filter/index/example_005" />

However, the `CASE WHEN` approach will not work as expected when using an aggregate function that does not ignore `NULL` values. The `first` function falls into this category, so `FILTER` is preferred in this case.

“Pivot” the data out by year (move each year out to a separate column):

<SqlLogicTest id="sql/query_syntax/filter/index/example_006" />

This will produce `NULL` values whenever the first evaluation of the `CASE WHEN` clause returns a `NULL`:

<SqlLogicTest id="sql/query_syntax/filter/index/example_007" />

## Aggregate Function Syntax (Including `FILTER` Clause)

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
