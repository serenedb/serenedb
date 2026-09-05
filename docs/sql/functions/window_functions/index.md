---
title: Window Functions
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

<!-- markdownlint-disable MD001 -->

SereneDB supports [window functions](https://en.wikipedia.org/wiki/Window_function_%28SQL%29), which can use multiple rows to calculate a value for each row.
Window functions are [blocking operators](../../../cookbook/performance/how_to_tune_workloads.md#blocking-operators), i.e., they require their entire input to be buffered, making them one of the most memory-intensive operators in SQL.

Window functions are available in SQL since [SQL:2003](https://en.wikipedia.org/wiki/SQL:2003) and are supported by major SQL database systems.

## Examples

Generate a `row_number` column to enumerate rows:

<SqlLogicTest id="sql/functions/window_functions/index/example_001" />

<DocCallout type="tip">

If you only need a number for each row in a table, you can use the [`rowid` pseudocolumn](../../../sql/statements/select/index.md#row-ids).

</DocCallout>

Generate a `row_number` column to enumerate rows, ordered by `time`:

<SqlLogicTest id="sql/functions/window_functions/index/example_002" />

Generate a `row_number` column to enumerate rows, ordered by `time` and partitioned by `region`:

<SqlLogicTest id="sql/functions/window_functions/index/example_003" />

Compute the difference between the current and the previous-by-`time` `amount`:

<SqlLogicTest id="sql/functions/window_functions/index/example_004" />

Compute the percentage of the total `amount` of sales per `region` for each row:

<SqlLogicTest id="sql/functions/window_functions/index/example_005" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

Window functions can only be used in the `SELECT` clause. To share `OVER` specifications between functions, use the statement's [`WINDOW` clause](../../../sql/query_syntax/window/index.md) and use the `OVER ⟨window_name⟩`{:.language-sql .highlight} syntax.

## General-Purpose Window Functions

The table below shows the available general window functions.

| Name                                                                                                                            | Description                                                                                                                                                                                                                                                                                                                                                                                                          |
| :------------------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`cume_dist([ORDER BY ordering])`](#cume_distorder-by-ordering)                                                                 | The cumulative distribution: (number of partition rows preceding or peer with current row) / total partition rows.                                                                                                                                                                                                                                                                                                   |
| [`dense_rank()`](#dense_rank)                                                                                                   | The rank of the current row _without gaps;_ this function counts peer groups.                                                                                                                                                                                                                                                                                                                                        |
| [`fill(expr [ ORDER BY ordering])`](#fillexpr-order-by-ordering)                                                                | Fill in missing values using linear interpolation with `ORDER BY` as the X-axis.                                                                                                                                                                                                                                                                                                                                     |
| [`first_value(expr[ ORDER BY ordering][ IGNORE NULLS])`](#first_valueexpr-order-by-ordering-ignore-nulls)                       | Returns `expr` evaluated at the row that is the first row (with a non-null value of `expr` if `IGNORE NULLS` is set) of the window frame.                                                                                                                                                                                                                                                                            |
| [`lag(expr[, offset[, default]][ ORDER BY ordering][ IGNORE NULLS])`](#lagexpr-offset-default-order-by-ordering-ignore-nulls)   | Returns `expr` evaluated at the row that is `offset` rows (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) before the current row within the window frame; if there is no such row, instead return `default` (which must be of the same type as `expr`). Both `offset` and `default` are evaluated with respect to the current row. If omitted, `offset` defaults to `1` and default to `NULL`. |
| [`last_value(expr[ ORDER BY ordering][ IGNORE NULLS])`](#last_valueexpr-order-by-ordering-ignore-nulls)                         | Returns `expr` evaluated at the row that is the last row (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) of the window frame.                                                                                                                                                                                                                                                                  |
| [`lead(expr[, offset[, default]][ ORDER BY ordering][ IGNORE NULLS])`](#leadexpr-offset-default-order-by-ordering-ignore-nulls) | Returns `expr` evaluated at the row that is `offset` rows after the current row (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) within the window frame; if there is no such row, instead return `default` (which must be of the same type as `expr`). Both `offset` and `default` are evaluated with respect to the current row. If omitted, `offset` defaults to `1` and default to `NULL`.  |
| [`nth_value(expr, nth[ ORDER BY ordering][ IGNORE NULLS])`](#nth_valueexpr-nth-order-by-ordering-ignore-nulls)                  | Returns `expr` evaluated at the nth row (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) of the window frame (counting from 1); `NULL` if no such row.                                                                                                                                                                                                                                          |
| [`ntile(num_buckets[ ORDER BY ordering])`](#ntilenum_buckets-order-by-ordering)                                                 | An integer ranging from 1 to `num_buckets`, dividing the partition as equally as possible.                                                                                                                                                                                                                                                                                                                           |
| [`percent_rank([ORDER BY ordering])`](#percent_rankorder-by-ordering)                                                           | The relative rank of the current row: `(rank() - 1) / (total partition rows - 1)`.                                                                                                                                                                                                                                                                                                                                   |
| [`rank([ORDER BY ordering])`](#rankorder-by-ordering)                                                                           | The rank of the current row _with gaps;_ same as `row_number` of its first peer.                                                                                                                                                                                                                                                                                                                                     |
| [`row_number([ORDER BY ordering])`](#row_numberorder-by-ordering)                                                               | The number of the current row within the partition, counting from 1.                                                                                                                                                                                                                                                                                                                                                 |

#### `cume_dist([ORDER BY ordering])`

<div class="nostroke_table"></div>

| **Description** | The cumulative distribution: (number of partition rows preceding or peer with current row) / total partition rows. If an `ORDER BY` clause is specified, the distribution is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | `DOUBLE` |
| **Example** | `cume_dist()` |

#### `dense_rank()`

<div class="nostroke_table"></div>

| **Description** | The rank of the current row _without gaps;_ this function counts peer groups. |
| :--- | :--- |
| **Return type** | `BIGINT` |
| **Example** | `dense_rank()` |
| **Aliases** | `rank_dense()` |

#### `fill(expr[ ORDER BY ordering])`

<div class="nostroke_table"></div>

| **Description** | Replaces `NULL` values of `expr` with a linear interpolation based on the closest non-`NULL` values and the sort values. Both values must support arithmetic and there must be only one ordering key. For missing values at the ends, linear extrapolation is used. Failure to interpolate results in the `NULL` value being retained. |
| :--- | :--- |
| **Return type** | Same type as `expr` |
| **Example** | `fill(column)` |

#### `first_value(expr[ ORDER BY ordering][ IGNORE NULLS])`

<div class="nostroke_table"></div>

| **Description** | Returns `expr` evaluated at the row that is the first row (with a non-null value of `expr` if `IGNORE NULLS` is set) of the window frame. If an `ORDER BY` clause is specified, the first row number is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | Same type as `expr` |
| **Example** | `first_value(column)` |

#### `lag(expr[, offset[, default]][ ORDER BY ordering][ IGNORE NULLS])`

<div class="nostroke_table"></div>

| **Description** | Returns `expr` evaluated at the row that is `offset` rows (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) before the current row within the window frame; if there is no such row, instead return `default` (which must be of the same type as `expr`). Both `offset` and `default` are evaluated with respect to the current row. If omitted, `offset` defaults to `1` and default to `NULL`. If an `ORDER BY` clause is specified, the lagged row number is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | Same type as `expr` |
| **Example** | `lag(column, 3, 0)` |

#### `last_value(expr[ ORDER BY ordering][ IGNORE NULLS])`

<div class="nostroke_table"></div>

| **Description** | Returns `expr` evaluated at the row that is the last row (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) of the window frame. If omitted, `offset` defaults to `1` and default to `NULL`. If an `ORDER BY` clause is specified, the last row is determined within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | Same type as `expr` |
| **Example** | `last_value(column)` |

#### `lead(expr[, offset[, default]][ ORDER BY ordering][ IGNORE NULLS])`

<div class="nostroke_table"></div>

| **Description** | Returns `expr` evaluated at the row that is `offset` rows after the current row (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) within the window frame; if there is no such row, instead return `default` (which must be of the same type as `expr`). Both `offset` and `default` are evaluated with respect to the current row. If omitted, `offset` defaults to `1` and default to `NULL`. If an `ORDER BY` clause is specified, the leading row number is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | Same type as `expr` |
| **Example** | `lead(column, 3, 0)` |

#### `nth_value(expr, nth[ ORDER BY ordering][ IGNORE NULLS])`

<div class="nostroke_table"></div>

| **Description** | Returns `expr` evaluated at the nth row (among rows with a non-null value of `expr` if `IGNORE NULLS` is set) of the window frame (counting from 1); `NULL` if no such row. If an `ORDER BY` clause is specified, the nth row number is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | Same type as `expr` |
| **Example** | `nth_value(column, 2)` |

#### `ntile(num_buckets[ ORDER BY ordering])`

<div class="nostroke_table"></div>

| **Description** | An integer ranging from 1 to `num_buckets`, dividing the partition as equally as possible. If an `ORDER BY` clause is specified, the ntile is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | `BIGINT` |
| **Example** | `ntile(4)` |

#### `percent_rank([ORDER BY ordering])`

<div class="nostroke_table"></div>

| **Description** | The relative rank of the current row: `(rank() - 1) / (total partition rows - 1)`. If an `ORDER BY` clause is specified, the relative rank is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | `DOUBLE` |
| **Example** | `percent_rank()` |

#### `rank([ORDER BY ordering])`

<div class="nostroke_table"></div>

| **Description** | The rank of the current row _with gaps_; same as `row_number` of its first peer. If an `ORDER BY` clause is specified, the rank is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | `BIGINT` |
| **Example** | `rank()` |

#### `row_number([ORDER BY ordering])`

<div class="nostroke_table"></div>

| **Description** | The number of the current row within the partition, counting from 1. If an `ORDER BY` clause is specified, the row number is computed within the frame using the provided ordering instead of the frame ordering. |
| :--- | :--- |
| **Return type** | `BIGINT` |
| **Example** | `row_number()` |

## Aggregate Window Functions

All [aggregate functions](../../../sql/functions/aggregates/index.md) can be used in a windowing context, including the optional [`FILTER` clause](../../../sql/query_syntax/filter/index.md).
The `first` and `last` aggregate functions are shadowed by the respective general-purpose window functions, with the minor consequence that the `FILTER` clause is not available for these but `IGNORE NULLS` is.

## DISTINCT Arguments

All aggregate window functions support using a `DISTINCT` clause for the arguments. When the `DISTINCT` clause is
provided, only distinct values are considered in the computation of the aggregate. This is typically used in combination
with the `COUNT` aggregate to get the number of distinct elements; but it can be used together with any aggregate
function in the system. There are some aggregates that are insensitive to duplicate values (e.g., `min`, `max`) and for
them this clause is parsed and ignored.

<SqlLogicTest id="sql/functions/window_functions/index/example_006" />

## ORDER BY Arguments

All aggregate window functions support using an `ORDER BY` argument clause that is _different_ from the window ordering.
When the `ORDER BY` argument clause is provided, the values being aggregated are sorted before applying the function.
Usually this is not important, but there are some order-sensitive aggregates that can have indeterminate results (e.g.,
`mode`, `list` and `string_agg`). These can be made deterministic by ordering the arguments. For order-insensitive
aggregates, this clause is parsed and ignored.

<SqlLogicTest id="sql/functions/window_functions/index/example_007" />

The SQL standard does not provide for using `ORDER BY` with general-purpose window functions, but we have extended all
of these functions (except `dense_rank`) to accept this syntax and use framing to restrict the range that the secondary
ordering applies to.

<SqlLogicTest id="sql/functions/window_functions/index/example_008" />

Note that there is no comma separating the arguments from the `ORDER BY` clause.

## Nulls

All [general-purpose window functions](#general-purpose-window-functions) that accept `IGNORE NULLS` respect nulls by default. This default behavior can optionally be made explicit via `RESPECT NULLS`.

In contrast, all [aggregate window functions](#aggregate-window-functions) (except for `list` and its aliases, which can be made to ignore nulls via a `FILTER`) ignore nulls and do not accept `RESPECT NULLS`. For example, `sum(column) OVER (ORDER BY time) AS cumulativeColumn` computes a cumulative sum where rows with a `NULL` value of `column` have the same value of `cumulativeColumn` as the row that precedes them.

## Evaluation

Windowing works by breaking a relation up into independent _partitions_,
_ordering_ those partitions,
and then computing a new column for each row as a function of the nearby values.
Some window functions depend only on the partition boundary and the ordering,
but a few (including all the aggregates) also use a _frame_.
Frames are specified as a number of rows on either side (_preceding_ or _following_) of the _current row_.
The distance can be specified as a number of _rows_,
as a _range_ of values using the partition's ordering value and a distance,
or as a number of _groups_ (sets of rows with the same sort value).

The full syntax is shown in the diagram at the top of the page,
and this diagram visually illustrates computation environment:

<img src="/images/framing-light.png" alt="The Window Computation Environment" title="Figure 1: The Window Computation Environment" style="max-width:90%;width:90%;height:auto" class="lightmode-img" />
<img src="/images/framing-dark.png" alt="The Window Computation Environment" title="Figure 1: The Window Computation Environment" style="max-width:90%;width:90%;height:auto" class="darkmode-img" />

### Partition and Ordering

Partitioning breaks the relation up into independent, unrelated pieces.
Partitioning is optional, and if none is specified then the entire relation is treated as a single partition.
Window functions cannot access values outside of the partition containing the row they are being evaluated at.

Ordering is also optional, but without it the results of [general-purpose window functions](#general-purpose-window-functions) and [order-sensitive aggregate functions](../../../sql/functions/aggregates/index.md#order-by-clause-in-aggregate-functions), and the order of [framing](#framing) are not well-defined.
Each partition is ordered using the same ordering clause.

Here is a table of power generation data. To load it from a CSV file, run:

<SqlLogicTest id="sql/functions/window_functions/index/example_009" />

After partitioning by plant and ordering by date, it will have this layout:

<SqlLogicTest id="sql/functions/window_functions/index/example_018" />

In what follows,
we shall use this table (or small sections of it) to illustrate various pieces of window function evaluation.

The simplest window function is `row_number()`.
This function just computes the 1-based row number within the partition using the query:

<SqlLogicTest id="sql/functions/window_functions/index/example_010" />

The row number restarts at 1 for each `Plant` partition.

Note that even though the function is computed with an `ORDER BY` clause,
the result does not have to be sorted,
so the `SELECT` also needs to be explicitly sorted if that is desired.

### Framing

Framing specifies a set of rows relative to each row where the function is evaluated.
The distance from the current row is given as an expression either `PRECEDING` or `FOLLOWING` the current row in the order specified by the `ORDER BY` clause in the `OVER` specification.
This distance can either be specified as an integral number of `ROWS` or `GROUPS`,
or as a `RANGE` delta expression. It is invalid for a frame to start after it ends.
For a `RANGE` specification, there must be only one ordering expression and it must support subtraction unless only the sentinel boundary values `UNBOUNDED PRECEDING` / `UNBOUNDED FOLLOWING` / `CURRENT ROW` are used.
Using the [`EXCLUDE` clause](#exclude-clause), rows comparing equal to the current row in the specified ordering expression (so-called peers) can be excluded from the frame.

The default frame is unbounded (i.e., the entire partition) when no `ORDER BY` clause is present and `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` when an `ORDER BY` clause is present. By default, the `CURRENT ROW` boundary value (but not the `CURRENT ROW` in the `EXCLUDE` clause) means the current row and all its peers when `RANGE` or `GROUP` framing are used but it means only the current row when `ROWS` framing is used.

#### `ROWS` Framing

Here is a simple `ROW` frame query, using an aggregate function:

<SqlLogicTest id="sql/functions/window_functions/index/example_011" />

This query computes the `sum` of each point and the points on either side of it:

<img src="/images/blog/windowing/moving-sum.jpg" alt="Moving SUM of three values" title="Figure 2: A moving SUM of three values" style="max-width:90%;width:90%;height:auto"/>

Notice that at the edge of the partition, there are only two values added together.
This is because frames are cropped to the edge of the partition.

#### `RANGE` Framing

Returning to the power data, suppose the data is noisy.
We might want to compute a 7 day moving average for each plant to smooth out the noise.
To do this, we can use this window query:

<SqlLogicTest id="sql/functions/window_functions/index/example_012" />

This query partitions the data by `Plant` (to keep the different power plants' data separate),
orders each plant's partition by `Date` (to put the energy measurements next to each other),
and uses a `RANGE` frame of three days on either side of each day for the `avg`
(to handle any missing days).

#### `GROUPS` Framing

The third type of framing counts _groups_ of rows relative the current row.
A _group_ in this framing is a set of values with identical `ORDER BY` values.
If we assume that power is being generated on every day,
we can use `GROUPS` framing to compute the moving average of all power generated in the system
without having to resort to date arithmetic:

<SqlLogicTest id="sql/functions/window_functions/index/example_013" />

Notice how the values for each date are the same.

#### `EXCLUDE` Clause

`EXCLUDE` is an optional modifier to the frame clause for excluding rows around the `CURRENT ROW`.
This is useful when you want to compute some aggregate value of nearby rows
to see how the current row compares to it.

In the following example, we want to know how an athlete's time in an event compares to
the average of all the times recorded for their event within ±10 days:

<SqlLogicTest id="sql/functions/window_functions/index/example_014" />

There are four options for `EXCLUDE` that specify how to treat the current row:

-   `CURRENT ROW` – exclude just the current row
-   `GROUP` – exclude the current row and all its “peers” (rows that have the same `ORDER BY` value)
-   `TIES` – exclude all peer rows, but _not_ the current row (this makes a hole on either side)
-   `NO OTHERS` – don't exclude anything (the default)

Exclusion is implemented for both windowed aggregates as well as for the `first`, `last` and `nth_value` functions.

### `WINDOW` Clauses

Multiple different `OVER` clauses can be specified in the same `SELECT`, and each will be computed separately.
Often, however, we want to use the same layout for multiple window functions.
The `WINDOW` clause can be used to define a _named_ window that can be shared between multiple window functions:

<SqlLogicTest id="sql/functions/window_functions/index/example_015" />

The three window functions will also share the data layout, which will improve performance.

Multiple windows can be defined in the same `WINDOW` clause by comma-separating them:

<SqlLogicTest id="sql/functions/window_functions/index/example_016" />

The queries above do not use a number of clauses commonly found in select statements, like
`WHERE`, `GROUP BY`, etc. For more complex queries you can find where `WINDOW` clauses fall in
the canonical order of the [`SELECT statement`](../../../sql/statements/select/index.md).

### Filtering the Results of Window Functions Using `QUALIFY`

Window functions are executed after the [`WHERE`](../../../sql/query_syntax/where/index.md) and [`HAVING`](../../../sql/query_syntax/having/index.md) clauses have been already evaluated, so it's not possible to use these clauses to filter the results of window functions
The [`QUALIFY` clause](../../../sql/query_syntax/qualify/index.md) avoids the need for a subquery or [`WITH` clause](../../../sql/query_syntax/with/index.md) to perform this filtering.

### Box and Whisker Queries

All aggregates can be used as windowing functions, including the complex statistical functions.
These function implementations have been optimized for windowing,
and we can use the window syntax to write queries that generate the data for moving box-and-whisker plots:

<SqlLogicTest id="sql/functions/window_functions/index/example_017" />
