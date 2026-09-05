---
title: PIVOT
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `PIVOT` statement allows distinct values within a column to be separated into their own columns.
The values within those new columns are calculated using an aggregate function on the subset of rows that match each distinct value.

SereneDB implements both the SQL Standard `PIVOT` syntax and a simplified `PIVOT` syntax that automatically detects the columns to create while pivoting.
`PIVOT_WIDER` may also be used in place of the `PIVOT` keyword.

<DocCallout type="tip">
The [`UNPIVOT` statement](../../statements/unpivot/index.md) is the inverse of the `PIVOT` statement.
</DocCallout>

## Simplified `PIVOT` Syntax

The full syntax diagram is below, but the simplified `PIVOT` syntax can be summarized using spreadsheet pivot table naming conventions as:

<SqlLogicTest id="sql/statements/pivot/index/example_001" hideResult />

The `ON`, `USING`, and `GROUP BY` clauses are each optional, but they may not all be omitted.

### Example Data

All examples use the dataset produced by the queries below:

<SqlLogicTest id="sql/statements/pivot/index/example_002" />

<SqlLogicTest id="sql/statements/pivot/index/example_003" />

### `PIVOT ON` and `USING`

Use the `PIVOT` statement below to create a separate column for each year and calculate the total population in each.
The `ON` clause specifies which column(s) to split into separate columns.
It is equivalent to the columns parameter in a spreadsheet pivot table.

The `USING` clause determines how to aggregate the values that are split into separate columns.
This is equivalent to the values parameter in a spreadsheet pivot table.
If the `USING` clause is not included, it defaults to `count(*)`.

<SqlLogicTest id="sql/statements/pivot/index/example_004" />

In the above example, the `sum` aggregate is always operating on a single value.
If we only want to change the orientation of how the data is displayed without aggregating, use the `first` aggregate function.
In this example, we are pivoting numeric values, but the `first` function works very well for pivoting out a text column.
(This is something that is difficult to do in a spreadsheet pivot table, but easy in SereneDB!)

This query produces a result that is identical to the one above:

<SqlLogicTest id="sql/statements/pivot/index/example_005" />

<DocCallout type="tip">
The SQL syntax permits [`FILTER` clauses](../../query_syntax/filter/index.md) with aggregate functions in the `USING` clause.
In SereneDB, the `PIVOT` statement currently does not support these and they are silently ignored.
</DocCallout>

### Expressions in the `USING` clause

The `USING` clause accepts expressions over aggregate functions, not only a bare aggregate.
For example, to report the population in people rather than thousands, multiply the aggregate by 1000:

<SqlLogicTest id="sql/statements/pivot/index/example_018" />

### `PIVOT ON`, `USING`, and `GROUP BY`

By default, the `PIVOT` statement retains all columns not specified in the `ON` or `USING` clauses.
To include only certain columns and further aggregate, specify columns in the `GROUP BY` clause.
This is equivalent to the rows parameter of a spreadsheet pivot table.

In the below example, the `name` column is no longer included in the output, and the data is aggregated up to the `country` level.

<SqlLogicTest id="sql/statements/pivot/index/example_006" />

### `IN` Filter for `ON` Clause

To only create a separate column for specific values within a column in the `ON` clause, use an optional `IN` expression.
Let's say for example that we wanted to forget about the year 2020 for no particular reason...

<SqlLogicTest id="sql/statements/pivot/index/example_007" />

### Multiple Expressions per Clause

Multiple columns can be specified in the `ON` and `GROUP BY` clauses, and multiple aggregate expressions can be included in the `USING` clause.

#### Multiple `ON` Columns and `ON` Expressions

Multiple columns can be pivoted out into their own columns.
SereneDB will find the distinct values in each `ON` clause column and create one new column for all combinations of those values (a Cartesian product).

In the below example, all combinations of unique countries and unique cities receive their own column.
Some combinations may not be present in the underlying data, so those columns are populated with `NULL` values.

<SqlLogicTest id="sql/statements/pivot/index/example_008" />

To pivot only the combinations of values that are present in the underlying data, use an expression in the `ON` clause.
Multiple expressions and/or columns may be provided.

Here, `country` and `name` are concatenated together and the resulting concatenations each receive their own column.
Any arbitrary non-aggregating expression may be used.
In this case, concatenating with an underscore is used to imitate the naming convention the `PIVOT` clause uses when multiple `ON` columns are provided (like in the prior example).

<SqlLogicTest id="sql/statements/pivot/index/example_009" />

#### Multiple `USING` Expressions

An alias may also be included for each expression in the `USING` clause.
It will be appended to the generated column names after an underscore (`_`).
This makes the column naming convention much cleaner when multiple expressions are included in the `USING` clause.

In this example, both the `sum` and `max` of the population column are calculated for each year and are split into separate columns.

<SqlLogicTest id="sql/statements/pivot/index/example_010" />

#### Multiple `GROUP BY` Columns

Multiple `GROUP BY` columns may also be provided.
Note that column names must be used rather than column positions (1, 2, etc.), and that expressions are not supported in the `GROUP BY` clause.

<SqlLogicTest id="sql/statements/pivot/index/example_011" />

### Using `PIVOT` within a `SELECT` Statement

The `PIVOT` statement may be included within a `SELECT` statement as a CTE ([a Common Table Expression, or `WITH` clause](../../query_syntax/with/index.md)), or a subquery.
This allows for a `PIVOT` to be used alongside other SQL logic, as well as for multiple `PIVOT`s to be used in one query.

No `SELECT` is needed within the CTE, the `PIVOT` keyword can be thought of as taking its place.

<SqlLogicTest id="sql/statements/pivot/index/example_012" />

A `PIVOT` may be used in a subquery and must be wrapped in parentheses.
Note that this behavior is different than the SQL Standard Pivot, as illustrated in subsequent examples.

<SqlLogicTest id="sql/statements/pivot/index/example_013" />

### Multiple `PIVOT` Statements

Each `PIVOT` can be treated as if it were a `SELECT` node, so they can be joined together or manipulated in other ways.

For example, if two `PIVOT` statements share the same `GROUP BY` expression, they can be joined together using the columns in the `GROUP BY` clause into a wider pivot.

<SqlLogicTest id="sql/statements/pivot/index/example_014" />

## Simplified `PIVOT` Full Syntax Diagram

Below is the full syntax diagram of the `PIVOT` statement.

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## SQL Standard `PIVOT` Syntax

The full syntax diagram is below, but the SQL Standard `PIVOT` syntax can be summarized as:

<SqlLogicTest id="sql/statements/pivot/index/example_015" hideResult />

Unlike the simplified syntax, the `IN` clause must be specified for each column to be pivoted.
If you are interested in dynamic pivoting, the simplified syntax is recommended.

Note that no commas separate the expressions in the `FOR` clause, but that `value` and `GROUP BY` expressions must be comma-separated!

## Examples

This example uses a single value expression, a single column expression, and a single row expression:

<SqlLogicTest id="sql/statements/pivot/index/example_016" />

This example is somewhat contrived, but serves as an example of using multiple value expressions and multiple columns in the `FOR` clause.

<SqlLogicTest id="sql/statements/pivot/index/example_017" />

### SQL Standard `PIVOT` Full Syntax Diagram

Below is the full syntax diagram of the SQL Standard version of the `PIVOT` statement.

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />
