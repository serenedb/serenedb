---
title: UNPIVOT
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `UNPIVOT` statement allows multiple columns to be stacked into fewer columns.
In the basic case, multiple columns are stacked into two columns: a `NAME` column (which contains the name of the source column) and a `VALUE` column (which contains the value from the source column).

SereneDB implements both the SQL Standard `UNPIVOT` syntax and a simplified `UNPIVOT` syntax.
Both can utilize a [`COLUMNS` expression](../../expressions/star/index.md#columns-expression) to automatically detect the columns to unpivot.
`PIVOT_LONGER` may also be used in place of the `UNPIVOT` keyword.

<DocCallout type="tip">
The [`PIVOT` statement](../../statements/pivot/index.md) is the inverse of the `UNPIVOT` statement.
</DocCallout>

## Simplified `UNPIVOT` Syntax

The full syntax diagram is below, but the simplified `UNPIVOT` syntax can be summarized using spreadsheet pivot table naming conventions as:

<SqlLogicTest id="sql/statements/unpivot/index/example_001" hideResult />

### Example Data

All examples use the dataset produced by the queries below:

<SqlLogicTest id="sql/statements/unpivot/index/example_002" />

<SqlLogicTest id="sql/statements/unpivot/index/example_003" />

### `UNPIVOT` Manually

The most typical `UNPIVOT` transformation is to take already pivoted data and re-stack it into a column each for the name and value.
In this case, all months will be stacked into a `month` column and a `sales` column.

<SqlLogicTest id="sql/statements/unpivot/index/example_004" />

### `UNPIVOT` Dynamically Using `COLUMNS` Expression

In many cases, the number of columns to unpivot is not easy to predetermine ahead of time.
In the case of this dataset, the query above would have to change each time a new month is added.
The [`COLUMNS` expression](../../expressions/star/index.md#columns-expression) can be used to select all columns that are not `empid` or `dept`.
This enables dynamic unpivoting that will work regardless of how many months are added.
The query below returns identical results to the one above.

<SqlLogicTest id="sql/statements/unpivot/index/example_005" />

### `UNPIVOT` into Multiple Value Columns

The `UNPIVOT` statement has additional flexibility: more than 2 destination columns are supported.
This can be useful when the goal is to reduce the extent to which a dataset is pivoted, but not completely stack all pivoted columns.
To demonstrate this, the query below will generate a dataset with a separate column for the number of each month within the quarter (month 1, 2, or 3), and a separate row for each quarter.
Since there are fewer quarters than months, this does make the dataset longer, but not as long as the above.

To accomplish this, multiple sets of columns are included in the `ON` clause.
The `q1` and `q2` aliases are optional.
The number of columns in each set of columns in the `ON` clause must match the number of columns in the `VALUE` clause.

<SqlLogicTest id="sql/statements/unpivot/index/example_006" />

### Using `UNPIVOT` within a `SELECT` Statement

The `UNPIVOT` statement may be included within a `SELECT` statement as a CTE ([a Common Table Expression, or WITH clause](../../query_syntax/with/index.md)), or a subquery.
This allows for an `UNPIVOT` to be used alongside other SQL logic, as well as for multiple `UNPIVOT`s to be used in one query.

No `SELECT` is needed within the CTE, the `UNPIVOT` keyword can be thought of as taking its place.

<SqlLogicTest id="sql/statements/unpivot/index/example_007" />

An `UNPIVOT` may be used in a subquery and must be wrapped in parentheses.
Note that this behavior is different than the SQL Standard Unpivot, as illustrated in subsequent examples.

<SqlLogicTest id="sql/statements/unpivot/index/example_008" />

### Expressions within `UNPIVOT` Statements

SereneDB allows expressions within the `UNPIVOT` statements, provided that they only involve a single column. These can be used to perform computations as well as [explicit casts](../../data_types/typecasting.md#explicit-casting). For example:

<SqlLogicTest id="sql/statements/unpivot/index/example_009" />

### Simplified `UNPIVOT` Full Syntax Diagram

Below is the full syntax diagram of the `UNPIVOT` statement.

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## SQL Standard `UNPIVOT` Syntax

The full syntax diagram is below, but the SQL Standard `UNPIVOT` syntax can be summarized as:

<SqlLogicTest id="sql/statements/unpivot/index/example_010" hideResult />

Note that only one column can be included in the `name-column-name` expression.

### SQL Standard `UNPIVOT` Manually

To complete the basic `UNPIVOT` operation using the SQL standard syntax, only a few additions are needed.

<SqlLogicTest id="sql/statements/unpivot/index/example_011" />

### SQL Standard `UNPIVOT` Dynamically Using the `COLUMNS` Expression

The [`COLUMNS` expression](../../expressions/star/index.md#columns-expression) can be used to determine the `IN` list of columns dynamically.
This will continue to work even if additional `month` columns are added to the dataset.
It produces the same result as the query above.

<SqlLogicTest id="sql/statements/unpivot/index/example_012" />

### SQL Standard `UNPIVOT` into Multiple Value Columns

The `UNPIVOT` statement has additional flexibility: more than 2 destination columns are supported.
This can be useful when the goal is to reduce the extent to which a dataset is pivoted, but not completely stack all pivoted columns.
To demonstrate this, the query below will generate a dataset with a separate column for the number of each month within the quarter (month 1, 2, or 3), and a separate row for each quarter.
Since there are fewer quarters than months, this does make the dataset longer, but not as long as the above.

To accomplish this, multiple columns are included in the `value-column-name` portion of the `UNPIVOT` statement.
Multiple sets of columns are included in the `IN` clause.
The `q1` and `q2` aliases are optional.
The number of columns in each set of columns in the `IN` clause must match the number of columns in the `value-column-name` portion.

<SqlLogicTest id="sql/statements/unpivot/index/example_013" />

### SQL Standard `UNPIVOT` Full Syntax Diagram

Below is the full syntax diagram of the SQL Standard version of the `UNPIVOT` statement.

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />
