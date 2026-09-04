---
title: FROM / JOIN
sidebar_position: 2
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `FROM` clause specifies the _source_ of the data on which the remainder of the query should operate. Logically, the `FROM` clause is where the query starts execution. The `FROM` clause can contain a single table, a combination of multiple tables that are joined together using `JOIN` clauses, or another `SELECT` query inside a subquery node. SereneDB also has an optional `FROM`-first syntax which enables you to also query without a `SELECT` statement.

## Examples

Select all columns from the table called `tbl`:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_001" />

Select all columns from the table using the `FROM`-first syntax:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_002" />

Select all columns using the `FROM`-first syntax and omitting the `SELECT` clause:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_003" />

Select all columns from the table called `tbl` through an alias `tn`:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_004" />

Use a prefix alias:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_005" />

Select all columns from the table `tbl` in the schema `schema_name`:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_006" />

Select the column `i` from the table function `range`, where the first column of the range function is renamed to `i`:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_007" />

Select all columns from the CSV file called `test.csv`:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_008" />

Select all columns from a subquery:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_009" />

Select the entire row of the table as a struct:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_010" />

Select the entire row of the subquery as a struct (i.e., a single column):

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_011" />

Join two tables together:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_012" />

Select a 10% sample from a table:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_013" />

Select a sample of 10 rows from a table:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_014" />

Use the `FROM`-first syntax with `WHERE` clause and aggregation:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_015" />

### Table Functions

Some functions in SereneDB return entire tables rather than individual values. These functions are accordingly called _table functions_ and can be used with a `FROM` clause like regular table references.
Examples include [`read_csv`](../../../data_import_and_export/csv/overview.md#csv-functions), [`read_parquet`](../../../data_import_and_export/parquet/overview.md#read_parquet-function), [`range`](../../functions/list.md#rangestart-stop-step), [`generate_series`](../../functions/list.md#generate_seriesstart-stop-step), [`repeat`](../../functions/utility.md#repeat_rowvarargs-num_rows), [`unnest`](../../query_syntax/unnest.md), and [`glob`](../../functions/utility.md#globsearch_path) (note that some of the examples here can be used as both scalar and table functions).

For example,

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_016" />

is implicitly translated to a call of the `read_csv` table function:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_017" />

All table functions support a `WITH ORDINALITY` suffix, which extends the returned table by an integer column `ordinality` that enumerates the generated rows starting at `1`.

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_018" />

Note that the same result could be achieved using the [`row_number` window function](../../functions/window_functions/index.md#row_numberorder-by-ordering).
In the presence of [joins](#joins), however, `WITH ORDINALITY` allows enumerating one side of the join instead of the final result set, without having to resort to sub-queries.

## Joins

Joins are a fundamental relational operation used to connect two tables or relations horizontally.
The relations are referred to as the _left_ and _right_ sides of the join
based on how they are written in the join clause.
Each result row has the columns from both relations.

A join uses a rule to match pairs of rows from each relation.
Often this is a predicate, but there are other implied rules that may be specified.

### Outer Joins

Rows that do not have any matches can still be returned if an `OUTER` join is specified.
Outer joins can be one of:

-   `LEFT` (All rows from the left relation appear at least once)
-   `RIGHT` (All rows from the right relation appear at least once)
-   `FULL` (All rows from both relations appear at least once)

A join that is not `OUTER` is `INNER` (only rows that get paired are returned).

When an unpaired row is returned, the attributes from the other table are set to `NULL`.

### Cross Product Joins (Cartesian Product)

The simplest type of join is a `CROSS JOIN`.
There are no conditions for this type of join,
and it just returns all the possible pairs.

Return all pairs of rows:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_019" />

This is equivalent to omitting the `JOIN` clause:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_020" />

### Conditional Joins

Most joins are specified by a predicate that connects
attributes from one side to attributes from the other side.
The conditions can be explicitly specified using an `ON` clause
with the join (clearer) or implied by the `WHERE` clause (old-fashioned).

We use the `l_regions` and the `l_nations` tables from the TPC-H schema:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_021" />

Return the regions for the nations:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_022" />

If the column names are the same and are required to be equal,
then the simpler `USING` syntax can be used:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_023" />

Return the regions for the nations:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_024" />

The expressions do not have to be equalities – any predicate can be used:

Return the pairs of jobs where one ran longer but cost less:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_025" />

### Natural Joins

Natural joins join two tables based on attributes that share the same name.

For example, take the following example with cities, airport codes and airport names. Note that both tables are intentionally incomplete, i.e., they do not have a matching pair in the other table.

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_026" />

To join the tables on their shared [`IATA`](https://en.wikipedia.org/wiki/IATA_airport_code) attributes, run:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_027" />

Note that only rows where the same `iata` attribute was present in both tables were included in the result.

We can also express this query using the vanilla `JOIN` clause with the `USING` keyword:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_028" />

### Semi and Anti Joins

Semi joins return rows from the left table that have at least one match in the right table.
Anti joins return rows from the left table that have _no_ matches in the right table.
When using a semi or anti join the result will never have more rows than the left hand side table.
Semi joins provide the same logic as the [`IN` operator](../../expressions/in/index.md) statement.
Anti joins provide the same logic as the `NOT IN` operator, except anti joins ignore `NULL` values from the right table.

#### Semi Join Example

Return a list of city–airport code pairs from the `city_airport` table where the airport name **is available** in the `airport_names` table:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_029" />

This query is equivalent to:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_030" />

#### Anti Join Example

Return a list of city–airport code pairs from the `city_airport` table where the airport name **is not available** in the `airport_names` table:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_031" />

This query is equivalent to:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_032" />

### Lateral Joins

The `LATERAL` keyword allows subqueries in the `FROM` clause to refer to previous subqueries. This feature is also known as a _lateral join_.

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_033" />

<div class="center_aligned_header_table"></div>

Lateral joins are a generalization of correlated subqueries, as they can return multiple values per input value rather than only a single value.

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_034" />

<div class="center_aligned_header_table"></div>

It may be helpful to think about `LATERAL` as a loop where we iterate through the rows of the first subquery and use it as input to the second (`LATERAL`) subquery.
In the examples above, we iterate through table `t` and refer to its column `i` from the definition of table `t2`. The rows of `t2` form column `j` in the result.

It is possible to refer to multiple attributes from the `LATERAL` subquery. Using the table from the first example:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_035" />

<div class="center_aligned_header_table"></div>

<DocCallout type="tip">
SereneDB detects when `LATERAL` joins should be used, making the use of the `LATERAL` keyword optional.
</DocCallout>

### Positional Joins

When working with data frames or other embedded tables of the same size,
the rows may have a natural correspondence based on their physical order.
In scripting languages, this is easily expressed using a loop:

```cpp
for (i = 0; i < n; i++) {
    f(t1.a[i], t2.b[i]);
}
```

It is difficult to express this in standard SQL because
relational tables are not ordered, but imported tables such as data frames
or disk files (like [CSVs](../../../data_import_and_export/csv/overview.md) or [Parquet files](../../../data_import_and_export/parquet/overview.md)) do have a natural ordering.

Connecting them using this ordering is called a _positional join:_

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_036" />

<div class="center_aligned_header_table"></div>

Positional joins are always `FULL OUTER` joins, i.e., the resulting table has the length of the longer input table and the missing entries are filled with `NULL` values.

### As-Of Joins

A common operation when working with temporal or similarly-ordered data
is to find the nearest (first) event in a reference table (such as prices).
This is called an _as-of join:_

Attach prices to stock trades:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_037" />

The `ASOF` join requires at least one inequality condition on the ordering field.
The inequality can be any inequality condition (`>=`, `>`, `<=`, `<`)
on any data type, but the most common form is `>=` on a temporal type.
Any other conditions must be equalities (or `NOT DISTINCT`).
This means that the left/right order of the tables is significant.

`ASOF` joins each left side row with at most one right side row.
It can be specified as an `OUTER` join to find unpaired rows
(e.g., trades without prices or prices which have no trades.)

Attach prices or NULLs to stock trades:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_038" />

`ASOF` joins can also specify join conditions on matching column names with the `USING` syntax,
but the _last_ attribute in the list must be the inequality,
which will be greater than or equal to (`>=`):

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_039" />

Returns symbol, trades.when, price (but NOT prices.when):

If you combine `USING` with a `SELECT *` like this,
the query will return the left side (probe) column values for the matches,
not the right side (build) column values.
To get the `prices` times in the example, you will need to list the columns explicitly:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_040" />

### Self-Joins

SereneDB allows self-joins for all types of joins.
Note that tables need to be aliased, using the same table name without aliases will result in an error:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_041" />

Adding the aliases allows the query to parse successfully:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_042" />

### Shorthands in the `JOIN` Clause

You can specify column names in the `JOIN` clause:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_043" />

You can also use the `VALUES` clause in the `JOIN` clause:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_044" />

## `FROM`-First Syntax

SereneDB's SQL supports the `FROM`-first syntax, i.e., it allows putting the `FROM` clause before the `SELECT` clause or completely omitting the `SELECT` clause. We use the following example to demonstrate it:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_045" />

### `FROM`-First Syntax with a `SELECT` Clause

The following statement demonstrates the use of the `FROM`-first syntax:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_046" />

This is equivalent to:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_047" />

<div class="center_aligned_header_table"></div>

### `FROM`-First Syntax without a `SELECT` Clause

The following statement demonstrates the use of the optional `SELECT` clause:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_048" />

This is equivalent to:

<SqlLogicTest id="sql/query_syntax/from_and_join/index/example_049" />

<div class="center_aligned_header_table"></div>

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
