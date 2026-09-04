---
title: Subqueries
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Subqueries are parenthesized query expressions that appear as part of a larger, outer query. Subqueries are usually based on `SELECT ... FROM`, but in SereneDB other query constructs such as [`PIVOT`](../../../sql/statements/pivot/index.md) can also appear as a subquery.

## Scalar Subquery

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

Scalar subqueries are subqueries that return a single value. They can be used anywhere where an expression can be used. If a scalar subquery returns more than a single value, an error is raised (unless `scalar_subquery_error_on_multiple_rows` is set to `false`, in which case a row is selected randomly).

Consider the following table:

### Grades

| grade | course |
| ----: | :----- |
|     7 | Math   |
|     9 | Math   |
|     8 | CS     |

<SqlLogicTest id="sql/expressions/subqueries/index/example_001" />

We can run the following query to obtain the minimum grade:

<SqlLogicTest id="sql/expressions/subqueries/index/example_002" />

By using a scalar subquery in the `WHERE` clause, we can figure out for which course this grade was obtained:

<SqlLogicTest id="sql/expressions/subqueries/index/example_003" />

## `ARRAY` Subqueries

Subqueries that return multiple values can be wrapped with `ARRAY` to collect all results in a list.

<SqlLogicTest id="sql/expressions/subqueries/index/example_004" />

## Subquery Comparisons: `ALL`, `ANY` and `SOME`

In the section on [scalar subqueries](#scalar-subquery), a scalar expression was compared directly to a subquery using the equality [comparison operator](../../../sql/expressions/comparison_operators/index.md#comparison-operators) (`=`).
Such direct comparisons only make sense with scalar subqueries.

Scalar expressions can still be compared to single-column subqueries returning multiple rows by specifying a quantifier. Available quantifiers are `ALL`, `ANY` and `SOME`. The quantifiers `ANY` and `SOME` are equivalent.

### `ALL`

The `ALL` quantifier specifies that the comparison as a whole evaluates to `true` when the individual comparison results of _the expression at the left hand side of the comparison operator_ with each of the values from _the subquery at the right hand side of the comparison operator_ **all** evaluate to `true`:

<SqlLogicTest id="sql/expressions/subqueries/index/example_005" />

because 6 is less than or equal to each of the subquery results 7, 8 and 9.

However, the following query

<SqlLogicTest id="sql/expressions/subqueries/index/example_006" />

because 8 is not greater than or equal to the subquery result 9. And thus, because not all comparisons evaluate to `true`, `>= ALL` as a whole evaluates to `false`.

### `ANY`

The `ANY` quantifier specifies that the comparison as a whole evaluates to `true` when at least one of the individual comparison results evaluates to `true`.
For example:

<SqlLogicTest id="sql/expressions/subqueries/index/example_007" />

because no result of the subquery is less than or equal to 5.

The quantifier `SOME` may be used instead of `ANY`: `ANY` and `SOME` are interchangeable.

## `EXISTS`

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

The `EXISTS` operator tests for the existence of any row inside the subquery. It returns either true when the subquery returns one or more records, and false otherwise. The `EXISTS` operator is generally the most useful as a _correlated_ subquery to express semijoin operations. However, it can be used as an uncorrelated subquery as well.

For example, we can use it to figure out if there are any grades present for a given course:

<SqlLogicTest id="sql/expressions/subqueries/index/example_008" />

<SqlLogicTest id="sql/expressions/subqueries/index/example_009" />

<DocCallout type="tip">
The subqueries in the examples above make use of the fact that you can omit the `SELECT *` in SereneDB thanks to the [`FROM`-first syntax](../../../sql/query_syntax/from_and_join/index.md). The `SELECT` clause is required in subqueries by other SQL systems but cannot fulfill any purpose in `EXISTS` and `NOT EXISTS` subqueries.
</DocCallout>

### `NOT EXISTS`

The `NOT EXISTS` operator tests for the absence of any row inside the subquery. It returns either true when the subquery returns an empty result, and false otherwise. The `NOT EXISTS` operator is generally the most useful as a _correlated_ subquery to express antijoin operations. For example, to find rows in `Person` with no matching row in `interest`:

<SqlLogicTest id="sql/expressions/subqueries/index/example_010" />

<DocCallout type="tip">
SereneDB automatically detects when a `NOT EXISTS` query expresses an antijoin operation. There is no need to manually rewrite such queries to use `LEFT OUTER JOIN ... WHERE ... IS NULL`.
</DocCallout>

## `IN` Operator

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />

The `IN` operator checks containment of the left expression inside the result defined by the subquery or the set of expressions on the right hand side (RHS). The `IN` operator returns true if the expression is present in the RHS, false if the expression is not in the RHS and the RHS has no `NULL` values, or `NULL` if the expression is not in the RHS and the RHS has `NULL` values.

We can use the `IN` operator in a similar manner as we used the `EXISTS` operator:

<SqlLogicTest id="sql/expressions/subqueries/index/example_011" />

## Correlated Subqueries

All the subqueries presented here so far have been **uncorrelated** subqueries, where the subqueries themselves are entirely self-contained and can be run without the parent query. There exists a second type of subqueries called **correlated** subqueries. For correlated subqueries, the subquery uses values from the parent subquery.

Conceptually, the subqueries are run once for every single row in the parent query. Perhaps a simple way of envisioning this is that the correlated subquery is a **function** that is applied to every row in the source dataset.

For example, suppose that we want to find the minimum grade for every course. We could do that as follows:

<SqlLogicTest id="sql/expressions/subqueries/index/example_012" />

The subquery uses a column from the parent query (`grades_parent.course`). Conceptually, we can see the subquery as a function where the correlated column is a parameter to that function. We can evaluate that function for every row by projecting the correlated subquery alongside the grade:

<SqlLogicTest id="sql/expressions/subqueries/index/example_013" />

For `Math` the function returns `7` and for `CS` it returns `8`. Comparing each result against the grade in that row, the row `(Math, 9)` is filtered out by the query above, as `9 <> 7`.

## Returning Each Row of the Subquery as a Struct

Using the name of a subquery in the `SELECT` clause (without referring to a specific column) turns each row of the subquery into a struct whose fields correspond to the columns of the subquery. For example:

<SqlLogicTest id="sql/expressions/subqueries/index/example_014" />
