---
title: IN Operator
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `IN` operator checks containment of the left expression inside the _collection_ on the right hand side (RHS).
Supported collections on the RHS are tuples, lists, maps and subqueries that return a single column.

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## `IN (val1, val2, ...)` (Tuple)

The `IN` operator on a tuple `(val1, val2, ...)` returns `true` if the expression is present in the RHS, `false` if the expression is not in the RHS and the RHS has no `NULL` values, or `NULL` if the expression is not in the RHS and the RHS has `NULL` values.

<SqlLogicTest id="sql/expressions/in/index/example_001" />

<SqlLogicTest id="sql/expressions/in/index/example_002" />

<SqlLogicTest id="sql/expressions/in/index/example_003" />

<SqlLogicTest id="sql/expressions/in/index/example_004" />

## `IN [val1, val2, ...]` (List)

The `IN` operator works on lists according to the semantics used in Python.
Unlike for the [`IN tuple` operator](#in-val1-val2--tuple), the presence of `NULL` values on the right hand side of the expression does not make a difference in the result:

<SqlLogicTest id="sql/expressions/in/index/example_005" />

<SqlLogicTest id="sql/expressions/in/index/example_006" />

## `IN` Map

The `IN` operator works on [maps](../../../sql/data_types/map.md) according to the semantics used in Python, i.e., it checks for the presence of keys (not values):

<SqlLogicTest id="sql/expressions/in/index/example_007" />

<SqlLogicTest id="sql/expressions/in/index/example_008" />

## `IN` Subquery

The `IN` operator works with [subqueries](../../../sql/expressions/subqueries/index.md) that return a single column.
For example:

<SqlLogicTest id="sql/expressions/in/index/example_009" />

If the subquery returns more than one column, a Binder Error is thrown:

<SqlLogicTest id="sql/expressions/in/index/example_010" />

## `IN` String

The `IN` operator can be used as a shorthand for the [`contains` string function](../../../sql/functions/text.md#containsstring-search_string).
For example:

<SqlLogicTest id="sql/expressions/in/index/example_011" />

## `NOT IN`

`NOT IN` can be used to check if an element is not present in the set.
`x NOT IN y` is equivalent to `NOT (x IN y)`.
