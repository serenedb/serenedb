---
title: TRY
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `TRY` expression ensures that errors caused by the input rows in the child (scalar) expression result in `NULL` for those rows, instead of causing the query to throw an error.

<DocCallout type="tip">
The `TRY` expression was inspired by the [`TRY_CAST` expression](../../sql/expressions/cast/index.md#try_cast).
</DocCallout>

## Examples

The following calls return errors when invoked without the `TRY` expression.
When they are wrapped into a `TRY` expression, they return `NULL`:

### Casting

#### Without `TRY`

<SqlLogicTest id="sql/expressions/try/example_001" />

#### With `TRY`

<SqlLogicTest id="sql/expressions/try/example_002" />

### Integer Overflow

#### Without `TRY`

<SqlLogicTest id="sql/expressions/try/example_003" />

#### With `TRY`

<SqlLogicTest id="sql/expressions/try/example_004" />

### Casting Multiple Rows

#### Without `TRY`

<SqlLogicTest id="sql/expressions/try/example_005" />

#### With `TRY`

<SqlLogicTest id="sql/expressions/try/example_006" />

<div class="center_aligned_header_table"></div>

## Limitations

`TRY` cannot be used in combination with a volatile function, an aggregate function, or a [scalar subquery](../../sql/expressions/subqueries/index.md#scalar-subquery).
For example:

<SqlLogicTest id="sql/expressions/try/example_007" />
