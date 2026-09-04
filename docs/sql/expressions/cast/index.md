---
title: Casting
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

Casting refers to the operation of converting a value in a particular data type to the corresponding value in another data type.
Casting can occur either implicitly or explicitly. The syntax described here performs an explicit cast. More information on casting can be found on the [typecasting page](../../../sql/data_types/typecasting.md).

## Explicit Casting

The standard SQL syntax for explicit casting is `CAST(expr AS TYPENAME)`, where `TYPENAME` is a name (or alias) of one of [SereneDB's data types](../../../sql/data_types/overview.md). SereneDB also supports the shorthand `expr::TYPENAME`, which is also present in PostgreSQL.

<SqlLogicTest id="sql/expressions/cast/index/example_001" />

<SqlLogicTest id="sql/expressions/cast/index/example_002" />

### Casting Rules

Not all casts are possible. For example, it is not possible to convert an `INTEGER` to a `DATE`. Casts may also throw errors when the cast could not be successfully performed. For example, trying to cast the string `'hello'` to an `INTEGER` will result in an error being thrown.

<SqlLogicTest id="sql/expressions/cast/index/example_003" />

The exact behavior of the cast depends on the source and destination types. For example, when casting from `VARCHAR` to any other type, the string will be attempted to be converted.

### `TRY_CAST`

`TRY_CAST` can be used when the preferred behavior is not to throw an error, but instead to return a `NULL` value. `TRY_CAST` will never throw an error, and will instead return `NULL` if a cast is not possible.

<SqlLogicTest id="sql/expressions/cast/index/example_004" />

## `cast_to_type` Function

The `cast_to_type` function allows generating a cast from an expression to the type of another column.
For example:

<SqlLogicTest id="sql/expressions/cast/index/example_005" />

This function is primarily useful in [macros](../../statements/create_macro/index.md), as it allows you to maintain types.
This helps with making generic macros that operate on different types. For example, the following macro adds to a number if the input is an `INTEGER`:

<SqlLogicTest id="sql/expressions/cast/conditional_macro/example_006" />

Note that the `CASE` statement needs to return the same type in all code paths. We can perform the addition on any input column by adding a cast to the desired type – but we need to cast the result of the addition back to the source type to make the binding work.
