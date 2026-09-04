---
title: NULL Values
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

`NULL` values are special values that are used to represent missing data in SQL. Columns of any type can contain `NULL` values. Logically, a `NULL` value can be seen as “the value of this field is unknown”.

A `NULL` value can be inserted to any field that does not have the `NOT NULL` qualifier:

<SqlLogicTest id="sql/data_types/nulls/example_001" />

`NULL` values have special semantics in many parts of the query as well as in many functions:

<DocCallout type="tip">
Any comparison with a `NULL` value returns `NULL`, including `NULL = NULL`.
</DocCallout>

You can use `IS NOT DISTINCT FROM` to perform an equality comparison where `NULL` values compare equal to each other. Use `IS (NOT) NULL` to check if a value is `NULL`.

<SqlLogicTest id="sql/data_types/nulls/example_002" />

<SqlLogicTest id="sql/data_types/nulls/example_003" />

<SqlLogicTest id="sql/data_types/nulls/example_004" />

## NULL and Functions

A function that has an input argument as `NULL` **usually** returns `NULL`.

<SqlLogicTest id="sql/data_types/nulls/example_005" />

The `coalesce` function is an exception to this: it takes any number of arguments, and returns for each row the first argument that is not `NULL`. If all arguments are `NULL`, `coalesce` also returns `NULL`.

<SqlLogicTest id="sql/data_types/nulls/example_006" />

<SqlLogicTest id="sql/data_types/nulls/example_007" />

<SqlLogicTest id="sql/data_types/nulls/example_008" />

The `ifnull` function is a two-argument version of `coalesce`.

<SqlLogicTest id="sql/data_types/nulls/example_009" />

<SqlLogicTest id="sql/data_types/nulls/example_010" />

## `NULL` and `AND` / `OR`

`NULL` values have special behavior when used with `AND` and `OR`.
For details, see the [Boolean Type documentation](../../sql/data_types/boolean.md).

## `NULL` and `IN` / `NOT IN`

The behavior of `... IN ⟨something with a NULL⟩`{:.language-sql .highlight} is different from `... IN ⟨something with no NULLs⟩`{:.language-sql .highlight}.
For details, see the [`IN` documentation](../../sql/expressions/in/index.md).

## `NULL` and Aggregate Functions

`NULL` values are ignored in most aggregate functions.

Aggregate functions that do not ignore `NULL` values include: `first`, `last`, `list` and `array_agg`. To exclude `NULL` values from those aggregate functions, the [`FILTER` clause](../../sql/query_syntax/filter/index.md) can be used.

<SqlLogicTest id="sql/data_types/nulls/example_011" />

<SqlLogicTest id="sql/data_types/nulls/example_012" />

<SqlLogicTest id="sql/data_types/nulls/example_013" />
