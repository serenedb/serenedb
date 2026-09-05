---
title: Struct
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Conceptually, a `STRUCT` column contains an ordered list of columns called “entries”. The entries are referenced by name using strings. This document refers to those entry names as keys. Each row in the `STRUCT` column must have the same keys. The names of the struct entries are part of the _schema_. Each row in a `STRUCT` column must have the same layout. The names of the struct entries are case-insensitive.

`STRUCT`s are typically used to nest multiple columns into a single column, and the nested column can be of any type, including other `STRUCT`s and `LIST`s.

`STRUCT`s are similar to PostgreSQL's `ROW` type. The key difference is that SereneDB `STRUCT`s require the same keys in each row of a `STRUCT` column. This allows SereneDB to provide significantly improved performance by fully utilizing its vectorized execution engine, and also enforces type consistency for improved correctness. SereneDB includes a `row` function as a special way to produce a `STRUCT`, but does not have a `ROW` data type. See an example below and the [`STRUCT` functions documentation](../../sql/functions/struct.md) for details.

See the [data types overview](../../sql/data_types/overview.md) for a comparison between nested data types.

## Creating Structs

Structs can be created using the [`struct_pack(name := expr, ...)`](../../sql/functions/struct.md) function, the equivalent array notation `{'name': expr, ...}`, using a row variable, or using the `row` function.

Create a struct using the `struct_pack` function. Note the lack of single quotes around the keys and the use of the `:=` operator:

<SqlLogicTest id="sql/data_types/struct/example_001" />

Create a struct using the array notation:

<SqlLogicTest id="sql/data_types/struct/example_002" />

Create a struct using a row variable:

<SqlLogicTest id="sql/data_types/struct/example_003" />

Create a struct of integers:

<SqlLogicTest id="sql/data_types/struct/example_004" />

Create a struct of strings with a `NULL` value:

<SqlLogicTest id="sql/data_types/struct/example_005" />

Create a struct with a different type for each key:

<SqlLogicTest id="sql/data_types/struct/example_006" />

Create a struct of structs with `NULL` values:

<SqlLogicTest id="sql/data_types/struct/example_007" />

## Adding or Updating Fields of Structs

To add new fields or update existing ones, you can use `struct_update`:

<SqlLogicTest id="sql/data_types/struct/example_008" />

Alternatively, `struct_insert` also allows adding new fields but not updating existing ones.

## Retrieving from Structs

Retrieving a value from a struct can be accomplished using dot notation, bracket notation, or through [struct functions](../../sql/functions/struct.md) like `struct_extract`.

Use dot notation to retrieve the value at a key's location. In the following query, the subquery generates a struct column `a`, which we then query with `a.x`.

<SqlLogicTest id="sql/data_types/struct/example_009" />

If a key contains a space, simply wrap it in double quotes (`"`).

<SqlLogicTest id="sql/data_types/struct/example_010" />

Bracket notation may also be used. Note that this uses single quotes (`'`) since the goal is to specify a certain string key and only constant expressions may be used inside the brackets (no expressions):

<SqlLogicTest id="sql/data_types/struct/example_011" />

The `struct_extract` function is also equivalent. This returns 1:

<SqlLogicTest id="sql/data_types/struct/example_012" />

### `unnest` / `STRUCT.*`

Rather than retrieving a single key from a struct, the `unnest` special function can be used to retrieve all keys from a struct as separate columns.
This is particularly useful when a prior operation creates a struct of unknown shape, or if a query must handle any potential struct keys:

<SqlLogicTest id="sql/data_types/struct/example_013" />

The same can be achieved with the star notation (`*`), which additionally allows [modifications of the returned columns](../../sql/expressions/star/index.md):

<SqlLogicTest id="sql/data_types/struct/example_014" />

<DocCallout type="attention">
The star notation is currently limited to top-level struct columns and non-aggregate expressions.
</DocCallout>

## Dot Notation Order of Operations

Referring to structs with dot notation can be ambiguous with referring to schemas and tables. In general, SereneDB looks for columns first, then for struct keys within columns. SereneDB resolves references in these orders, using the first match to occur:

### No Dots

<SqlLogicTest id="sql/data_types/struct/example_015" />

1. `part1` is a column

### One Dot

<SqlLogicTest id="sql/data_types/struct/example_016" />

1. `part1` is a table, `part2` is a column
2. `part1` is a column, `part2` is a property of that column

### Two (or More) Dots

<SqlLogicTest id="sql/data_types/struct/example_017" />

1. `part1` is a schema, `part2` is a table, `part3` is a column
2. `part1` is a table, `part2` is a column, `part3` is a property of that column
3. `part1` is a column, `part2` is a property of that column, `part3` is a property of that column

Any extra parts (e.g., `.part4.part5`, etc.) are always treated as properties

## Creating Structs with the `row` Function

The `row` function can be used to automatically convert multiple columns to a single struct column.
When using `row` the keys will be empty strings allowing for easy insertion into a table with a struct column.
Columns, however, cannot be initialized with the `row` function, and must be explicitly named.
For example, inserting values into a struct column using the `row` function:

<SqlLogicTest id="sql/data_types/struct/example_018" />

The table will contain a single entry:

<SqlLogicTest id="sql/data_types/struct/example_019" />

The following produces the same result as above:

<SqlLogicTest id="sql/data_types/struct/example_020" />

Initializing a struct column with the `row` function will fail:

<SqlLogicTest id="sql/data_types/struct/example_021" />

When casting between structs, the names of at least one field have to match. Therefore, the following query will fail:

<SqlLogicTest id="sql/data_types/struct/example_022" />

A workaround for this is to use [`struct_pack`](#creating-structs) instead:

<SqlLogicTest id="sql/data_types/struct/example_023" />

The `row` function can be used to return unnamed structs. For example:

<SqlLogicTest id="sql/data_types/struct/example_024" />

This produces `(1, 2, a)`.

If using multiple expressions when creating a struct, the `row` function is optional. The following query returns the same result as the previous one:

<SqlLogicTest id="sql/data_types/struct/example_025" />

## Comparison and Ordering

The `STRUCT` type can be compared using all the [comparison operators](../../sql/expressions/comparison_operators/index.md).
These comparisons can be used in [logical expressions](../../sql/expressions/logical_operators/index.md)
such as `WHERE` and `HAVING` clauses and return [`BOOLEAN` values](../../sql/data_types/boolean.md).

Comparisons are done in lexicographical order, with individual entries being compared as usual except that `NULL` values are treated as larger than all other values.

Specifically:

-   If all values of `s1` and `s2` compare equal, then `s1` and `s2` compare equal.
-   else, if `s1.value[i] < s2.value[i] OR s2.value[i] is NULL` for the first index `i` where `s1.value[i] != s2.value[i]`, then `s1` is less than `s2`, and vice versa.

Structs of different types are implicitly cast to a struct type with the union of the involved keys, following the rules for [combination casting](../../sql/data_types/typecasting.md#structs).

The following queries return `true`:

<SqlLogicTest id="sql/data_types/struct/example_026" />

<SqlLogicTest id="sql/data_types/struct/example_027" />

<SqlLogicTest id="sql/data_types/struct/example_028" />

<SqlLogicTest id="sql/data_types/struct/example_029" />

<SqlLogicTest id="sql/data_types/struct/example_030" />

<SqlLogicTest id="sql/data_types/struct/example_031" />

<SqlLogicTest id="sql/data_types/struct/example_033" />

<SqlLogicTest id="sql/data_types/struct/example_035" />

The following queries return `false`:

<SqlLogicTest id="sql/data_types/struct/example_032" />

<SqlLogicTest id="sql/data_types/struct/example_034" />

<SqlLogicTest id="sql/data_types/struct/example_036" />

## Updating the Schema

With SereneDB it's possible to update the sub-schema of structs
using the [`ALTER TABLE` clause](../../sql/statements/alter_table/index.md).
Adding, dropping or renaming a struct field rewrites the stored values of the column; added fields are backfilled with `NULL`. Nested structs are addressed with a dotted path (`s.sub.field`).

To follow the examples, initialize the `test` table as follows:

<SqlLogicTest id="sql/data_types/struct/example_037" />

### Adding a Field

Add field `k INTEGER` to struct `s` in table `test`:

<SqlLogicTest id="sql/data_types/struct/example_038" />

### Dropping a Field

Drop field `i` from struct `s` in table `test`:

<SqlLogicTest id="sql/data_types/struct/example_039" />

### Renaming a Field

Rename field `j` of struct `s` to `v1` in table `test`:

<SqlLogicTest id="sql/data_types/struct/example_040" />

## Functions

See [Struct Functions](../../sql/functions/struct.md).
