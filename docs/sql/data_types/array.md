---
title: Array
sidebar_position: 2
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

An `ARRAY` column stores fixed-sized arrays. All fields in the column must have the same length and the same underlying type. Arrays are typically used to store arrays of numbers, but can contain any uniform data type, including `ARRAY`, [`LIST`](../data_types/list.md) and [`STRUCT`](../data_types/struct.md) types.

Arrays can be used to store vectors such as [word embeddings](https://en.wikipedia.org/wiki/Word_embedding) or image embeddings.

To store variable-length lists, use the [`LIST` type](../data_types/list.md). See the [data types overview](../data_types/overview.md) for a comparison between nested data types.

<DocCallout type="tip">
The `ARRAY` type in PostgreSQL allows variable-length fields. SereneDB's `ARRAY` type is fixed-length.
</DocCallout>

## Creating Arrays

Arrays can be created using the [`array_value(expr, ...)` function](../../sql/functions/array.md#array_valuearg-).

Construct with the `array_value` function:

<SqlLogicTest id="sql/data_types/array/example_001" />

You can always implicitly cast an array to a list (and use list functions, like `list_extract`, `[i]`):

<SqlLogicTest id="sql/data_types/array/example_002" />

You can cast from a list to an array (the dimensions have to match):

<SqlLogicTest id="sql/data_types/array/example_003" />

Arrays can be nested:

<SqlLogicTest id="sql/data_types/array/example_004" />

Arrays can store structs:

<SqlLogicTest id="sql/data_types/array/example_005" />

## Defining an Array Field

Arrays can be created using the `⟨TYPE_NAME⟩[⟨LENGTH⟩]`{:.language-sql .highlight} syntax. For example, to create an array field for 3 integers, run:

<SqlLogicTest id="sql/data_types/array/example_006" />

## Retrieving Values from Arrays

Retrieving one or more values from an array can be accomplished using brackets and slicing notation, or through [list functions](../../sql/functions/list.md) like `list_extract` and `array_extract`. Using the example in [Defining an Array Field](#defining-an-array-field).

The following queries for extracting the first element of an array are equivalent:

<SqlLogicTest id="sql/data_types/array/example_007" />

Using the slicing notation returns a `LIST`:

<SqlLogicTest id="sql/data_types/array/example_008" />

## Functions

All [`LIST` functions](../../sql/functions/list.md) work with the `ARRAY` type. Additionally, several `ARRAY`-native functions are also supported.
See the [`ARRAY` functions](../../sql/functions/array.md#array-native-functions).

## Examples

Create sample data:

<SqlLogicTest id="sql/data_types/array/example_009" />

Compute cross product:

<SqlLogicTest id="sql/data_types/array/example_010" />

Compute cosine similarity:

<SqlLogicTest id="sql/data_types/array/example_011" />

## Ordering

The ordering of `ARRAY` instances is defined using a lexicographical order. `NULL` values compare greater than all other values and are considered equal to each other.

## See Also

For more functions, see [List Functions](../../sql/functions/list.md).
