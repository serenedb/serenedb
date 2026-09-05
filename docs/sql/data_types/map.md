---
title: Map
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`MAP`s are similar to `STRUCT`s in that they are an ordered list of key-value pairs. However, `MAP`s do not need to have the same keys present for each row, and thus are suitable for use cases where the schema is unknown beforehand or varies per row.

`MAP`s must have a single type for all keys, and a single type for all values. Keys and values can be any type, and the type of the keys does not need to match the type of the values (e.g., a `MAP` of `VARCHAR` to `INT` is valid). `MAP`s may not have duplicate keys. `MAP`s return `NULL` if a key is not found rather than throwing an error as structs do.

In contrast, `STRUCT`s must have string keys, but each value may have a different type. See the [data types overview](../../sql/data_types/overview.md) for a comparison between nested data types.

To construct a `MAP`, use the bracket syntax preceded by the `MAP` keyword.

## Creating Maps

A map with `VARCHAR` keys and `INTEGER` values:

<SqlLogicTest id="sql/data_types/map/example_001" />

Alternatively use the `map_from_entries` function:

<SqlLogicTest id="sql/data_types/map/example_002" />

A map can be also created using two lists: keys and values:

<SqlLogicTest id="sql/data_types/map/example_003" />

A map can also use `INTEGER` keys and `NUMERIC` values:

<SqlLogicTest id="sql/data_types/map/example_004" />

Keys and/or values can also be nested types:

<SqlLogicTest id="sql/data_types/map/example_005" />

Create a table with a map column that has `INTEGER` keys and `DOUBLE` values:

<SqlLogicTest id="sql/data_types/map/example_006" />

## Retrieving from Maps

`MAP` values can be retrieved using the `map_extract_value` function or bracket notation:

<SqlLogicTest id="sql/data_types/map/example_007" />

If the key has the wrong type, an error is thrown:

<SqlLogicTest id="sql/data_types/map/example_008" />

If the key has the correct type but is merely not contained in the map, a `NULL` value is returned instead:

<SqlLogicTest id="sql/data_types/map/example_011" />

The `map_extract` function (and its synonym `element_at`) can be used to retrieve a value wrapped in a list; it returns an empty list if the key is not contained in the map:

<SqlLogicTest id="sql/data_types/map/example_009" />

<SqlLogicTest id="sql/data_types/map/example_010" />

## Comparison Operators

Nested types can be compared using all the [comparison operators](../../sql/expressions/comparison_operators/index.md).
These comparisons can be used in [logical expressions](../../sql/expressions/logical_operators/index.md)
for both `WHERE` and `HAVING` clauses, as well as for creating [Boolean values](../../sql/data_types/boolean.md).

The ordering is defined positionally in the same way that words can be ordered in a dictionary.
`NULL` values compare greater than all other values and are considered equal to each other.

At the top level, `NULL` nested values obey standard SQL `NULL` comparison rules:
comparing a `NULL` nested value to a non-`NULL` nested value produces a `NULL` result.
Comparing nested value _members_, however, uses the internal nested value rules for `NULL`s,
and a `NULL` nested value member will compare above a non-`NULL` nested value member.

## Functions

See [Map Functions](../../sql/functions/map.md).
