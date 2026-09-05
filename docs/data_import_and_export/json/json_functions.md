---
title: JSON Functions
sidebar_position: 6
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## JSON Extraction Functions

SereneDB follows PostgreSQL semantics for extracting values from `JSON`. The `->` and `->>` operators — and their function equivalents `json_extract` and `json_extract_string` — access a member by its **name** or an array element by its **integer index** (they do not interpret `$.` JSONPath or `/` JSON Pointer syntax). To address a value by a JSON Pointer or JSONPath location instead, use `json_value`. The operators require the value to be of the `JSON` logical type.

| Function                                  | Alias                    | Operator | Description                                                                                                                                                  |
| :---------------------------------------- | :----------------------- | :------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `json_exists(json, path)`                 |                          |          | Returns `true` if the supplied path exists in the `json`, and `false` otherwise.                                                                             |
| `json_extract(json, key_or_index)`        | `json_extract_path`      | `->`     | Extracts the named member (string key) or array element (integer index) as `JSON`. If the argument is a `LIST`, the result is a `LIST` of `JSON`.            |
| `json_extract_string(json, key_or_index)` | `json_extract_path_text` | `->>`    | Same as `->`, but returns the value as `VARCHAR` instead of `JSON`.                                                                                          |
| `json_value(json, path)`                  |                          |          | Extracts the scalar at the given [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) or JSONPath (`$.…`) `path`. Returns `NULL` if it is not a scalar. |

Note that the arrow operator `->`, which is used for JSON extracts, has a low precedence as it is also used in [lambda functions](../../sql/functions/lambda.md). Therefore, you need to surround the `->` operator with parentheses when expressing operations such as equality comparisons (`=`).
For example:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_001" />

<DocCallout type="attention">
SereneDB's JSON data type uses [0-based indexing](../../data_import_and_export/json/overview.md#indexing).
</DocCallout>

The examples below use this dataset:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_002" />

Extract a member by name — `json_extract` and the `->` operator are equivalent and both return `JSON`:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_003" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_004" />

Use `->>` to return the value as text (`VARCHAR`) instead of `JSON`:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_005" />

The operators chain, so a nested array comes back as `JSON`:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_006" />

Index into an array with an integer ([0-based](../../data_import_and_export/json/overview.md#indexing)); negative indices count from the end:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_007" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_008" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_009" />

`json_extract_string` is the function form of `->>`:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_010" />

To address a value by a JSON Pointer or JSONPath location, use `json_value` — it accepts both the JSONPath `$.…` form and the `/…` pointer form:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_011" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_012" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_013" />

When the input is a plain `VARCHAR` string rather than the `JSON` type, `json_extract` accepts a JSONPath directly:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_014" />

When several values are needed from the same JSON, calling `json_extract` once per value parses the document repeatedly:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_015" />

Passing a `LIST` of keys extracts them all in a single pass, which is faster and uses less memory:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_016" />

## JSON Scalar Functions

The following scalar JSON functions can be used to gain information about the stored JSON values.
With the exception of `json_valid(json)`, all JSON functions produce an error when invalid JSON is supplied.

We support two kinds of notations to describe locations within JSON: [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901) and JSONPath.

| Function                                    | Description                                                                                                                                                                                                                                                                       |
| :------------------------------------------ | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `json_array_length(json[, path])`           | Return the number of elements in the JSON array `json`, or `0` if it is not a JSON array. If `path` is specified, return the number of elements in the JSON array at the given `path`. If `path` is a `LIST`, the result will be `LIST` of array lengths.                         |
| `json_contains(json_haystack, json_needle)` | Returns `true` if `json_needle` is contained in `json_haystack`. Both parameters are of JSON type, but `json_needle` can also be a numeric value or a string, however the string must be wrapped in double quotes.                                                                |
| `json_keys(json[, path])`                   | Returns the keys of `json` as a `LIST` of `VARCHAR`, if `json` is a JSON object. If `path` is specified, return the keys of the JSON object at the given `path`. If `path` is a `LIST`, the result will be `LIST` of `LIST` of `VARCHAR`.                                         |
| `json_structure(json)`                      | Return the structure of `json`. Defaults to `JSON` if the structure is inconsistent (e.g., incompatible types in an array).                                                                                                                                                       |
| `json_type(json[, path])`                   | Return the type of the supplied `json`, which is one of `ARRAY`, `BIGINT`, `BOOLEAN`, `DOUBLE`, `OBJECT`, `UBIGINT`, `VARCHAR` and `NULL`. If `path` is specified, return the type of the element at the given `path`. If `path` is a `LIST`, the result will be `LIST` of types. |
| `json_valid(json)`                          | Return whether `json` is valid JSON.                                                                                                                                                                                                                                              |
| `json(json)`                                | Parse and minify `json`.                                                                                                                                                                                                                                                          |

The JSONPointer syntax separates each field with a `/`.
For example, to extract the first element of the array with key `mercury`, you can do:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_017" />

The JSONPath syntax separates fields with a `.`, and accesses array elements with `[i]`, and always starts with `$`. Using the same example, we can do the following:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_018" />

Note that SereneDB's JSON data type uses [0-based indexing](../../data_import_and_export/json/overview.md#indexing).

JSONPath is more expressive, and can also access from the back of lists:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_019" />

JSONPath also allows escaping syntax tokens, using double quotes:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_020" />

Examples using a solar-system dataset:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_021" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_022" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_023" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_024" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_025" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_026" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_027" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_028" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_029" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_030" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_031" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_032" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_033" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_034" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_035" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_036" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_037" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_038" />

## JSON Aggregate Functions

There are three JSON aggregate functions.

| Function                        | Description                                                            |
| :------------------------------ | :--------------------------------------------------------------------- |
| `json_group_array(any)`         | Return a JSON array with all values of `any` in the aggregation.       |
| `json_group_object(key, value)` | Return a JSON object with all `key`, `value` pairs in the aggregation. |
| `json_group_structure(json)`    | Return the combined `json_structure` of all `json` in the aggregation. |

Examples:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_039" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_040" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_041" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_042" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_043" />

## Transforming JSON to Nested Types

In many cases, it is inefficient to extract values from JSON one-by-one.
Instead, we can “extract” all values at once, transforming JSON to the nested types `LIST` and `STRUCT`.

| Function                                 | Description                                                            |
| :--------------------------------------- | :--------------------------------------------------------------------- |
| `json_transform(json, structure)`        | Transform `json` according to the specified `structure`.               |
| `from_json(json, structure)`             | Alias for `json_transform`.                                            |
| `json_transform_strict(json, structure)` | Same as `json_transform`, but throws an error when type casting fails. |
| `from_json_strict(json, structure)`      | Alias for `json_transform_strict`.                                     |

The `structure` argument is JSON of the same form as returned by `json_structure`.
The `structure` argument can be modified to transform the JSON into the desired structure and types.
It is possible to extract fewer key/value pairs than are present in the JSON, and it is also possible to extract more: missing keys become `NULL`.

Examples:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_044" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_045" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_046" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_047" />

## JSON Table Functions

SereneDB implements two JSON table functions that take a JSON value and produce a table from it.

| Function                 | Description                                                                                  |
| :----------------------- | :------------------------------------------------------------------------------------------- |
| `json_each(json[ ,path]` | Traverse `json` and return one row for each element in the top-level array or object.        |
| `json_tree(json[ ,path]` | Traverse `json` in depth-first fashion and return one row for each element in the structure. |

If the element is not an array or object, the element itself is returned.
If the optional `path` argument is supplied, traversal starts from the element at the given path instead of the root element.

The resulting table has the following columns:

| Field     | Type               | Description                                 |
| :-------- | :----------------- | :------------------------------------------ |
| `key`     | `VARCHAR`          | Key of element relative to its parent       |
| `value`   | `JSON`             | Value of element                            |
| `type`    | `VARCHAR`          | `json_type` (function) of this element      |
| `atom`    | `JSON`             | `json_value` (function) of this element     |
| `id`      | `UBIGINT`          | Element identifier, numbered by parse order |
| `parent`  | `UBIGINT`          | `id` of parent element                      |
| `fullkey` | `VARCHAR`          | JSON path to element                        |
| `path`    | `VARCHAR`          | JSON path to parent element                 |
| `json`    | `JSON` (Virtual)   | The `json` parameter                        |
| `root`    | `TEXT` (Virtual)   | The `path` parameter                        |
| `rowid`   | `BIGINT` (Virtual) | The row identifier                          |

These functions are analogous to [SQLite's functions with the same name](https://www.sqlite.org/json1.html#jeach).
Note that, because the `json_each` and `json_tree` functions refer to previous subqueries in the same FROM clause, they are [_lateral joins_](../../sql/query_syntax/from_and_join/index.md#lateral-joins).

Examples:

<SqlLogicTest id="data_import_and_export/json/json_functions/example_048" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_049" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_050" />

<SqlLogicTest id="data_import_and_export/json/json_functions/example_051" />
