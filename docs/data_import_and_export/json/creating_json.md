---
title: Creating JSON
sidebar_position: 2
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## JSON Creation Functions

The following functions are used to create JSON.

| Function                       | Description                                                                                                                                   |
| :----------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------- |
| `to_json(any)`                 | Create `JSON` from a value of `any` type. Our `LIST` is converted to a JSON array, and our `STRUCT` and `MAP` are converted to a JSON object. |
| `json_quote(any)`              | Alias for `to_json`.                                                                                                                          |
| `array_to_json(list)`          | Alias for `to_json` that only accepts `LIST`.                                                                                                 |
| `row_to_json(list)`            | Alias for `to_json` that only accepts `STRUCT`.                                                                                               |
| `json_array(any, ...)`         | Create a JSON array from the values in the argument lists.                                                                                    |
| `json_object(key, value, ...)` | Create a JSON object from `key`, `value` pairs in the argument list. Requires an even number of arguments.                                    |
| `json_merge_patch(json, json)` | Merge two JSON documents together.                                                                                                            |

Examples:

<SqlLogicTest id="data_import_and_export/json/creating_json/example_001" />

<SqlLogicTest id="data_import_and_export/json/creating_json/example_002" />

<SqlLogicTest id="data_import_and_export/json/creating_json/example_003" />

<SqlLogicTest id="data_import_and_export/json/creating_json/example_004" />

<SqlLogicTest id="data_import_and_export/json/creating_json/example_005" />

<SqlLogicTest id="data_import_and_export/json/creating_json/example_006" />

<SqlLogicTest id="data_import_and_export/json/creating_json/example_007" />
