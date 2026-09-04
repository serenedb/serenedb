---
title: Format Settings
sidebar_position: 7
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB can attempt to determine the format of a JSON file when setting `format` to `auto`.
Here are some example JSON files and the corresponding `format` settings that should be used.

In each of the below cases, the `format` setting was not needed, as SereneDB was able to infer it correctly, but it is included for illustrative purposes.
A query of this shape would work in each case:

<SqlLogicTest id="data_import_and_export/json/format_settings/example_001" />

## Format: `newline_delimited`

With `format = 'newline_delimited'` newline-delimited JSON can be parsed.
Each line is a JSON.

We use the example file <a href="/files/docs/records.json" download>`records.json`</a> with the following content:

```json
{"key1":"value1", "key2": "value1"}
{"key1":"value2", "key2": "value2"}
{"key1":"value3", "key2": "value3"}
```

<SqlLogicTest id="data_import_and_export/json/format_settings/example_002" />

## Format: `array`

If the JSON file contains a JSON array of objects (pretty-printed or not), `array` may be used.
To demonstrate its use, we use the example file <a href="/files/docs/records-in-array.json" download>`records-in-array.json`</a>:

```json
[
    { "key1": "value1", "key2": "value1" },
    { "key1": "value2", "key2": "value2" },
    { "key1": "value3", "key2": "value3" }
]
```

<SqlLogicTest id="data_import_and_export/json/format_settings/example_003" />

## Format: `unstructured`

If the JSON file contains JSON that is not newline-delimited or an array, `unstructured` may be used.
To demonstrate its use, we use the example file <a href="/files/docs/unstructured.json" download>`unstructured.json`</a>:

```json
{
    "key1":"value1",
    "key2":"value1"
}
{
    "key1":"value2",
    "key2":"value2"
}
{
    "key1":"value3",
    "key2":"value3"
}
```

<SqlLogicTest id="data_import_and_export/json/format_settings/example_004" />

## `records` Options

SereneDB can attempt to determine whether a JSON file contains records when setting `records = auto`.
When `records = true`, SereneDB expects JSON objects, and will unpack the fields of JSON objects into individual columns.

Continuing with the same example file, <a href="/files/docs/records.json" download>`records.json`</a>:

```json
{"key1":"value1", "key2": "value1"}
{"key1":"value2", "key2": "value2"}
{"key1":"value3", "key2": "value3"}
```

<SqlLogicTest id="data_import_and_export/json/format_settings/example_005" />

When `records = false`, SereneDB will not unpack the top-level objects, and create `STRUCT`s instead:

<SqlLogicTest id="data_import_and_export/json/format_settings/example_006" />

This is especially useful if we have non-object JSON, for example, <a href="/files/docs/arrays.json" download>`arrays.json`</a>:

```json
[1, 2, 3]
[4, 5, 6]
[7, 8, 9]
```

<SqlLogicTest id="data_import_and_export/json/format_settings/example_007" />
