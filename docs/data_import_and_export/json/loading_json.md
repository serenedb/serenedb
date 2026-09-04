---
title: Loading JSON
sidebar_position: 3
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The SereneDB JSON reader can automatically infer which configuration flags to use by analyzing the JSON file. This will work correctly in most situations, and should be the first option attempted. In rare situations where the JSON reader cannot figure out the correct configuration, it is possible to manually configure the JSON reader to correctly parse the JSON file.

## The `read_json` Function

The `read_json` is the simplest method of loading JSON files: it automatically attempts to figure out the correct configuration of the JSON reader. It also automatically deduces types of columns.
In the following example, we use the <a href="/files/docs/todos.json" download>`todos.json`</a> file,

<SqlLogicTest id="data_import_and_export/json/loading_json/example_001" />

We can use `read_json` to create a persistent table as well:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_002" />

If we specify types for a subset of columns, `read_json` excludes columns that we don't specify. Note that only the `userId` and `completed` columns are shown:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_003" />

Multiple files can be read at once by providing a glob or a list of files. Refer to the [multiple files section](../../data_import_and_export/multiple_files/overview.md) for more information.

## Functions for Reading JSON Objects

The following table functions are used to read JSON:

| Function                           | Description                                                                                         |
| :--------------------------------- | :-------------------------------------------------------------------------------------------------- |
| `read_json_objects(filename)`      | Read a JSON object from `filename`, where `filename` can also be a list of files or a glob pattern. |
| `read_ndjson_objects(filename)`    | Alias for `read_json_objects` with the parameter `format` set to `newline_delimited`.               |
| `read_json_objects_auto(filename)` | Alias for `read_json_objects` with the parameter `format` set to `auto` .                           |

### Parameters

These functions have the following parameters:

| Name                   | Description                                                                                                                                                                                                                | Type       | Default         |
| :--------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------- | :-------------- |
| `compression`          | The compression type for the file. By default this will be detected automatically from the file extension (e.g., `t.json.gz` will use gzip, `t.json` will use none). Options are `none`, `gzip`, `zstd` and `auto_detect`. | `VARCHAR`  | `auto_detect`   |
| `filename`             | Whether or not an extra `filename` column should be included in the result. The `filename` column is added automatically as a virtual column and this option is only kept for compatibility reasons.                       | `BOOL`     | `false`         |
| `format`               | Can be one of `auto`, `unstructured`, `newline_delimited` and `array`.                                                                                                                                                     | `VARCHAR`  | `array`         |
| `hive_partitioning`    | Whether or not to interpret the path as a [Hive partitioned path](../../data_import_and_export/partitioning/hive_partitioning.md).                                                                                         | `BOOL`     | (auto-detected) |
| `ignore_errors`        | Whether to ignore parse errors (only possible when `format` is `newline_delimited`).                                                                                                                                       | `BOOL`     | `false`         |
| `maximum_sample_files` | The maximum number of JSON files sampled for auto-detection.                                                                                                                                                               | `BIGINT`   | `32`            |
| `maximum_object_size`  | The maximum size of a JSON object (in bytes).                                                                                                                                                                              | `UINTEGER` | `16777216`      |

The `format` parameter specifies how to read the JSON from a file.
With `unstructured`, the top-level JSON is read, e.g., for `data.json`:

```json
{
  "a": 42
}
{
  "b": [1, 2, 3]
}
```

<SqlLogicTest id="data_import_and_export/json/loading_json/example_004" />

With `newline_delimited`, [NDJSON](https://github.com/ndjson/ndjson-spec) is read, where each JSON is separated by a newline (`\n`), e.g., for `data-nd.json`:

```json
{"a": 42}
{"b": [1, 2, 3]}
```

<SqlLogicTest id="data_import_and_export/json/loading_json/example_005" />

With `array`, each array element is read, e.g., for `data-array.json`:

```json
[
    {
        "a": 42
    },
    {
        "b": [1, 2, 3]
    }
]
```

<SqlLogicTest id="data_import_and_export/json/loading_json/example_006" />

## Functions for Reading JSON as a Table

SereneDB also supports reading JSON as a table, using the following functions:

| Function                     | Description                                                                                 |
| :--------------------------- | :------------------------------------------------------------------------------------------ |
| `read_json(filename)`        | Read JSON from `filename`, where `filename` can also be a list of files, or a glob pattern. |
| `read_json_auto(filename)`   | Alias for `read_json`.                                                                      |
| `read_ndjson(filename)`      | Alias for `read_json` with parameter `format` set to `newline_delimited`.                   |
| `read_ndjson_auto(filename)` | Alias for `read_json` with parameter `format` set to `newline_delimited`.                   |

### Parameters

Besides the `maximum_object_size`, `format`, `ignore_errors` and `compression`, these functions have additional parameters:

| Name                         | Description                                                                                                                                                                                                                                                                                                                         | Type      | Default   |
| :--------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :-------- |
| `auto_detect`                | Whether to auto-detect the names of the keys and data types of the values automatically                                                                                                                                                                                                                                             | `BOOL`    | `true`    |
| `columns`                    | A struct that specifies the key names and value types contained within the JSON file (e.g., `{key1: 'INTEGER', key2: 'VARCHAR'}`). If `auto_detect` is enabled these will be inferred                                                                                                                                               | `STRUCT`  | `(empty)` |
| `dateformat`                 | Specifies the date format to use when parsing dates. See [Date Format](../../sql/functions/dateformat.md)                                                                                                                                                                                                                           | `VARCHAR` | `iso`     |
| `maximum_depth`              | Maximum nesting depth to which the automatic schema detection detects types. Set to -1 to fully detect nested JSON types                                                                                                                                                                                                            | `BIGINT`  | `-1`      |
| `records`                    | Can be one of `auto`, `true`, `false`                                                                                                                                                                                                                                                                                               | `VARCHAR` | `auto`    |
| `sample_size`                | Option to define number of sample objects for automatic JSON type detection. Set to -1 to scan the entire input file                                                                                                                                                                                                                | `UBIGINT` | `20480`   |
| `timestampformat`            | Specifies the date format to use when parsing timestamps. See [Date Format](../../sql/functions/dateformat.md). When set to `iso` (the default), ISO 8601 timestamps with timezone offsets (e.g., `2024-01-01T12:00:00+05:00`) and fractional seconds (e.g., `2024-01-01T12:00:00.123Z`) are automatically inferred as `TIMESTAMP`. | `VARCHAR` | `iso`     |
| `union_by_name`              | Whether the schemas of multiple JSON files should be [unified](../../data_import_and_export/multiple_files/combining_schemas.md)                                                                                                                                                                                                    | `BOOL`    | `false`   |
| `map_inference_threshold`    | Controls the threshold for number of columns whose schema will be auto-detected; if JSON schema auto-detection would infer a `STRUCT` type for a field that has _more_ than this threshold number of subfields, it infers a `MAP` type instead. Set to `-1` to disable `MAP` inference.                                             | `BIGINT`  | `200`     |
| `field_appearance_threshold` | The JSON reader divides the number of appearances of each JSON field by the auto-detection sample size. If the average over the fields of an object is less than this threshold, it will default to using a `MAP` type with value type of merged field types.                                                                       | `DOUBLE`  | `0.1`     |

Note that SereneDB can convert JSON arrays directly to its internal `LIST` type, and missing keys become `NULL`:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_007" />

`read_json` reads NDJSON / JSONL files directly with `format = 'newline_delimited'`; pairing it with explicit `columns` skips auto-detection, which is the robust choice when a key holds values of mixed shapes across lines:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_016" />

SereneDB can automatically detect the types like so:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_008" />

SereneDB can read (and auto-detect) a variety of formats, specified with the `format` parameter.
Querying a JSON file that contains an `array`, e.g.:

```json
[
    {
        "a": 42,
        "b": 4.2
    },
    {
        "a": 43,
        "b": 4.3
    }
]
```

Can be queried exactly the same as a JSON file that contains `unstructured` JSON, e.g.:

```json
{
    "a": 42,
    "b": 4.2
}
{
    "a": 43,
    "b": 4.3
}
```

Both can be read as the table:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_009" />

If your JSON file does not contain “records”, i.e., any other type of JSON than objects, SereneDB can still read it.
This is specified with the `records` parameter.
The `records` parameter specifies whether the JSON contains records that should be unpacked into individual columns.
SereneDB also attempts to auto-detect this.
For example, take the following file, `data-records.json`:

```json
{"a": 42, "b": [1, 2, 3]}
{"a": 43, "b": [4, 5, 6]}
```

<SqlLogicTest id="data_import_and_export/json/loading_json/example_010" />

You can read the same file with `records` set to `false`, to get a single column, which is a `STRUCT` containing the data:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_015" />

## Loading with the `COPY` Statement Using `FORMAT json`

`FORMAT json` is supported for `COPY FROM`, `IMPORT DATABASE`, as well as `COPY TO` and `EXPORT DATABASE`. See the [`COPY` statement](../../sql/statements/copy/index.md) and the [`IMPORT` / `EXPORT` clauses](../../sql/statements/export_and_import_database/index.md).

By default, `COPY` expects newline-delimited JSON. If you prefer copying data to/from a JSON array, you can specify `ARRAY true`, e.g.,

<SqlLogicTest id="data_import_and_export/json/loading_json/example_011" />

will create the following file:

```json
[{ "i": 0 }, { "i": 1 }, { "i": 2 }, { "i": 3 }, { "i": 4 }]
```

This can be read back to SereneDB as follows:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_012" />

The format can be detected automatically like so:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_013" />

We can also create a table from the auto-detected schema:

<SqlLogicTest id="data_import_and_export/json/loading_json/example_014" />

### Parameters

| Name                          | Description                                                                                                                                                                                                                        | Type       | Default       |
| :---------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------- | :------------ |
| `auto_detect`                 | Whether to auto-detect the names of the keys and data types of the values automatically                                                                                                                                            | `BOOL`     | `false`       |
| `columns`                     | A struct that specifies the key names and value types contained within the JSON file (e.g., `{key1: 'INTEGER', key2: 'VARCHAR'}`). If `auto_detect` is enabled these will be inferred                                              | `STRUCT`   | `(empty)`     |
| `compression`                 | The compression type for the file. By default this will be detected automatically from the file extension (e.g., `t.json.gz` will use gzip, `t.json` will use none). Options are `uncompressed`, `gzip`, `zstd` and `auto_detect`. | `VARCHAR`  | `auto_detect` |
| `convert_strings_to_integers` | Whether strings representing integer values should be converted to a numerical type.                                                                                                                                               | `BOOL`     | `false`       |
| `dateformat`                  | Specifies the date format to use when parsing dates. See [Date Format](../../sql/functions/dateformat.md)                                                                                                                          | `VARCHAR`  | `iso`         |
| `filename`                    | Whether or not an extra `filename` column should be included in the result.                                                                                                                                                        | `BOOL`     | `false`       |
| `format`                      | Can be one of `auto, unstructured, newline_delimited, array`                                                                                                                                                                       | `VARCHAR`  | `array`       |
| `hive_partitioning`           | Whether or not to interpret the path as a [Hive partitioned path](../../data_import_and_export/partitioning/hive_partitioning.md).                                                                                                 | `BOOL`     | `false`       |
| `ignore_errors`               | Whether to ignore parse errors (only possible when `format` is `newline_delimited`)                                                                                                                                                | `BOOL`     | `false`       |
| `maximum_depth`               | Maximum nesting depth to which the automatic schema detection detects types. Set to -1 to fully detect nested JSON types                                                                                                           | `BIGINT`   | `-1`          |
| `maximum_object_size`         | The maximum size of a JSON object (in bytes)                                                                                                                                                                                       | `UINTEGER` | `16777216`    |
| `records`                     | Can be one of `auto`, `true`, `false`                                                                                                                                                                                              | `VARCHAR`  | `records`     |
| `sample_size`                 | Option to define number of sample objects for automatic JSON type detection. Set to -1 to scan the entire input file                                                                                                               | `UBIGINT`  | `20480`       |
| `timestampformat`             | Specifies the date format to use when parsing timestamps. See [Date Format](../../sql/functions/dateformat.md)                                                                                                                     | `VARCHAR`  | `iso`         |
| `union_by_name`               | Whether the schemas of multiple JSON files should be [unified](../../data_import_and_export/multiple_files/combining_schemas.md).                                                                                                  | `BOOL`     | `false`       |
