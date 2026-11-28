---
title: Overview
sidebar_position: 1
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Examples

Write a table to an Avro file:

<SqlLogicTest id="data_import_and_export/avro/overview/example_001" />

Write the result of a query to a zstandard-compressed Avro file:

<SqlLogicTest id="data_import_and_export/avro/overview/example_002" />

Query an Avro file by its name:

<SqlLogicTest id="data_import_and_export/avro/overview/example_003" />

Read an Avro file with the `read_avro` function:

<SqlLogicTest id="data_import_and_export/avro/overview/example_004" />

Read a list of files as one table:

<SqlLogicTest id="data_import_and_export/avro/overview/example_005" />

Load an Avro file into a table:

<SqlLogicTest id="data_import_and_export/avro/overview/example_006" />

Query the codec of an Avro file:

<SqlLogicTest id="data_import_and_export/avro/overview/example_007" />

## Avro Files

[Apache Avro](https://avro.apache.org/) is a row-oriented binary format. An Avro object container file starts with a header that holds the schema of its records as JSON, the codec that compresses them and other key-value metadata; the records follow in blocks. SereneDB reads these files with [`read_avro`](#reading-avro-files) and [`COPY ... FROM`](#loading-avro-files-into-tables), writes them with [`COPY ... TO`](#writing-avro-files) and returns their header with [`avro_metadata`](#metadata).

## Reading Avro Files

`read_avro` reads a file, a list of files or the files that match a [glob](../multiple_files/overview.md#glob-syntax) as one table. A file whose name ends in `.avro` can also be queried by its name, as in `SELECT * FROM 'fruit.avro'`. `read_avro` reads blocks in the codecs `null` (uncompressed), `deflate`, `snappy` and `zstandard`, and a large file on several threads. Only the columns a query uses are decoded, and filters on its columns are applied as the file is read. `read_avro` takes these parameters:

| Name                | Description                                                                                                                              | Type   | Default         |
| :------------------ | :--------------------------------------------------------------------------------------------------------------------------------------- | :----- | :-------------- |
| `filename`          | Whether to add a `filename` column with the file each row comes from.                                                                    | `BOOL` | `false`         |
| `hive_partitioning` | Whether to interpret the path as a [Hive partitioned path](../partitioning/hive_partitioning.md).                                        | `BOOL` | (auto-detected) |
| `union_by_name`     | Whether to [unify the columns](../multiple_files/combining_schemas.md) of files with different schemas by name, rather than by position. | `BOOL` | `false`         |

A `snappy` block whose CRC32 checksum does not match its data fails the query. So does a file in another codec, such as `bzip2` or `xz`, with the error `Unknown codec bzip2, the supported codecs are null, deflate, snappy and zstandard`.

Read two files with different columns, matching the columns by name:

<SqlLogicTest id="data_import_and_export/avro/overview/union_by_name" />

### Row Numbers

The virtual column `file_row_number` identifies each record in its file. It is a `BIGINT` that increases along the file, but it is not the position of the record: use it to order or tell apart the records of a file, not to count them. A query returns it only when it names it:

<SqlLogicTest id="data_import_and_export/avro/overview/file_row_number" />

An [inverted index over a view](../../sql/indexes/inverted/views.md) of `read_avro` (`CREATE INDEX ... ON <view> USING inverted(...)`) keys its rows by `file_row_number`, and through it reads the columns it does not store back from the file. Reading `file_row_number` fails for a record in a block that starts 512 GiB or more into its file, or after the first 16,777,216 records of its block.

## Loading Avro Files into Tables

`COPY ... FROM` loads an Avro file, or all the files that match a glob, into a table, with `FORMAT avro` or a file name that ends in `.avro`:

<SqlLogicTest id="data_import_and_export/avro/overview/copy_from_glob" />

## Avro Types

When the schema of a file is a record, or a union of `null` and a record, each field of the record is a column. Any other schema is one column, named after the `name` of the schema, or `avro_schema` when it has none. The Avro types are read as these SQL types:

| Avro type                          | SQL type                                                                    |
| :--------------------------------- | :-------------------------------------------------------------------------- |
| `null`                             | `NULL`, which holds only `NULL`                                             |
| `boolean`                          | `BOOLEAN`                                                                   |
| `int`                              | `INTEGER`                                                                   |
| `long`                             | `BIGINT`                                                                    |
| `float`                            | `FLOAT` (`REAL`)                                                            |
| `double`                           | `DOUBLE`                                                                    |
| `bytes`                            | `BLOB`                                                                      |
| `string`                           | `VARCHAR`                                                                   |
| `fixed`                            | `BLOB`                                                                      |
| `enum`                             | `ENUM` of the symbols of the enum                                           |
| `array`                            | `LIST` of the type of the items                                             |
| `map`                              | `MAP` from `VARCHAR` to the type of the values                              |
| `record`                           | `STRUCT` of the fields of the record, or `NULL` for a record without fields |
| union of `null` and one other type | that type                                                                   |
| any other union                    | `UNION`, see [Unions](#unions)                                              |

A `string` must be valid UTF-8: a file with a `string` that is not fails the query. A schema in which a record contains itself, such as a linked list, cannot be read: `Recursive Avro types not supported: <record name>`.

### Logical Types

A logical type reads as the SQL type that holds its values:

| Logical type                                       | Avro type                          | SQL type                                                                |
| :------------------------------------------------- | :--------------------------------- | :---------------------------------------------------------------------- |
| `date`                                             | `int`                              | `DATE`                                                                  |
| `time-millis`                                      | `int`                              | `TIME`                                                                  |
| `time-micros`                                      | `long`                             | `TIME`                                                                  |
| `timestamp-millis`, `timestamp-micros`             | `long`                             | `TIMESTAMP`, or `TIMESTAMP WITH TIME ZONE` with `"adjust-to-utc": true` |
| `timestamp-nanos`                                  | `long`                             | `TIMESTAMP_NS`                                                          |
| `local-timestamp-millis`, `local-timestamp-micros` | `long`                             | `TIMESTAMP`                                                             |
| `local-timestamp-nanos`                            | `long`                             | `TIMESTAMP_NS`                                                          |
| `decimal`                                          | `bytes` or `fixed`                 | `DECIMAL(precision, scale)`                                             |
| `uuid`                                             | `string` or `fixed` of 16 bytes    | `UUID`                                                                  |
| `duration`                                         | `fixed` of 12 bytes                | `INTERVAL`                                                              |
| `map`                                              | `array` of records with two fields | `MAP` from the type of the first field to the type of the second        |

The `map` logical type is how Iceberg writes a map whose keys are not strings. A `decimal` with a precision above 38 fails the query, and so does a `timestamp-nanos` with `"adjust-to-utc": true`. Any other logical type reads as its Avro type.

### Unions

A union of `null` and one other type reads as that type, with `NULL` for the values in the `null` branch. Any other union reads as a [`UNION`](../../sql/data_types/union.md) with a member for each branch but `null`, named `u` and the position of the branch in the Avro union, counted from 0; a value in the `null` branch reads as `NULL`. With `events.avro`, whose records have this schema:

```json
{
    "type": "record",
    "name": "event",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "kind", "type": {"type": "enum", "name": "kind", "symbols": ["click", "view"]}},
        {"name": "note", "type": ["null", "string"]},
        {"name": "value", "type": ["null", "int", "string"]},
        {"name": "attrs", "type": {"type": "map", "values": "long"}}
    ]
}
```

<SqlLogicTest id="data_import_and_export/avro/overview/union_types" />

`union_tag` names the member that holds each value:

<SqlLogicTest id="data_import_and_export/avro/overview/union_values" />

## Writing Avro Files

`COPY ... TO` writes a table or the result of a query as an Avro object container file, with `FORMAT avro` or a file name that ends in `.avro`, and writes the rows on several threads. Besides the [`COPY ... TO` options](../../sql/statements/copy/index.md#copy--to-options) of all formats, the `avro` format takes these options:

| Name                   | Description                                                                                                                                                                                                                                            | Type      | Default |
| :--------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :------ |
| `CODEC`                | The codec that compresses the blocks of records: `null` (uncompressed), `deflate`, `snappy` or `zstandard`. The name is case-insensitive.                                                                                                              | `VARCHAR` | `null`  |
| `ROOT_NAME`            | The name of the record at the root of the schema. It must match `[A-Za-z_][A-Za-z0-9_]*`.                                                                                                                                                              | `VARCHAR` | `root`  |
| `FIELD_IDS`            | The Iceberg field ids of the columns and of their nested fields, as a struct: `FIELD_IDS {id: 1, point: {__duckdb_field_id: 2, x: 3, y: 4}}`. See [Field IDs](#field-ids).                                                                             | `STRUCT`  | (none)  |
| `SANITIZE_FIELD_NAMES` | Whether to make the names of columns and nested fields valid Avro names: a leading digit `d` becomes `_d`, and every other byte that an Avro name cannot hold becomes `_x` and its value in hexadecimal, so that `unit price` becomes `unit_x20price`. | `BOOL`    | `false` |
| `METADATA`             | Key-value metadata to write into the header of the file, as a struct: `METADATA {'source': 'orchard'}`.                                                                                                                                                | `STRUCT`  | (none)  |

Every column, and every value nested in one, is written as a union of `null` and its type, so that it can hold `NULL`, unless [`FIELD_IDS`](#field-ids) writes it as not nullable. A column of type `NULL` is written as the Avro type `null`.

Over the PostgreSQL wire protocol, `COPY ... TO STDOUT (FORMAT avro)` sends the file to the client and `COPY ... FROM STDIN (FORMAT avro)` loads one from it: see [`STDIN` and `STDOUT`](../../sql/statements/copy/index.md#stdin-and-stdout).

These column types are written: `BOOLEAN`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `VARCHAR`, `BLOB`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMP_MS`, `TIMESTAMP_NS`, `TIMESTAMP WITH TIME ZONE`, `UUID` and `DECIMAL`, and `STRUCT`, `LIST` and `MAP` of them. A `STRUCT` is written as a record, a `LIST` as an array, and a `MAP` as an array of key-value records with the logical type `map`, so that its keys can be of any type. The temporal types, `UUID` and `DECIMAL` are written with their logical types, and `read_avro` reads each column back as the type it was written from, a `TIMESTAMP_MS` as a `TIMESTAMP`:

<SqlLogicTest id="data_import_and_export/avro/overview/write_types" />

A column of another type, such as `ENUM` or `UNION`, cannot be written:

<SqlLogicTest id="data_import_and_export/avro/overview/enum_error" />

Cast it to one of the types above, such as `TEXT`:

<SqlLogicTest id="data_import_and_export/avro/overview/enum_text" />

Any other codec fails the `COPY`:

<SqlLogicTest id="data_import_and_export/avro/overview/codec_error" />

The names of the columns and of their nested fields must be valid Avro names, which match `[A-Za-z_][A-Za-z0-9_]*`, or the `COPY` fails:

<SqlLogicTest id="data_import_and_export/avro/overview/name_error" />

`SANITIZE_FIELD_NAMES` makes them valid, and `read_avro` returns the columns by their new names:

<SqlLogicTest id="data_import_and_export/avro/overview/sanitize_field_names" />

### Field IDs

`FIELD_IDS` writes Iceberg field ids into the schema: the id of a column, or of a field nested in one, becomes the `field-id` of its field, and the id of the items of a `LIST` the `element-id` of its array. `FIELD_IDS` is a struct from column names to ids. In place of an id, it takes a struct that holds the id under `__duckdb_field_id`, optionally `__duckdb_nullable: false` to write the field as not nullable, and the ids of the nested fields by the same rules: the fields of a `STRUCT` by their names, the items of a `LIST` as `list`, and the keys and values of a `MAP` as `key` and `value`:

<SqlLogicTest id="data_import_and_export/avro/overview/field_ids" />

A `NULL` in a field written as not nullable fails the `COPY`:

<SqlLogicTest id="data_import_and_export/avro/overview/field_ids_null" />

## Metadata

`avro_metadata` returns the key-value metadata in the header of one Avro file, a row per key, in the columns `key` and `value`. It holds the schema of the file as JSON under `avro.schema`, its codec under `avro.codec`, and the other keys that the writer of the file added, such as those of the `METADATA` option of `COPY ... TO`:

<SqlLogicTest id="data_import_and_export/avro/overview/metadata" />
