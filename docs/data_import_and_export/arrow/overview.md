---
title: Overview
sidebar_position: 1
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Examples

Write a table to an Arrow IPC file:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_001" />

Write the result of a query to an Arrow IPC stream:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_002" />

Query an Arrow IPC file by its name:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_003" />

Read an Arrow IPC stream with the `read_arrow` function:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_004" />

Read a list of files as one table:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_005" />

Write a zstd-compressed Arrow IPC stream:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_006" />

Load an Arrow IPC stream into a table:

<SqlLogicTest id="data_import_and_export/arrow/overview/example_007" />

## Arrow IPC Files and Streams

Arrow IPC is the binary format of [Apache Arrow](https://arrow.apache.org/) for columnar data, which it holds in record batches. It has two layouts, and SereneDB reads and writes both:

- An Arrow IPC **file** ends with a footer that lists its record batches. Through the footer a reader can go to any batch, so SereneDB reads a large file on several threads.
- An Arrow IPC **stream** has no footer and is read from start to end, as it arrives, so a client can send it with [`COPY ... FROM STDIN`](#streaming-over-copy).

`COPY ... TO` writes a file with `FORMAT arrow` or a file name that ends in `.arrow`, and a stream with `FORMAT arrows` or a name that ends in `.arrows`. `read_arrow` reads both layouts whatever the name of the file, and so does `COPY ... FROM` with `FORMAT arrow` or `FORMAT arrows`:

<SqlLogicTest id="data_import_and_export/arrow/overview/stream_extension" />

## Reading Arrow Files

`read_arrow` reads a file, a list of files or the files that match a [glob](../multiple_files/overview.md#glob-syntax) as one table. A file whose name ends in `.arrow` or `.arrows` can also be queried by its name, as in `SELECT * FROM 'fruit.arrow'`. Only the columns a query uses are decoded, and record batches compressed with `zstd` or `lz4` are decompressed as they are read. `read_arrow` takes these parameters:

| Name                | Description                                                                                                                              | Type   | Default         |
| :------------------ | :--------------------------------------------------------------------------------------------------------------------------------------- | :----- | :-------------- |
| `filename`          | Whether to add a `filename` column with the file each row comes from.                                                                    | `BOOL` | `false`         |
| `hive_partitioning` | Whether to interpret the path as a [Hive partitioned path](../partitioning/hive_partitioning.md).                                        | `BOOL` | (auto-detected) |
| `union_by_name`     | Whether to [unify the columns](../multiple_files/combining_schemas.md) of files with different schemas by name, rather than by position. | `BOOL` | `false`         |

Read two files with different columns, matching the columns by name:

<SqlLogicTest id="data_import_and_export/arrow/overview/union_by_name" />

## Writing Arrow Files

`COPY ... TO` writes a table or the result of a query as an Arrow IPC file or stream. Besides the [`COPY ... TO` options](../../sql/statements/copy/index.md#copy--to-options) of every format, such as `PARTITION_BY` and `FILE_SIZE_BYTES`, the `arrow` and `arrows` formats take these options:

| Name                             | Description                                                                                                                                                             | Type                  | Default             |
| :------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------- | :------------------ |
| `COMPRESSION`                    | The codec that compresses the record batches: `uncompressed`, `zstd` or `lz4`.                                                                                          | `VARCHAR`             | `uncompressed`      |
| `COMPRESSION_LEVEL`              | The level of the codec, from -131072 to 22 for `zstd` and from -65536 to 12 for `lz4`. A lower level compresses faster, a higher one smaller. Requires `COMPRESSION`. | `BIGINT`              | the codec's default |
| `ROW_GROUP_SIZE` or `CHUNK_SIZE` | The number of rows in a record batch. The number is approximate: a batch can hold more rows, up to the next multiple of 2,048.                                          | `BIGINT`              | 122,880             |
| `ROW_GROUP_SIZE_BYTES`           | The size of a record batch in bytes, as a number or a string such as `'2MB'`. With `ROW_GROUP_SIZE`, the smaller size applies.                                         | `BIGINT` or `VARCHAR` | (none)              |
| `ROW_GROUPS_PER_FILE`            | Start a new file after this many record batches, so that `COPY` writes a directory of files. With several threads, a file can hold a few more batches.                 | `BIGINT`              | (none)              |
| `KV_METADATA`                    | Key-value metadata of the schema, as a struct: `KV_METADATA {'source': 'sensors'}`.                                                                                     | `STRUCT`              | (none)              |
| `FIELD_METADATA`                 | Key-value metadata of columns, as a struct from column names to structs: `FIELD_METADATA {'weight': {'unit': 'g'}}`.                                                    | `STRUCT`              | (none)              |

A metadata value must not be `NULL`, and must be valid UTF-8 without NUL bytes.

Write a query result as a directory of Arrow IPC files with two record batches each, and read them back with a glob:

<SqlLogicTest id="data_import_and_export/arrow/overview/row_groups_per_file" />

An `ENUM` column cannot be written to Arrow:

<SqlLogicTest id="data_import_and_export/arrow/overview/enum_error" />

Cast it to `TEXT` instead:

<SqlLogicTest id="data_import_and_export/arrow/overview/enum_text" />

A column of a PostgreSQL type that Arrow has no type for, such as `macaddr`, `int4range` or `point`, is written as its text, and loads back into the same type: [PostgreSQL Types](../parquet/overview.md#postgresql-types) applies to Arrow as it does to Parquet.

## Loading Arrow Files into Tables

`COPY ... FROM` loads an Arrow IPC file or stream, or all the files that match a glob, into a table:

<SqlLogicTest id="data_import_and_export/arrow/overview/copy_from_glob" />

## Streaming over `COPY`

Over the PostgreSQL wire protocol, `COPY ... TO STDOUT` sends an Arrow IPC file (`FORMAT arrow`) or stream (`FORMAT arrows`) to the client, and `COPY ... FROM STDIN (FORMAT arrows)` loads a stream from the client. `COPY ... FROM STDIN` refuses an Arrow IPC file:

<SqlLogicTest id="data_import_and_export/arrow/overview/stdin_file" />

With `psql`, copy a table from one server to another, without a file in between:

```sh
psql -h source -c "COPY fruit TO STDOUT (FORMAT arrows)" | psql -h target -c "COPY fruit FROM STDIN (FORMAT arrows)"
```

See [`STDIN` and `STDOUT`](../../sql/statements/copy/index.md#stdin-and-stdout) for the other formats.
