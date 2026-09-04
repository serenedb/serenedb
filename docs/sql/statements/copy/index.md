---
title: COPY
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## Examples

Read a CSV file into the `lineitem` table, using auto-detected CSV options:

<SqlLogicTest id="sql/statements/copy/index/example_001" />

Read a CSV file into the `lineitem` table, using manually specified CSV options:

<SqlLogicTest id="sql/statements/copy/index/example_002" />

Read a Parquet file into the `lineitem` table:

<SqlLogicTest id="sql/statements/copy/index/example_003" />

Read a JSON file into the `lineitem` table, using auto-detected options:

<SqlLogicTest id="sql/statements/copy/index/example_004" />

Read a CSV file into the `lineitem` table, using double quotes:

<SqlLogicTest id="sql/statements/copy/index/example_005" />

Write a table to a CSV file:

<SqlLogicTest id="sql/statements/copy/index/example_007" />

Write a table to a CSV file, using double quotes:

<SqlLogicTest id="sql/statements/copy/index/example_008" />

Write the result of a query to a Parquet file:

<SqlLogicTest id="sql/statements/copy/index/example_010" />

Copy the entire content of database `db1` to database `db2`:

<SqlLogicTest id="sql/statements/copy/index/example_011" />

Copy only the schema (catalog elements) but not any data:

<SqlLogicTest id="sql/statements/copy/database_schema_only/example_012" />

## Overview

`COPY` moves data between SereneDB and external files. `COPY ... FROM` imports data into SereneDB from an external file. `COPY ... TO` writes data from SereneDB to an external file. The `COPY` command can be used for `CSV`, `PARQUET` and `JSON` files.

## `COPY ... FROM`

`COPY ... FROM` imports data from an external file into an existing table. The data is appended to whatever data is in the table already. The amount of columns inside the file must match the amount of columns in the table `tbl`, and the contents of the columns must be convertible to the column types of the table. In case this is not possible, an error will be thrown.

If a list of columns is specified, `COPY` will only copy the data in the specified columns from the file. If there are any columns in the table that are not in the column list, `COPY ... FROM` will insert the default values for those columns.

Copy the contents of a comma-separated file `test.csv` without a header into the table `test`:

<SqlLogicTest id="sql/statements/copy/index/example_013" />

Copy the contents of a comma-separated file with a header into the `category` table:

<SqlLogicTest id="sql/statements/copy/index/example_014" />

Copy the contents of `lineitem.tbl` into the `lineitem` table, where the contents are delimited by a pipe character (`|`):

<SqlLogicTest id="sql/statements/copy/index/example_015" />

Copy the contents of `lineitem.tbl` into the `lineitem` table, where the delimiter, quote character, and presence of a header are automatically detected:

<SqlLogicTest id="sql/statements/copy/index/example_016" />

Read the contents of a comma-separated file `names.csv` into the `name` column of the `category` table. Any other columns of this table are filled with their default value:

<SqlLogicTest id="sql/statements/copy/index/example_017" />

Read the contents of a Parquet file `lineitem.parquet` into the `lineitem` table:

<SqlLogicTest id="sql/statements/copy/index/example_018" />

Read the contents of a newline-delimited JSON file `lineitem.ndjson` into the `lineitem` table:

<SqlLogicTest id="sql/statements/copy/index/example_019" />

Read the contents of a JSON file `lineitem.json` into the `lineitem` table:

<SqlLogicTest id="sql/statements/copy/index/example_020" />

An expression may be used as the source of a `COPY ... FROM` command if it is placed within parentheses.

Read the contents of a file whose path is stored in a variable into the `lineitem` table:

<SqlLogicTest id="sql/statements/copy/index/example_021" />

Read the contents of a file provided as parameter of a prepared statement into the `lineitem` table:

<SqlLogicTest id="sql/statements/copy/index/example_022" />

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

> To ensure compatibility with PostgreSQL, SereneDB accepts `COPY ... FROM` statements that do not fully comply with the railroad diagram shown here. For example, the following is a valid statement:
>
> <SqlLogicTest id="sql/statements/copy/index/example_023" />

## `COPY ... TO`

`COPY ... TO` exports data from SereneDB to an external CSV, Parquet, JSON or BLOB file. It has mostly the same set of options as `COPY ... FROM`, however, in the case of `COPY ... TO` the options specify how the file should be written to disk. Any file created by `COPY ... TO` can be copied back into the database by using `COPY ... FROM` with a similar set of options.

The `COPY ... TO` function can be called specifying either a table name, or a query. When a table name is specified, the contents of the entire table will be written into the resulting file. When a query is specified, the query is executed and the result of the query is written to the resulting file.

Copy the contents of the `lineitem` table to a CSV file with a header:

<SqlLogicTest id="sql/statements/copy/index/example_024" />

Copy the contents of the `lineitem` table to the file `lineitem.tbl`, where the columns are delimited by a pipe character (`|`), including a header line:

<SqlLogicTest id="sql/statements/copy/index/example_025" />

Use tab separators to create a TSV file without a header:

<SqlLogicTest id="sql/statements/copy/index/example_026" />

Copy the l_orderkey column of the `lineitem` table to the file `orderkey.tbl`:

<SqlLogicTest id="sql/statements/copy/index/example_027" />

Copy the result of a query to the file `query.csv`, including a header with column names:

<SqlLogicTest id="sql/statements/copy/index/example_028" />

Copy the result of a query to the Parquet file `query.parquet`:

<SqlLogicTest id="sql/statements/copy/index/example_029" />

Copy the result of a query to the newline-delimited JSON file `query.ndjson`:

<SqlLogicTest id="sql/statements/copy/index/example_030" />

Copy the result of a query to the JSON file `query.json`:

<SqlLogicTest id="sql/statements/copy/index/example_031" />

The `RETURN_STATS` option makes `COPY ... TO` return one row per written file, including the filename, row `count`, the file and footer sizes in bytes, per-column statistics (min, max, null count and size) and any partition keys:

<SqlLogicTest id="sql/statements/copy/index/example_032" />

Note: for nested columns (e.g., structs) the column statistics are defined for each part. For example, if we have a column `name STRUCT(field1 INTEGER, field2 INTEGER)` the column statistics will have stats for `name.field1` and `name.field2`.

An expression may be used as the target of a `COPY ... TO` command if it is placed within parentheses.

Copy the result of a query to a file whose path is stored in a variable:

<SqlLogicTest id="sql/statements/copy/index/example_033" />

Copy to a file provided as parameter of a prepared statement:

<SqlLogicTest id="sql/statements/copy/index/example_034" />

Expressions may be used for options as well. Copy to a file using a format stored in a variable:

<SqlLogicTest id="sql/statements/copy/index/example_035" />

### `COPY ... TO` Options

Zero or more copy options may be provided as a part of the copy operation. The `WITH` specifier is optional, but if any options are specified, the parentheses are required. Parameter values can be passed in with or without wrapping in single quotes. Arbitrary expressions may be used for parameter values.

Any option that is a Boolean can be enabled or disabled in multiple ways. You can write `true`, `ON`, or `1` to enable the option, and `false`, `OFF`, or `0` to disable it. The `BOOLEAN` value can also be omitted, e.g., by only passing `(HEADER)`, in which case `true` is assumed.

With few exceptions, the below options are applicable to all formats written with `COPY`.

| Name                      | Description                                                                                                                                                                                                                                                                                                                                                                                                                                 | Type                  | Default |
| :------------------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------- | :------ |
| `FORMAT`                  | Specifies the copy function to use. The default is selected from the file extension (e.g., `.parquet` results in a Parquet file being written/read). If the file extension is unknown `CSV` is selected. SereneDB provides the `CSV`, `PARQUET` and `JSON` copy functions.                                                                  | `VARCHAR`             | `auto`  |
| `USE_TMP_FILE`            | Whether or not to write to a temporary file first if the original file exists (`target.csv.tmp`). This prevents overwriting an existing file with a broken file in case the writing is cancelled.                                                                                                                                                                                                                                           | `BOOL`                | `auto`  |
| `OVERWRITE_OR_IGNORE`     | Whether or not to allow overwriting files if they already exist. Only has an effect when used with `PARTITION_BY`.                                                                                                                                                                                                                                                                                                                          | `BOOL`                | `false` |
| `OVERWRITE`               | When `true`, all existing files inside targeted directories will be removed (not supported on remote filesystems). Only has an effect when used with `PARTITION_BY`.                                                                                                                                                                                                                                                                        | `BOOL`                | `false` |
| `APPEND`                  | When `true`, in the event a filename pattern is generated that already exists, the path will be regenerated to ensure no existing files are overwritten. Only has an effect when used with `PARTITION_BY`.                                                                                                                                                                                                                                  | `BOOL`                | `false` |
| `FILENAME_PATTERN`        | Set a pattern to use for the filename, can optionally contain `{uuid}` / `{uuidv4}` or `{uuidv7}` to be filled in with a generated [UUID](../../data_types/numeric.md#universally-unique-identifiers-uuids) (v4 or v7, respectively), and `{i}`, which is replaced by an incrementing index. Only has an effect when used with `PARTITION_BY`.                                                                                              | `VARCHAR`             | `auto`  |
| `FILE_EXTENSION`          | Set the file extension that should be assigned to the generated file(s).                                                                                                                                                                                                                                                                                                                                                                    | `VARCHAR`             | `auto`  |
| `PER_THREAD_OUTPUT`       | When `true`, the `COPY` command generates one file per thread, rather than one file in total. This allows for faster parallel writing.                                                                                                                                                                                                                                                                                                      | `BOOL`                | `false` |
| `FILE_SIZE_BYTES`         | If this parameter is set, the `COPY` process creates a directory which will contain the exported files. If a file exceeds the set limit (specified as bytes such as `1000` or in human-readable format such as `1k`), the process creates a new file in the directory. This parameter works in combination with `PER_THREAD_OUTPUT`. Note that the size is used as an approximation, and files can be occasionally slightly over the limit. | `VARCHAR` or `BIGINT` | (empty) |
| `PARTITION_BY`            | The columns to partition by using a Hive partitioning scheme, see the [partitioned writes section](../../../data_import_and_export/partitioning/partitioned_writes.md).                                                                                                                                                                                                                                                                     | `VARCHAR[]`           | (empty) |
| `PRESERVE_ORDER`          | Whether or not to [preserve order](../../../compatibility/order_preservation.md) during the copy operation. Defaults to the value of the `preserve_insertion_order` [configuration option](../../../configuration/overview.md).                                                                                                                                                                                                                      | `BOOL`                | (\*)    |
| `RETURN_FILES`            | Whether or not to include the created filepath(s) (as a `files VARCHAR[]` column) in the query result.                                                                                                                                                                                                                                                                                                                                      | `BOOL`                | `false` |
| `RETURN_STATS`            | Whether or not to return the files and their column statistics that were written as part of the `COPY` statement.                                                                                                                                                                                                                                                                                                                           | `BOOL`                | `false` |
| `WRITE_PARTITION_COLUMNS` | Whether or not to write partition columns into files. Only has an effect when used with `PARTITION_BY`.                                                                                                                                                                                                                                                                                                                                     | `BOOL`                | `false` |

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

<DocCallout type="tip">
To ensure compatibility with PostgreSQL, SereneDB accepts `COPY ... TO` statements that do not fully comply with the railroad diagram shown here. For example, the following is a valid statement:

<SqlLogicTest id="sql/statements/copy/index/example_036" />
</DocCallout>

## `COPY FROM DATABASE ... TO`

The `COPY FROM DATABASE ... TO` statement copies the entire content from one attached database to another attached database. This includes the schema, including constraints, indexes, sequences, macros and the data itself.

<SqlLogicTest id="sql/statements/copy/copy_database_full/example_037" />

To only copy the **schema** of `db1` to `db2` but omit copying the data, add `SCHEMA` to the statement:

<SqlLogicTest id="sql/statements/copy/copy_database_schema/example_038" />

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />

## Format-Specific Options

### CSV Options

The below options are applicable when writing CSV files.

| Name              | Description                                                                                                                                                                                                                                          | Type        | Default |
| :---------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------- | :------ |
| `COMPRESSION`     | The compression type for the file. By default this will be detected automatically from the file extension (e.g., `file.csv.gz` will use `gzip`, `file.csv.zst` will use `zstd`, and `file.csv` will use `none`). Options are `none`, `gzip`, `zstd`. | `VARCHAR`   | `auto`  |
| `DATEFORMAT`      | Specifies the date format to use when writing dates. See [Date Format](../../functions/dateformat.md).                                                                                                                                               | `VARCHAR`   | (empty) |
| `DELIM` or `SEP`  | The character that is written to separate columns within each row.                                                                                                                                                                                   | `VARCHAR`   | `,`     |
| `ESCAPE`          | The character that should appear before a character that matches the `quote` value.                                                                                                                                                                  | `VARCHAR`   | `"`     |
| `FORCE_QUOTE`     | The list of columns to always add quotes to, even if not required.                                                                                                                                                                                   | `VARCHAR[]` | `[]`    |
| `HEADER`          | Whether or not to write a header for the CSV file.                                                                                                                                                                                                   | `BOOL`      | `true`  |
| `NULLSTR`         | The string that is written to represent a `NULL` value.                                                                                                                                                                                              | `VARCHAR`   | (empty) |
| `PREFIX`          | Prefixes the CSV file with a specified string. This option must be used in conjunction with `SUFFIX` and requires `HEADER` to be set to `false`.                                                                                                     | `VARCHAR`   | (empty) |
| `SUFFIX`          | Appends a specified string as a suffix to the CSV file. This option must be used in conjunction with `PREFIX` and requires `HEADER` to be set to `false`.                                                                                            | `VARCHAR`   | (empty) |
| `QUOTE`           | The quoting character to be used when a data value is quoted.                                                                                                                                                                                        | `VARCHAR`   | `"`     |
| `TIMESTAMPFORMAT` | Specifies the date format to use when writing timestamps. See [Date Format](../../functions/dateformat.md).                                                                                                                                          | `VARCHAR`   | (empty) |

### Parquet Options

The below options are applicable when writing Parquet files.

| Name                   | Description                                                                                                                                                                                                                                                                                                                                                                                               | Type      | Default                 |
| :--------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :---------------------- |
| `COMPRESSION`          | The compression format to use (`uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, `lz4_raw`).                                                                                                                                                                                                                                                                                                     | `VARCHAR` | `snappy`                |
| `COMPRESSION_LEVEL`    | Compression level, set between 1 (lowest compression, fastest) and 22 (highest compression, slowest). Only supported for zstd compression.                                                                                                                                                                                                                                                                | `BIGINT`  | `3`                     |
| `FIELD_IDS`            | The `field_id` for each column. Pass `auto` to attempt to infer automatically.                                                                                                                                                                                                                                                                                                                            | `STRUCT`  | (empty)                 |
| `ROW_GROUP_SIZE_BYTES` | The target size of each row group. You can pass either a human-readable string, e.g., `2MB`, or an integer, i.e., the number of bytes. This option is only used when you have issued `SET preserve_insertion_order = false;`, otherwise, it is ignored.                                                                                                                                                   | `BIGINT`  | `row_group_size * 1024` |
| `ROW_GROUP_SIZE`       | The target size, i.e., number of rows, of each row group.                                                                                                                                                                                                                                                                                                                                                 | `BIGINT`  | 122880                  |
| `ROW_GROUPS_PER_FILE`  | Create a new Parquet file if the current one has a specified number of row groups. If multiple threads are active, the number of row groups in a file may slightly exceed the specified number of row groups to limit the amount of locking – similarly to the behavior of `FILE_SIZE_BYTES`. However, if `per_thread_output` is set, only one thread writes to each file, and it becomes accurate again. | `BIGINT`  | (empty)                 |
| `PARQUET_VERSION`      | The Parquet version to use (`V1`, `V2`).                                                                                                                                                                                                                                                                                                                                                                  | `VARCHAR` | `V1`                    |

Some examples of `FIELD_IDS` are as follows.

Assign `field_ids` automatically:

<SqlLogicTest id="sql/statements/copy/index/example_039" />

Sets the `field_id` of column `i` to 42:

<SqlLogicTest id="sql/statements/copy/index/example_040" />

Sets the `field_id` of column `i` to 42, and column `j` to 43:

<SqlLogicTest id="sql/statements/copy/index/example_041" />

Sets the `field_id` of column `my_struct` to 42, and column `i` (nested inside `my_struct`) to 43:

<SqlLogicTest id="sql/statements/copy/index/example_042" />

Sets the `field_id` of column `my_list` to 42, and column `element` (default name of list child) to 43:

<SqlLogicTest id="sql/statements/copy/index/example_043" />

Sets the `field_id` of column `my_map` to 42, and columns `key` and `value` (default names of map children) to 43 and 44:

<SqlLogicTest id="sql/statements/copy/index/example_044" />

### JSON Options

The below options are applicable when writing `JSON` files.

| Name              | Description                                                                                                                                                                                                                                             | Type      | Default |
| :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------- | :------ |
| `ARRAY`           | Whether to write a JSON array. If `true`, a JSON array of records is written, if `false`, newline-delimited JSON is written                                                                                                                             | `BOOL`    | `false` |
| `COMPRESSION`     | The compression type for the file. By default this will be detected automatically from the file extension (e.g., `file.json.gz` will use `gzip`, `file.json.zst` will use `zstd`, and `file.json` will use `none`). Options are `none`, `gzip`, `zstd`. | `VARCHAR` | `auto`  |
| `DATEFORMAT`      | Specifies the date format to use when writing dates. See [Date Format](../../functions/dateformat.md).                                                                                                                                                  | `VARCHAR` | (empty) |
| `TIMESTAMPFORMAT` | Specifies the date format to use when writing timestamps. See [Date Format](../../functions/dateformat.md).                                                                                                                                             | `VARCHAR` | (empty) |

Sets the value of column `hello` to `HELLO!` and outputs the results to `hello.json`:

<SqlLogicTest id="sql/statements/copy/index/example_045" />

Sets the value of column `num_list` to `[1,2,3]` and outputs the results to `numbers.json`:

<SqlLogicTest id="sql/statements/copy/index/example_046" />

Sets the value of column `compression_type` to `gzip_explicit` and outputs the results to `compression.json.gz` with explicit compression:

<SqlLogicTest id="sql/statements/copy/index/example_047" />

Sets all values of single rows to be returned as nested arrays to `array_true.json`:

<SqlLogicTest id="sql/statements/copy/index/example_048" />

Sets all values of single rows to be returned as non-nested arrays to `array_false.json`:

<SqlLogicTest id="sql/statements/copy/index/example_049" />

### BLOB Options

The `BLOB` format option allows you to select a single column of a SereneDB table into a `.blob` file.
The column must be cast to the `BLOB` data type. For details on typecasting, see the
[Casting Operations Matrix](../../data_types/typecasting.md#casting-operations-matrix).

The below options are applicable when writing `BLOB` files.

| Name          | Description                                                                                                                                                                                                                                             | Type      | Default |
| :------------ | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------- | :------ |
| `COMPRESSION` | The compression type for the file. By default this will be detected automatically from the file extension (e.g., `file.blob.gz` will use `gzip`, `file.blob.zst` will use `zstd`, and `file.blob` will use `none`). Options are `none`, `gzip`, `zstd`. | `VARCHAR` | `auto`  |

Type casts the string value `foo` to the `BLOB` data type and outputs the results to `blob_output.blob`:

<SqlLogicTest id="sql/statements/copy/index/example_050" />

Type casts the string value `foo` to the `BLOB` data type and outputs the results to `blob_output_gzip.blob.gz` with `gzip` compression:

<SqlLogicTest id="sql/statements/copy/index/example_051" />

## Limitations

`COPY` does not support copying between tables. To copy between tables, use an [`INSERT statement`](../insert/index.md):

<SqlLogicTest id="sql/statements/copy/index/example_052" />
