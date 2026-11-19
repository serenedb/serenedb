---
title: COPY
split: headings
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

`COPY` moves data between SereneDB and external files. `COPY ... FROM` imports data into SereneDB from an external file. `COPY ... TO` writes data from SereneDB to an external file. The `COPY` command reads and writes the `text`, `csv` and `binary` formats of PostgreSQL, JSON, Parquet, and [Arrow IPC](../../../data_import_and_export/arrow/overview.md) files (`arrow`) and streams (`arrows`), and writes `BLOB` files. Over the PostgreSQL wire protocol, `COPY` also exchanges the data with the client: see [`STDIN` and `STDOUT`](#stdin-and-stdout).

## `COPY ... FROM`

`COPY ... FROM` imports data from an external file into an existing table. The data is appended to whatever data is in the table already. The amount of columns inside the file must match the amount of columns in the table `tbl`, and the contents of the columns must be convertible to the column types of the table. In case this is not possible, an error will be thrown.

If a list of columns is specified, `COPY` will only copy the data in the specified columns from the file. If there are any columns in the table that are not in the column list, `COPY ... FROM` will insert the default values for those columns.

A statement that names the format, as `FORMAT csv` or `CSV`, reads the CSV dialect of PostgreSQL: a comma between values, double quotes, a doubled quote inside a quoted value, no header, and an unquoted empty value read as `NULL` while a quoted one (`""`) is an empty string. The dialect is not detected from the file unless `AUTO_DETECT true` asks for it; only the line ending is. Without a format, a CSV file's delimiter, quoting and header are detected. Without a format, a file whose extension names no format, such as `.tbl`, `.tsv` or `.dat`, or that has no extension, is read and written in the `text` format.

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

### `COPY ... FROM STDIN`

Over the PostgreSQL wire protocol, `COPY ... FROM STDIN` in the `text`, `csv` and `binary` formats parses rows on several threads, up to the `threads` setting, while the client is still sending them. The table may store the rows in another order than they arrived in; see [Insertion Order](../../../compatibility/order_preservation.md#insertion-order). A client that sends slowly does not keep threads busy: they run other queries until more data arrives. The initial table copy of a [subscription](../create_subscription/index.md) loads rows the same way. The formats a client can send are listed under [`STDIN` and `STDOUT`](#stdin-and-stdout).

`COPY ... FROM 'file'` in the `text`, `csv` and `binary` formats also parses rows on several threads, with the same ordering rules. A `text` or `csv` file is read in parallel parts. A `binary` file, a file that cannot be read at an offset, such as a named pipe or a compressed file, and a file with an `ENCODING` other than UTF-8 are read in order on the asynchronous I/O pool, which `async_threads` sizes, while other threads parse what has arrived; with `async_threads` set to 0 the query's own threads read them. A `text`, `csv` or `binary` file whose name ends in `.gz` or `.zst` is decompressed as it is read and compressed as it is written.

In the `text` format a row ends at a newline or at a carriage return followed by a newline, as in PostgreSQL. A newline or carriage return preceded by a backslash is part of the value, not the end of the row.

In the `text` format a line holding only `\.` ends the data. Over STDIN anything after it is ignored. In a file only empty lines may follow it, so the data files of a `pg_dump` directory or tar archive load as they are; any other line after it fails the `COPY` with `end-of-copy marker is followed by data`. A `\.` elsewhere in a line fails with `end-of-copy marker is not alone on its line`. In the `csv` format `\.` is data.

### Errors in the Data

In the `text`, `csv` and `binary` formats, a value that does not convert to its column's type fails the `COPY` with the error of that type, and the context of the error names the table, the line, the column and the value. Lines are counted from the start of the data, the header line included, and in the `csv` format a line break inside a quoted value counts too. Given a file `readings.csv`:

```text
1,20.5
x,21.0
3,abc
4,19.75
```

<SqlLogicTest id="sql/statements/copy/index/value_error" />

The context of this error is `COPY readings, line 2, column sensor: "x"`, which `psql` prints after `CONTEXT:`. A row with more or fewer values than the table has columns fails with `extra data after last expected column` or `missing data for column`, and its context quotes the whole line. In the `binary` format the context counts rows instead of lines, and names the column without the value. When the data has several bad rows, the error is reported for the first of them, also when several threads parse the data.

In the `text` and `csv` formats, `ON_ERROR ignore` skips each row that has a value that does not convert, and loads the other rows:

<SqlLogicTest id="sql/statements/copy/index/on_error_ignore" />

A row with more or fewer values than the table has columns fails the `COPY` either way. When the `COPY` ends, a notice reports how many rows were skipped, here `2 rows were skipped due to data type incompatibility`. `LOG_VERBOSITY verbose` adds a notice for each skipped row, such as `skipping row due to data type incompatibility at line 2 for column "sensor": "x"`, and `LOG_VERBOSITY silent` sends no notice. `REJECT_LIMIT n` fails the `COPY` once more than `n` rows are skipped:

<SqlLogicTest id="sql/statements/copy/index/reject_limit" />

`REJECT_LIMIT` requires `ON_ERROR ignore`. Neither is allowed in `COPY ... TO` or in the `binary` format.

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

<DocCallout type="note">

To ensure compatibility with PostgreSQL, SereneDB accepts `COPY ... FROM` statements that do not fully comply with the railroad diagram shown here. For example, the following is a valid statement:

<SqlLogicTest id="sql/statements/copy/index/example_023" />

</DocCallout>

## `COPY ... TO`

`COPY ... TO` exports data from SereneDB to an external file in any of the formats above, or to the client with [`STDOUT`](#stdin-and-stdout). It has mostly the same set of options as `COPY ... FROM`, however, in the case of `COPY ... TO` the options specify how the file should be written to disk. Any file created by `COPY ... TO` can be copied back into the database by using `COPY ... FROM` with a similar set of options.

The `COPY ... TO` function can be called specifying either a table name, or a query. When a table name is specified, the contents of the entire table will be written into the resulting file. When a query is specified, the query is executed and the result of the query is written to the resulting file.

`COPY ... TO` writes the rows from several threads, so they reach the file in any order, unless the query has an `ORDER BY`, or the `PRESERVE_ORDER` option or the [`preserve_insertion_order`](../../../compatibility/order_preservation.md) setting is `true`. Then the file holds the rows in the order of the query.

Copy the contents of the `lineitem` table to a CSV file with a header:

<SqlLogicTest id="sql/statements/copy/index/example_024" />

Copy the contents of the `lineitem` table to the file `lineitem.tbl`, where the columns are delimited by a pipe character (`|`), including a header line:

<SqlLogicTest id="sql/statements/copy/index/example_025" />

Write the `lineitem` table to `lineitem.tsv` in the `text` format, with tab separators and no header:

<SqlLogicTest id="sql/statements/copy/index/example_026" />

With `HEADER`, a `text` file starts with a line of the column names as the query gives them, separated and escaped like the values.

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
| `FORMAT`                  | Specifies the copy function to use. The default is selected from the file extension (e.g., `.parquet` results in a Parquet file being written/read). If the file extension names no format, or the file has none, `TEXT` is selected. SereneDB provides the `TEXT`, `BINARY`, `CSV`, `PARQUET`, `JSON`, `ARROW`, `ARROWS` and `BLOB` copy functions. The name is case-insensitive, quoted or not: `FORMAT 'CSV'` is `FORMAT csv`. | `VARCHAR`             | `auto`  |
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

## `STDIN` and `STDOUT`

Over the PostgreSQL wire protocol, `COPY ... FROM STDIN` reads the data from the client and `COPY ... TO STDOUT` sends it to the client. Without `FORMAT`, both use the `text` format. The formats work in these directions:

| Format                  | `COPY ... FROM STDIN` | `COPY ... TO STDOUT` |
| :---------------------- | :-------------------- | :------------------- |
| `text`, `csv`, `binary` | yes                   | yes                  |
| `json`                  | yes                   | yes                  |
| `parquet`               | no                    | yes                  |
| `arrow`                 | no                    | yes                  |
| `arrows`                | yes                   | yes                  |

In the `json` format, `COPY ... TO STDOUT` sends one JSON object per line, as `COPY ... TO` writes it to a file. A Parquet file and an Arrow IPC file (`arrow`) reach the client whole, ready to be saved as a file. A Parquet file keeps the description of its contents in a footer at the end, which a reader needs before it can read any row, so it loads only from a file, not from `STDIN`:

<SqlLogicTest id="sql/statements/copy/index/stdin_parquet" />

`COPY ... FROM STDIN` refuses an Arrow IPC file too, with a hint to send an Arrow IPC stream, `FORMAT arrows`, instead. A stream has no footer and is read as it arrives, which makes it the format to send columnar data to SereneDB. With `psql`:

```sh
psql -c "COPY lineitem TO STDOUT (FORMAT parquet)" > lineitem.parquet
psql -c "COPY lineitem TO STDOUT (FORMAT arrows)" > lineitem.arrows
psql -c "COPY lineitem FROM STDIN (FORMAT arrows)" < lineitem.arrows
```

## `text`, `csv` and `binary` Options

The `text`, `csv` and `binary` formats take the options below, which include those of PostgreSQL's `COPY`. The `binary` format takes only `FREEZE` and `LOG_VERBOSITY`.

| Name                        | Formats                 | Description                                                                                                                                                                                                                                                                                                                    | Default                                          |
| :-------------------------- | :---------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------- |
| `HEADER`                    | `text`, `csv`           | `COPY ... TO` writes the column names as the first line. `COPY ... FROM` skips the first line, and with `MATCH` also checks the names in it; see [Header Line](#header-line). Without `HEADER`, `COPY ... TO` in the `csv` format writes no header on a server, following the `copy_csv_header_default` setting, and writes one in `serenedb shell`. | `false`                                          |
| `DELIMITER`, `DELIM`, `SEP` | `text`, `csv`           | The one-byte character between two values of a row; see [Delimiter](#delimiter).                                                                                                                                                                                                                                              | a tab in `text`, `,` in `csv`                    |
| `NULL`, `NULLSTR`           | `text`, `csv`           | The string that stands for `NULL`; see [`NULL` Values](#null-values).                                                                                                                                                                                                                                                          | `\N` in `text`, an unquoted empty value in `csv` |
| `DEFAULT`                   | `text`, `csv`           | `COPY ... FROM` only: the string that stands for the default value of its column; see [Default Values](#default-values).                                                                                                                                                                                                       | none                                             |
| `QUOTE`                     | `csv`                   | The one-byte character that quotes a value.                                                                                                                                                                                                                                                                                    | `"`                                              |
| `ESCAPE`                    | `csv`                   | The one-byte character before a `QUOTE` character inside a quoted value.                                                                                                                                                                                                                                                       | the `QUOTE` character                            |
| `FORCE_QUOTE`               | `csv`                   | `COPY ... TO` only: a list of columns, or `*` for all, whose values other than `NULL` are always quoted.                                                                                                                                                                                                                      | none                                             |
| `FORCE_NOT_NULL`            | `csv`                   | `COPY ... FROM` only: a list of columns, or `*` for all, whose values are never `NULL`: a value that matches the `NULL` string is read as that string.                                                                                                                                                                         | none                                             |
| `FORCE_NULL`                | `csv`                   | `COPY ... FROM` only: a list of columns, or `*` for all, in which a value that matches the `NULL` string is `NULL` even when it is quoted.                                                                                                                                                                                    | none                                             |
| `ON_ERROR`                  | `text`, `csv`           | `COPY ... FROM` only: `stop` fails the `COPY` at a value that does not convert to its column's type, `ignore` skips the row; see [Errors in the Data](#errors-in-the-data).                                                                                                                                                   | `stop`                                           |
| `REJECT_LIMIT`              | `text`, `csv`           | `COPY ... FROM` with `ON_ERROR ignore` only: the number of skipped rows above which the `COPY` fails.                                                                                                                                                                                                                         | none                                             |
| `LOG_VERBOSITY`             | `text`, `csv`, `binary` | The notices about skipped rows: `default` reports how many rows were skipped, `verbose` also reports each of them, `silent` reports nothing.                                                                                                                                                                                  | `default`                                        |
| `ENCODING`                  | `text`, `csv`           | The character encoding of the data; see [Encoding](#encoding).                                                                                                                                                                                                                                                                 | `UTF8`                                           |
| `RENDERING`                 | `text`, `csv`           | The text form of the values: `postgres` reads and writes them with PostgreSQL's input and output functions, `duckdb` with DuckDB's casts from and to `VARCHAR` (a list is `[1, 2]` instead of `{1,2}`, a boolean `true` instead of `t`); see [Rendering](#rendering).                                                          | `postgres`                                       |
| `COMPRESSION`               | `text`, `csv`           | `none`, `gzip` or `zstd`.                                                                                                                                                                                                                                                                                                      | from the file name                               |
| `FREEZE`                    | `text`, `csv`, `binary` | `COPY ... FROM` only: accepted, and has no effect.                                                                                                                                                                                                                                                                             | `false`                                          |

Each option that takes a column list, as `FORCE_NULL (body)`, fails the `COPY` when it names a column the `COPY` does not have. An option given in the wrong direction fails the `COPY` too, as in `COPY FORCE_QUOTE cannot be used with COPY FROM`.

### Header Line

`HEADER` makes `COPY ... TO` write the column names as the first line, and `COPY ... FROM` skip the first line. `HEADER MATCH` also checks that the first line names the columns of the `COPY`, in order, and fails at the first difference:

<SqlLogicTest id="sql/statements/copy/index/header_match" />

<SqlLogicTest id="sql/statements/copy/index/header_match_error" />

### Delimiter

In the `text` format `DELIMITER` is one character other than a backslash, a period, a lowercase letter, a digit, a newline or a carriage return, and it must not appear in the `NULL` string. `COPY ... TO` writes a backslash before every character of a value that equals the delimiter, whatever the column's type, so the file reads back with the same `DELIMITER`.

In the `csv` format `DELIMITER` and `QUOTE` must differ, and neither may appear in the `NULL` string. `COPY ... TO` quotes a value that holds the delimiter, the quote character or a line break, and an empty string, which would otherwise read back as `NULL`.

### `NULL` Values

In the `text` format `\N` stands for `NULL`. In the `csv` format an unquoted empty value is `NULL`, and a quoted empty value `""` is an empty string. The `NULL` option names another string. In the `csv` format a quoted value never matches that string, except in the columns listed in `FORCE_NULL`, and no value matches it in the columns listed in `FORCE_NOT_NULL`. Given a file `notes.csv`:

```text
1,,
2,"",
3,N,"N"
```

<SqlLogicTest id="sql/statements/copy/index/csv_nulls" />

With `N` for `NULL`, `FORCE_NOT_NULL` for the column `title` and `FORCE_NULL` for the column `body`:

<SqlLogicTest id="sql/statements/copy/index/csv_force_null" />

### Default Values

`DEFAULT` names a string that, in place of a value, stores the default value of its column. In the `csv` format a quoted value does not match it. The string must differ from the `NULL` string, and a match in a column without a default fails the `COPY` with `unexpected default marker in COPY data`. Given a file `orders.csv`:

```text
1,shipped,5
2,default,default
3,"default",2
```

<SqlLogicTest id="sql/statements/copy/index/default_marker" />

### Encoding

`ENCODING` names the character encoding of the data: `COPY ... FROM` converts the data from it, and `COPY ... TO` converts the data to it. The encodings are `UTF8`, `LATIN1` and the Unicode encodings `UTF16BE`, `UTF16LE`, `UTF32BE` and `UTF32LE`, where `UTF16` and `UTF32` are little-endian. A name is case-insensitive and may contain `-` and `_`, as in `UTF-8`, and `ISO_8859_1` is another name of `LATIN1`:

<SqlLogicTest id="sql/statements/copy/index/encoding" />

The other encodings of PostgreSQL are not supported:

<SqlLogicTest id="sql/statements/copy/index/encoding_unsupported" />

Without `ENCODING`, `COPY ... FROM` reads UTF-8 and fails at a byte sequence that is not valid UTF-8, with `invalid byte sequence for encoding "UTF8"`. `COPY ... TO` fails at a character that its `ENCODING` cannot represent, as in `character with byte sequence 0xe2 0x82 0xac in encoding "UTF8" has no equivalent in encoding "LATIN1"` for the character `€`.

### Rendering

`RENDERING` picks the text form of the values. With `postgres`, the default, `COPY` reads and writes them as PostgreSQL does: an array is `{1,2}`, a boolean `t` or `f`. With `duckdb` it reads and writes them as DuckDB's casts from and to `VARCHAR` do: a list is `[1, 2]`, a boolean `true` or `false`. The quoting, escaping and `NULL` string stay those of the format:

<SqlLogicTest id="sql/statements/copy/index/rendering" />

`read_csv` and `read_json` take a `rendering` parameter too, `duckdb` by default; with `rendering = 'postgres'` they read values with PostgreSQL's input functions. The `sdb_rendering` [setting](../../../configuration/overview.md) picks the text form of query results.

## `COPY FROM DATABASE ... TO`

The `COPY FROM DATABASE ... TO` statement copies the entire content from one attached database to another attached database. This includes the schema, including constraints, indexes, sequences, macros and the data itself. [Jobs](../create_job/index.md) are not copied.

Copied objects keep their owners, and a sequence owned by a column, such as the one behind a `SERIAL` column, stays owned by that column in the copy.

<SqlLogicTest id="sql/statements/copy/copy_database_full/example_037" />

To only copy the **schema** of `db1` to `db2` but omit copying the data, add `SCHEMA` to the statement:

<SqlLogicTest id="sql/statements/copy/copy_database_schema/example_038" />

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram3" />

## Format-Specific Options

### CSV Options

Besides the [`text`, `csv` and `binary` options](#text-csv-and-binary-options), `COPY ... TO` in the `csv` format takes these options.

| Name              | Description                                                                                                                                               | Type      | Default |
| :---------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :------ |
| `DATEFORMAT`      | Specifies the date format to use when writing dates. See [Date Format](../../functions/dateformat.md).                                                    | `VARCHAR` | (empty) |
| `PREFIX`          | Prefixes the CSV file with a specified string. This option must be used in conjunction with `SUFFIX` and requires `HEADER` to be set to `false`.          | `VARCHAR` | (empty) |
| `SUFFIX`          | Appends a specified string as a suffix to the CSV file. This option must be used in conjunction with `PREFIX` and requires `HEADER` to be set to `false`. | `VARCHAR` | (empty) |
| `TIMESTAMPFORMAT` | Specifies the date format to use when writing timestamps. See [Date Format](../../functions/dateformat.md).                                               | `VARCHAR` | (empty) |

`COPY ... FROM` in the `csv` format with an option that only the [`read_csv` function](../../../data_import_and_export/csv/overview.md#parameters) has, such as `AUTO_DETECT` or `DATEFORMAT`, reads the file with the options of `read_csv` instead.

### Parquet Options

The below options are applicable when writing Parquet files.

| Name                   | Description                                                                                                                                                                                                                                                                                                                                                                                               | Type      | Default                 |
| :--------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :---------------------- |
| `COMPRESSION`          | The compression format to use (`uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, `lz4_raw`).                                                                                                                                                                                                                                                                                                     | `VARCHAR` | `snappy`                |
| `COMPRESSION_LEVEL`    | Compression level, set between 1 (lowest compression, fastest) and 22 (highest compression, slowest). Only supported for zstd compression.                                                                                                                                                                                                                                                                | `BIGINT`  | `3`                     |
| `FIELD_IDS`            | The `field_id` for each column. Pass `auto` to attempt to infer automatically.                                                                                                                                                                                                                                                                                                                            | `STRUCT`  | (empty)                 |
| `ROW_GROUP_SIZE_BYTES` | The target size of each row group. You can pass either a human-readable string, e.g., `2MB`, or an integer, i.e., the number of bytes. This option is only used when you have issued `SET preserve_insertion_order = false;`, otherwise, it is ignored.                                                                                                                                                   | `BIGINT`  | `row_group_size * 1024` |
| `ROW_GROUP_SIZE`       | The target size, i.e., number of rows, of each row group.                                                                                                                                                                                                                                                                                                                                                 | `BIGINT`  | 122880                  |
| `ROW_GROUPS_PER_FILE`  | Create a new Parquet file if the current one has a specified number of row groups. If multiple threads are active, the number of row groups in a file may slightly exceed the specified number of row groups to limit the amount of locking, similarly to the behavior of `FILE_SIZE_BYTES`. However, if `per_thread_output` is set, only one thread writes to each file, and it becomes accurate again. | `BIGINT`  | (empty)                 |
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

A column of a PostgreSQL type that Parquet has no type for, such as `macaddr` or `int4range`, is written as its text; see [PostgreSQL Types](../../../data_import_and_export/parquet/overview.md#postgresql-types).

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

`COPY ... FROM` in the `json` format accepts `NaN`, `Infinity` and trailing commas in the data, and refuses them with `STRICT true`.

### Arrow Options

The `arrow` format writes an Arrow IPC file, which ends with a footer through which a reader can go to any record batch. The `arrows` format writes an Arrow IPC stream, which has no footer and is read from start to end, so it can be sent through a pipe or over `STDIN`. `COPY ... FROM` and `read_arrow` read both. Their options, such as `COMPRESSION` and `ROW_GROUP_SIZE`, are listed under [Writing Arrow Files](../../../data_import_and_export/arrow/overview.md#writing-arrow-files).

### BLOB Options

The `BLOB` format option allows you to select a single column of a SereneDB table into a `.blob` file. The column must be cast to the `BLOB` data type. For details on typecasting, see the [Casting Operations Matrix](../../data_types/typecasting.md#casting-operations-matrix).

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
