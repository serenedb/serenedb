---
title: Overview
sidebar_position: 1
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## Examples

The following examples use the <a href="/files/docs/flights.csv" download>`flights.csv`</a> file.

Read a CSV file from disk, auto-infer options:

<SqlLogicTest id="data_import_and_export/csv/overview/example_001" />

Use the `read_csv` function with custom options:

<SqlLogicTest id="data_import_and_export/csv/overview/example_002" />

Read a CSV from stdin, auto-infer options:

```batch
cat flights.csv | serened shell -c "SELECT * FROM read_csv('/dev/stdin')"
```

Read a CSV file into a table:

<SqlLogicTest id="data_import_and_export/csv/overview/example_003" />

Alternatively, create a table without specifying the schema manually using a [`CREATE TABLE ... AS SELECT` statement](../../sql/statements/create_table/index.md#create-table--as-select-ctas):

<SqlLogicTest id="data_import_and_export/csv/overview/example_004" />

We can use the [`FROM`-first syntax](../../sql/query_syntax/from_and_join/index.md#from-first-syntax) to omit `SELECT *`.

<SqlLogicTest id="data_import_and_export/csv/overview/example_005" />

## CSV Loading

CSV loading, i.e., importing CSV files to the database, is a very common, and yet surprisingly tricky, task. While CSVs seem simple on the surface, there are a lot of inconsistencies found within CSV files that can make loading them a challenge. CSV files come in many different varieties, are often corrupt, and do not have a schema. The CSV reader needs to cope with all of these different situations.

The SereneDB CSV reader can automatically infer which configuration flags to use by analyzing the CSV file using the CSV sniffer. This will work correctly in most situations, and should be the first option attempted. In rare situations where the CSV reader cannot figure out the correct configuration it is possible to manually configure the CSV reader to correctly parse the CSV file. See the [auto detection page](../../data_import_and_export/csv/auto_detection.md) for more information.

## Parameters

Below are parameters that can be passed to the [`read_csv` function](#csv-functions). Where meaningfully applicable, these parameters can also be passed to the [`COPY` statement](../../sql/statements/copy/index.md#copy--to).

| Name                                                                           | Description                                                                                                                                                                                                                                                                                                                                                     | Type                     | Default                                        |
| :----------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------- | :--------------------------------------------- |
| `all_varchar`                                                                  | Skip type detection and assume all columns are of type `VARCHAR`. This option is only supported by the `read_csv` function.                                                                                                                                                                                                                                     | `BOOL`                   | `false`                                        |
| `allow_quoted_nulls`                                                           | Allow the conversion of quoted values to `NULL` values                                                                                                                                                                                                                                                                                                          | `BOOL`                   | `true`                                         |
| `auto_detect`                                                                  | [Auto detect CSV parameters](../../data_import_and_export/csv/auto_detection.md).                                                                                                                                                                                                                                                                               | `BOOL`                   | `true`                                         |
| `auto_type_candidates`                                                         | Types that the sniffer uses when detecting column types. The `VARCHAR` type is always included as a fallback option. See [example](#auto_type_candidates-details).                                                                                                                                                                                              | `TYPE[]`                 | [default types](#auto_type_candidates-details) |
| `buffer_size`                                                                  | Size of the buffers used to read files, in bytes. Must be large enough to hold four lines and can significantly impact performance.                                                                                                                                                                                                                             | `BIGINT`                 | `16 * max_line_size`                           |
| `columns`                                                                      | Column names and types, as a struct (e.g., `{'col1': 'INTEGER', 'col2': 'VARCHAR'}`). Using this option disables auto detection of the schema.                                                                                                                                                                                                                  | `STRUCT`                 | (empty)                                        |
| `comment`                                                                      | Character used to initiate comments. Lines starting with a comment character (optionally preceded by space characters) are completely ignored; other lines containing a comment character are parsed only up to that point.                                                                                                                                     | `VARCHAR`                | (empty)                                        |
| `compression`                                                                  | Method used to compress CSV files. By default this is detected automatically from the file extension (e.g., `t.csv.gz` will use gzip, `t.csv` will use `none`). Options are `none`, `gzip`, `zstd`.                                                                                                                                                             | `VARCHAR`                | `auto`                                         |
| `dateformat`                                                                   | [Date format](../../sql/functions/dateformat.md) used when parsing and writing dates.                                                                                                                                                                                                                                                                           | `VARCHAR`                | (empty)                                        |
| `date_format`                                                                  | Alias for `dateformat`; only available in the `COPY` statement.                                                                                                                                                                                                                                                                                                 | `VARCHAR`                | (empty)                                        |
| `decimal_separator`                                                            | Decimal separator for numbers.                                                                                                                                                                                                                                                                                                                                  | `VARCHAR`                | `.`                                            |
| `delim`                                                                        | Delimiter character used to separate columns within each line, e.g., `,` `;` `\t`. The delimiter character can be up to 4 bytes, e.g., 🌊. Alias for `sep`.                                                                                                                                                                                                     | `VARCHAR`                | `,`                                            |
| `delimiter`                                                                    | Alias for `delim`; only available in the `COPY` statement.                                                                                                                                                                                                                                                                                                      | `VARCHAR`                | `,`                                            |
| `escape`                                                                       | String used to escape the `quote` character within quoted values.                                                                                                                                                                                                                                                                                               | `VARCHAR`                | `"`                                            |
| `encoding`                                                                     | Encoding used by the CSV file. Options are `utf-8`, `utf-16`, `latin-1`. Not available in the `COPY` statement (which always uses `utf-8`).                                                                                                                                                                                                                     | `VARCHAR`                | `utf-8`                                        |
| `filename`                                                                     | Add path of the containing file to each row, as a string column named `filename`. Relative or absolute paths are returned depending on the path or glob pattern provided to `read_csv`, not just filenames. `filename` column is added automatically as a virtual column and this option is only kept for compatibility reasons.                                | `BOOL`                   | `false`                                        |
| `force_not_null`                                                               | Do not match values in the specified columns against the `NULL` string. In the default case where the `NULL` string is empty, this means that empty values are read as zero-length strings instead of `NULL`s.                                                                                                                                                  | `VARCHAR[]`              | `[]`                                           |
| `header`                                                                       | First line of each file contains the column names.                                                                                                                                                                                                                                                                                                              | `BOOL`                   | `false`                                        |
| `hive_partitioning`                                                            | Interpret the path as a [Hive partitioned path](../../data_import_and_export/partitioning/hive_partitioning.md).                                                                                                                                                                                                                                                | `BOOL`                   | (auto-detected)                                |
| `ignore_errors`                                                                | Ignore any parsing errors encountered.                                                                                                                                                                                                                                                                                                                          | `BOOL`                   | `false`                                        |
| `max_line_size` or `maximum_line_size`. Not available in the `COPY` statement. | Maximum line size, in bytes.                                                                                                                                                                                                                                                                                                                                    | `BIGINT`                 | 2000000                                        |
| `names` or `column_names`                                                      | Column names, as a list. See [example](../../data_import_and_export/csv/tips.md#provide-names-if-the-file-does-not-contain-a-header).                                                                                                                                                                                                                           | `VARCHAR[]`              | (empty)                                        |
| `new_line`                                                                     | New line character(s). Options are `'\r'`,`'\n'`, or `'\r\n'`. The CSV parser only distinguishes between single-character and double-character line delimiters. Therefore, it does not differentiate between `'\r'` and `'\n'`.                                                                                                                                 | `VARCHAR`                | (empty)                                        |
| `normalize_names`                                                              | Normalize column names. This removes any non-alphanumeric characters from them. Column names that are reserved SQL keywords are prefixed with an underscore character (`_`).                                                                                                                                                                                    | `BOOL`                   | `false`                                        |
| `null_padding`                                                                 | Pad the remaining columns on the right with `NULL` values when a line lacks columns.                                                                                                                                                                                                                                                                            | `BOOL`                   | `false`                                        |
| `nullstr` or `null`                                                            | Strings that represent a `NULL` value.                                                                                                                                                                                                                                                                                                                          | `VARCHAR` or `VARCHAR[]` | (empty)                                        |
| `parallel`                                                                     | Use the parallel CSV reader.                                                                                                                                                                                                                                                                                                                                    | `BOOL`                   | `true`                                         |
| `quote`                                                                        | String used to quote values.                                                                                                                                                                                                                                                                                                                                    | `VARCHAR`                | `"`                                            |
| `rejects_scan`                                                                 | Name of the [temporary table where information on faulty scans is stored](../../data_import_and_export/csv/reading_faulty_csv_files.md#reject-scans).                                                                                                                                                                                                           | `VARCHAR`                | `reject_scans`                                 |
| `rejects_table`                                                                | Name of the [temporary table where information on faulty lines is stored](../../data_import_and_export/csv/reading_faulty_csv_files.md#reject-errors).                                                                                                                                                                                                          | `VARCHAR`                | `reject_errors`                                |
| `rejects_limit`                                                                | Upper limit on the number of faulty lines per file that are recorded in the rejects table. Setting this to `0` means that no limit is applied.                                                                                                                                                                                                                  | `BIGINT`                 | `0`                                            |
| `sample_size`                                                                  | Number of sample lines for [auto detection of parameters](../../data_import_and_export/csv/auto_detection.md).                                                                                                                                                                                                                                                  | `BIGINT`                 | 20480                                          |
| `sep`                                                                          | Delimiter character used to separate columns within each line, e.g., `,` `;` `\t`. The delimiter character can be up to 4 bytes, e.g., 🌊. Alias for `delim`.                                                                                                                                                                                                   | `VARCHAR`                | `,`                                            |
| `skip`                                                                         | Number of lines to skip at the start of each file.                                                                                                                                                                                                                                                                                                              | `BIGINT`                 | 0                                              |
| `store_rejects`                                                                | Skip any lines with errors and store them in the rejects table.                                                                                                                                                                                                                                                                                                 | `BOOL`                   | `false`                                        |
| `strict_mode`                                                                  | Enforces the strictness level of the CSV Reader. When set to `true`, the parser will throw an error upon encountering any issues. When set to `false`, the parser will attempt to read structurally incorrect files. It is important to note that reading structurally incorrect files can cause ambiguity; therefore, this option should be used with caution. | `BOOL`                   | `true`                                         |
| `thousands`                                                                    | Character used to identify thousands separators in numeric values. It must be a single character and different from the `decimal_separator` option.                                                                                                                                                                                                             | `VARCHAR`                | (empty)                                        |
| `timestampformat`                                                              | [Timestamp format](../../sql/functions/dateformat.md) used when parsing and writing timestamps.                                                                                                                                                                                                                                                                 | `VARCHAR`                | (empty)                                        |
| `timestamp_format`                                                             | Alias for `timestampformat`; only available in the `COPY` statement.                                                                                                                                                                                                                                                                                            | `VARCHAR`                | (empty)                                        |
| `types` or `dtypes` or `column_types`                                          | Column types, as either a list (by position) or a struct (by name). See [example](../../data_import_and_export/csv/tips.md#override-the-types-of-specific-columns).                                                                                                                                                                                             | `VARCHAR[]` or `STRUCT`  | (empty)                                        |
| `union_by_name`                                                                | Align columns from different files [by column name](../../data_import_and_export/multiple_files/combining_schemas.md#union-by-name) instead of position. Using this option increases memory consumption.                                                                                                                                                        | `BOOL`                   | `false`                                        |

<DocCallout type="tip">

SereneDB's CSV reader supports `UTF-8` (default), `UTF-16` and `Latin-1` encodings.
For other encodings, convert them e.g. using the [`iconv` command-line tool](https://linux.die.net/man/1/iconv):

```batch
iconv -f ISO-8859-2 -t UTF-8 input.csv > input-utf-8.csv
```

</DocCallout>

### `auto_type_candidates` Details

The `auto_type_candidates` option lets you specify the data types that should be considered by the CSV reader for [column data type detection](../../data_import_and_export/csv/auto_detection.md#type-detection).
Usage example:

<SqlLogicTest id="data_import_and_export/csv/overview/example_006" />

The default value for the `auto_type_candidates` option is `['NULL', 'BOOLEAN', 'BIGINT', 'DOUBLE', 'TIME', 'DATE', 'TIMESTAMP', 'VARCHAR']`.

## CSV Functions

The `read_csv` automatically attempts to figure out the correct configuration of the CSV reader using the CSV sniffer. It also automatically deduces types of columns. If the CSV file has a header, it will use the names found in that header to name the columns. Otherwise, the columns will be named `column0, column1, column2, ...`. An example with the <a href="/files/docs/flights.csv" download>`flights.csv`</a> file:

<SqlLogicTest id="data_import_and_export/csv/overview/example_007" />

The path can either be a relative path (relative to the current working directory) or an absolute path.

We can use `read_csv` to create a persistent table as well:

<SqlLogicTest id="data_import_and_export/csv/overview/example_008" />

<SqlLogicTest id="data_import_and_export/csv/overview/example_009" />

If we set `delim` / `sep`, `quote`, `escape`, or `header` explicitly, we can bypass the automatic detection of this particular parameter:

<SqlLogicTest id="data_import_and_export/csv/overview/example_010" />

Multiple files can be read at once by providing a glob or a list of files. Refer to the [multiple files section](../../data_import_and_export/multiple_files/overview.md) for more information.

## Writing Using the `COPY` Statement

The [`COPY` statement](../../sql/statements/copy/index.md#copy--to) can be used to load data from a CSV file into a table. This statement has the same syntax as the one used in PostgreSQL. To load the data using the `COPY` statement, we must first create a table with the correct schema (which matches the order of the columns in the CSV file and uses types that fit the values in the CSV file). `COPY` detects the CSV's configuration options automatically.

<SqlLogicTest id="data_import_and_export/csv/overview/example_011" />

If we want to manually specify the CSV format, we can do so using the configuration options of `COPY`.

<SqlLogicTest id="data_import_and_export/csv/overview/example_012" />

## Reading Faulty CSV Files

SereneDB supports reading erroneous CSV files. For details, see the [Reading Faulty CSV Files page](../../data_import_and_export/csv/reading_faulty_csv_files.md).

## Order Preservation

The CSV reader respects the `preserve_insertion_order` [configuration option](../../configuration/overview.md) to [preserve insertion order](../../compatibility/order_preservation.md).
When `true` (the default), the order of the rows in the result set returned by the CSV reader is the same as the order of the corresponding lines read from the file(s).
When `false`, there is no guarantee that the order is preserved.

## Writing CSV Files

SereneDB can write CSV files using the [`COPY ... TO` statement](../../sql/statements/copy/index.md#copy--to).
