---
title: Reading Faulty CSV Files
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

CSV files can come in all shapes and forms, with some presenting many errors that make the process of cleanly reading them inherently difficult. To help users read these files, SereneDB supports detailed error messages, the ability to skip faulty lines and the possibility of storing faulty lines in a temporary table to assist users with a data cleaning step.

## Structural Errors

SereneDB supports the detection and skipping of several different structural errors. In this section, we will go over each error with an example.
For the examples, consider the following table:

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_001" />

SereneDB detects the following error types:

-   `CAST`: Casting errors occur when a column in the CSV file cannot be cast to the expected schema value. For example, the line `Pedro,The 90s` would cause an error since the string `The 90s` cannot be cast to a date.
-   `MISSING COLUMNS`: This error occurs if a line in the CSV file has fewer columns than expected. In our example, we expect two columns; therefore, a row with just one value, e.g., `Pedro`, would cause this error.
-   `TOO MANY COLUMNS`: This error occurs if a line in the CSV has more columns than expected. In our example, any line with more than two columns would cause this error, e.g., `Pedro,01-01-1992,pdet`.
-   `UNQUOTED VALUE`: Quoted values in CSV lines must always be unquoted at the end; if a quoted value remains quoted throughout, it will cause an error. For example, assuming our scanner uses `quote='"'`, the line `"pedro"holanda, 01-01-1992` would present an unquoted value error.
-   `LINE SIZE OVER MAXIMUM`: SereneDB has a parameter that sets the maximum line size a CSV file can have, which by default is set to 2,097,152 bytes. Assuming our scanner is set to `max_line_size = 25`, the line `Pedro Holanda, 01-01-1992` would produce an error, as it exceeds 25 bytes.
-   `INVALID ENCODING`: SereneDB supports UTF-8 strings, UTF-16 and Latin-1 encodings. Lines containing other characters will produce an error. For example, the line `pedro\xff\xff, 01-01-1992` would be problematic.

### Anatomy of a CSV Error

By default, when performing a CSV read, if any structural errors are encountered, the scanner will immediately stop the scanning process and throw the error to the user.
These errors are designed to provide as much information as possible to allow users to evaluate them directly in their CSV file.

For example, reading a file where a value cannot be cast to the expected column type throws a conversion error:

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_008" />

The error reports where the failure occurred — the line number and the original CSV line — together with the column being converted and why its value could not be cast. The full message also lists the column's resolved type and the scanner options in effect (and whether each was auto-detected or set manually), so you can evaluate the error directly against your file. When the type was auto-detected, common remedies are to override it explicitly (e.g., `columns = {'birth_date': 'VARCHAR'}`) or to increase `sample_size` so the sniffer scans more rows.

## Using the `ignore_errors` Option

There are cases where CSV files may have multiple structural errors, and users simply wish to skip these and read the correct data. Reading erroneous CSV files is possible by utilizing the `ignore_errors` option. With this option set, rows containing data that would otherwise cause the CSV parser to generate an error will be ignored. In our example, we will demonstrate a CAST error, but note that any of the errors described in our Structural Error section would cause the faulty line to be skipped.

For example, consider the following CSV file, <a href="/files/docs/faulty.csv" download>`faulty.csv`</a>:

```csv
Pedro,31
Oogie Boogie, three
```

If you read the CSV file, specifying that the first column is a `VARCHAR` and the second column is an `INTEGER`, loading the file would fail, as the string `three` cannot be converted to an `INTEGER`.

For example, the following query will throw a casting error.

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_002" />

However, with `ignore_errors` set, the second row of the file is skipped, outputting only the complete first row. For example:

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_003" />

One should note that the CSV Parser is affected by the projection pushdown optimization. Hence, if we were to select only the name column, both rows would be considered valid, as the casting error on the age would never occur. For example:

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_004" />

## Retrieving Faulty CSV Lines

Being able to read faulty CSV files is important, but for many data cleaning operations, it is also necessary to know exactly which lines are corrupted and what errors the parser discovered on them. For scenarios like these, it is possible to use SereneDB's CSV Rejects Table feature.
By default, this feature creates two temporary tables.

1. `reject_scans`: Stores information regarding the parameters of the CSV Scanner.
2. `reject_errors`: Stores information regarding each CSV faulty line and in which CSV Scanner they happened.

Note that any of the errors described in our Structural Error section will be stored in the rejects tables. Also, if a line has multiple errors, multiple entries will be stored for the same line, one for each error.

### Reject Scans

The CSV Reject Scans Table returns the following information:

| Column name         | Description                                                                                      | Type       |
| :------------------ | :----------------------------------------------------------------------------------------------- | :--------- |
| `scan_id`           | The internal ID used in SereneDB to represent that scanner                                       | `UBIGINT`  |
| `file_id`           | A scanner might happen over multiple files, so the file_id represents a unique file in a scanner | `UBIGINT`  |
| `file_path`         | The file path                                                                                    | `VARCHAR`  |
| `delimiter`         | The delimiter used e.g., ;                                                                       | `VARCHAR`  |
| `quote`             | The quote used e.g., "                                                                           | `VARCHAR`  |
| `escape`            | The escape used e.g., "                                                                          | `VARCHAR`  |
| `newline_delimiter` | The newline delimiter used e.g., \r\n                                                            | `VARCHAR`  |
| `skip_rows`         | If any rows were skipped from the top of the file                                                | `UINTEGER` |
| `has_header`        | If the file has a header                                                                         | `BOOLEAN`  |
| `columns`           | The schema of the file (i.e., all column names and types)                                        | `VARCHAR`  |
| `date_format`       | The format used for date types                                                                   | `VARCHAR`  |
| `timestamp_format`  | The format used for timestamp types                                                              | `VARCHAR`  |
| `user_arguments`    | Any extra scanner parameters manually set by the user                                            | `VARCHAR`  |

### Reject Errors

The CSV Reject Errors Table returns the following information:

| Column name          | Description                                                                                       | Type      |
| :------------------- | :------------------------------------------------------------------------------------------------ | :-------- |
| `scan_id`            | The internal ID used in SereneDB to represent that scanner, used to join with reject scans tables | `UBIGINT` |
| `file_id`            | The file_id represents a unique file in a scanner, used to join with reject scans tables          | `UBIGINT` |
| `line`               | Line number, from the CSV File, where the error occurred.                                         | `UBIGINT` |
| `line_byte_position` | Byte Position of the start of the line, where the error occurred.                                 | `UBIGINT` |
| `byte_position`      | Byte Position where the error occurred.                                                           | `UBIGINT` |
| `column_idx`         | If the error happens in a specific column, the index of the column.                               | `UBIGINT` |
| `column_name`        | If the error happens in a specific column, the name of the column.                                | `VARCHAR` |
| `error_type`         | The type of the error that happened.                                                              | `ENUM`    |
| `csv_line`           | The original CSV line.                                                                            | `VARCHAR` |
| `error_message`      | The error message produced by SereneDB.                                                           | `VARCHAR` |

## Parameters

The parameters listed below are used in the `read_csv` function to configure the CSV Rejects Table.

| Name            | Description                                                                                                                                        | Type      | Default       |
| :-------------- | :------------------------------------------------------------------------------------------------------------------------------------------------- | :-------- | :------------ |
| `store_rejects` | If set to true, any errors in the file will be skipped and stored in the default rejects temporary tables.                                         | `BOOLEAN` | False         |
| `rejects_scan`  | Name of a temporary table where the information of the scan information of faulty CSV file are stored.                                             | `VARCHAR` | reject_scans  |
| `rejects_table` | Name of a temporary table where the information of the faulty lines of a CSV file are stored.                                                      | `VARCHAR` | reject_errors |
| `rejects_limit` | Upper limit on the number of faulty records from a CSV file that will be recorded in the rejects table. 0 is used when no limit should be applied. | `BIGINT`  | 0             |

To store the information of the faulty CSV lines in a rejects table, the user must simply set the `store_rejects` option to true. For example:

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_005" />

You can then query both the `reject_scans` and `reject_errors` tables, to retrieve information about the rejected tuples. For example:

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_006" />

<SqlLogicTest id="data_import_and_export/csv/reading_faulty_csv_files/example_007" />
