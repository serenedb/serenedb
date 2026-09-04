---
title: Auto Detection
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

When using `read_csv`, the system tries to automatically infer how to read the CSV file using the CSV sniffer.
This step is necessary because CSV files are not self-describing and come in many different dialects. The auto-detection works roughly as follows:

-   Detect the dialect of the CSV file (delimiter, quoting rule, escape).
-   Detect the types of each of the columns.
-   Detect whether or not the file has a header row.

By default the system will try to auto-detect all options. However, options can be individually overridden by the user. This can be useful in case the system makes a mistake. For example, if the delimiter is chosen incorrectly, we can override it by calling the `read_csv` with an explicit delimiter (e.g., `read_csv('file.csv', delim = '|')`).

## Sample Size

The type detection works by operating on a sample of the file.
The size of the sample can be modified by setting the `sample_size` parameter.
The default sample size is 20,480 rows.
Setting the `sample_size` parameter to `-1` means the entire file is read for sampling:

<SqlLogicTest id="data_import_and_export/csv/auto_detection/example_001" />

The way sampling is performed depends on the type of file. If we are reading from a regular file on disk, we will jump into the file and try to sample from different locations in the file.
If we are reading from a file in which we cannot jump – such as a `.gz` compressed CSV file or `stdin` – samples are taken only from the beginning of the file.

## `sniff_csv` Function

It is possible to run the CSV sniffer as a separate step using the `sniff_csv(filename)` function, which returns the detected CSV properties as a table with a single row.
The `sniff_csv` function accepts an optional `sample_size` parameter to configure the number of rows sampled.

<SqlLogicTest id="data_import_and_export/csv/auto_detection/example_002" />

| Column name        | Description                                   | Example                                                           |
| ------------------ | --------------------------------------------- | ----------------------------------------------------------------- |
| `Delimiter`        | Delimiter                                     | `,`                                                               |
| `Quote`            | Quote character                               | `"`                                                               |
| `Escape`           | Escape                                        | `\`                                                               |
| `NewLineDelimiter` | New-line delimiter                            | `\r\n`                                                            |
| `Comment`          | Comment character                             | `#`                                                               |
| `SkipRows`         | Number of rows skipped                        | 1                                                                 |
| `HasHeader`        | Whether the CSV has a header                  | `true`                                                            |
| `Columns`          | Column types encoded as a `LIST` of `STRUCT`s | `({'name': 'VARCHAR', 'age': 'BIGINT'})`                          |
| `DateFormat`       | Date format                                   | `%d/%m/%Y`                                                        |
| `TimestampFormat`  | Timestamp Format                              | `%Y-%m-%dT%H:%M:%S.%f`                                            |
| `UserArguments`    | Arguments used to invoke `sniff_csv`          | `sample_size = 1000`                                              |
| `Prompt`           | Prompt ready to be used to read the CSV       | `FROM read_csv('my_file.csv', auto_detect=false, delim=',', ...)` |

### Prompt

The `Prompt` column contains a SQL command with the configurations detected by the sniffer.

<SqlLogicTest id="data_import_and_export/csv/auto_detection/example_003" />

## Detection Steps

### Dialect Detection

Dialect detection works by attempting to parse the samples using the set of considered values. The detected dialect is the dialect that has (1) a consistent number of columns for each row, and (2) the highest number of columns for each row.

The following dialects are considered for automatic dialect detection.

<!-- markdownlint-disable MD056 -->

| Parameters | Considered values   |
| ---------- | ------------------- | ---------- |
| `delim`    | `,` `               | ` `;` `\t` |
| `quote`    | `"` `'` (empty)     |
| `escape`   | `"` `'` `\` (empty) |

<!-- markdownlint-enable MD056 -->

Consider the example file <a href="/files/docs/flights.csv" download>`flights.csv`</a>:

```csv
FlightDate|UniqueCarrier|OriginCityName|DestCityName
1988-01-01|AA|New York, NY|Los Angeles, CA
1988-01-02|AA|New York, NY|Los Angeles, CA
1988-01-03|AA|New York, NY|Los Angeles, CA
```

In this file, the dialect detection works as follows:

-   If we split by a `|` every row is split into `4` columns.
-   If we split by a `,` rows 2-4 are split into `3` columns, while the first row is split into `1` column.
-   If we split by `;`, every row is split into `1` column.
-   If we split by `\t`, every row is split into `1` column.

In this example – the system selects the `|` as the delimiter. All rows are split into the same amount of columns, and there is more than one column per row meaning the delimiter was actually found in the CSV file.

### Type Detection

After detecting the dialect, the system will attempt to figure out the types of each of the columns. Note that this step is only performed if we are calling `read_csv`. In case of the `COPY` statement the types of the table that we are copying into will be used instead.

The type detection works by attempting to convert the values in each column to the candidate types. If the conversion is unsuccessful, the candidate type is removed from the set of candidate types for that column. After all samples have been handled – the remaining candidate type with the highest priority is chosen. The default set of candidate types is given below, in order of priority:

<div class="monospace_table"></div>

| Types       |
| ----------- |
| NULL        |
| BOOLEAN     |
| TIME        |
| DATE        |
| TIMESTAMP   |
| TIMESTAMPTZ |
| BIGINT      |
| DOUBLE      |
| VARCHAR     |

Everything can be cast to `VARCHAR`, therefore, this type has the lowest priority meaning that all columns are converted to `VARCHAR` as a fallback if they cannot be cast to anything else.
In <a href="/files/docs/flights.csv" download>`flights.csv`</a> the `FlightDate` column will be cast to a `DATE`, while the other columns will be cast to `VARCHAR`.

The set of candidate types that should be considered by the CSV reader can be specified explicitly using the [`auto_type_candidates`](../../data_import_and_export/csv/overview.md#auto_type_candidates-details) option. `VARCHAR` as the fallback type will always be considered as a candidate type whether you specify it or not.

Here are all additional candidate types that may be specified using the `auto_type_candidates` option, in order of priority:

<div class="monospace_table"></div>

| Types    |
| -------- |
| TINYINT  |
| SMALLINT |
| INTEGER  |
| DECIMAL  |
| FLOAT    |

Even though the set of data types that can be automatically detected may appear quite limited, the CSV reader can be configured to read arbitrarily complex types by using the `types`-option described in the next section.

Type detection can be entirely disabled by using the `all_varchar` option. If this is set all columns will remain as `VARCHAR` (as they originally occur in the CSV file).

Note that using quote characters vs. no quote characters (e.g., `"42"` and `42`) does not make a difference for type detection.
Quoted fields will not be converted to `VARCHAR`, instead, the sniffer will try to find the type candidate with the highest priority.

#### Overriding Type Detection

The detected types can be individually overridden using the `types` option. This option takes either of two options:

-   A list of type definitions (e.g., `types = ['INTEGER', 'VARCHAR', 'DATE']`). This overrides the types of the columns in-order of occurrence in the CSV file.
-   Alternatively, `types` takes a `name` → `type` map which overrides options of individual columns (e.g., `types = {'quarter': 'INTEGER'}`).

The set of column types that may be specified using the `types` option is not as limited as the types available for the `auto_type_candidates` option: any valid type definition is acceptable to the `types`-option. (To get a valid type definition, use the [`typeof()`](../../sql/functions/utility.md#typeofexpression) function, or use the `column_type` column of the [`DESCRIBE`](../../cookbook/meta/describe.md) result.)

The `sniff_csv()` function's `Column` field returns a struct with column names and types that can be used as a basis for overriding types.

## Header Detection

Header detection works by checking if the candidate header row deviates from the other rows in the file in terms of types. For example, in <a href="/files/docs/flights.csv" download>`flights.csv`</a>, we can see that the header row consists of only `VARCHAR` columns – whereas the values contain a `DATE` value for the `FlightDate` column. As such – the system defines the first row as the header row and extracts the column names from the header row.

In files that do not have a header row, the column names are generated as `column0`, `column1`, etc.

Note that headers cannot be detected correctly if all columns are of type `VARCHAR` – as in this case the system cannot distinguish the header row from the other rows in the file. In this case, the system assumes the file has a header. This can be overridden by setting the `header` option to `false`.

### Dates and Timestamps

SereneDB supports the [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601) by default for timestamps, dates and times. Unfortunately, not all dates and times are formatted using this standard. For that reason, the CSV reader also supports the `dateformat` and `timestampformat` options. Using this format the user can specify a [format string](../../sql/functions/dateformat.md) that specifies how the date or timestamp should be read.

As part of the auto-detection, the system tries to figure out if dates and times are stored in a different representation. This is not always possible – as there are ambiguities in the representation. For example, the date `01-02-2000` can be parsed as either January 2nd or February 1st. Often these ambiguities can be resolved. For example, if we later encounter the date `21-02-2000` then we know that the format must have been `DD-MM-YYYY`. `MM-DD-YYYY` is no longer possible as there is no 21st month.

If the ambiguities cannot be resolved by looking at the data the system has a list of preferences for which date format to use. If the system chooses incorrectly, the user can specify the `dateformat` and `timestampformat` options manually.

The system considers the following formats for dates (`dateformat`). Higher entries are chosen over lower entries in case of ambiguities (i.e., ISO 8601 is preferred over `MM-DD-YYYY`).

<div class="monospace_table"></div>

| dateformat |
| ---------- |
| ISO 8601   |
| %y-%m-%d   |
| %Y-%m-%d   |
| %d-%m-%y   |
| %d-%m-%Y   |
| %m-%d-%y   |
| %m-%d-%Y   |

The system considers the following formats for timestamps (`timestampformat`). Higher entries are chosen over lower entries in case of ambiguities.

<div class="monospace_table"></div>

| timestampformat      |
| -------------------- |
| ISO 8601             |
| %y-%m-%d %H:%M:%S    |
| %Y-%m-%d %H:%M:%S    |
| %d-%m-%y %H:%M:%S    |
| %d-%m-%Y %H:%M:%S    |
| %m-%d-%y %I:%M:%S %p |
| %m-%d-%Y %I:%M:%S %p |
| %Y-%m-%d %H:%M:%S.%f |
