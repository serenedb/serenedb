---
title: Overview
sidebar_position: 1
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB can read multiple files of different types (CSV, Parquet, JSON files) at the same time using either the glob syntax, or by providing a list of files to read.
See the [combining schemas](../../data_import_and_export/multiple_files/combining_schemas.md) page for tips on reading files with different schemas.

## CSV

Read all files with a name ending in `.csv` in the folder `dir`:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_001" />

Read all files with a name ending in `.csv`, two directories deep:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_002" />

Read all files with a name ending in `.csv`, at any depth in the folder `dir`:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_003" />

Read the CSV files `flights1.csv` and `flights2.csv`:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_004" />

Read the CSV files `flights1.csv` and `flights2.csv`, unifying schemas by name and outputting a `filename` column:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_005" />

## Parquet

Read all files that match the glob pattern:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_006" />

Read three Parquet files and treat them as a single table:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_007" />

Read all Parquet files from two specific folders:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_008" />

Read all Parquet files that match the glob pattern at any depth:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_009" />

## Multi-File Reads and Globs

SereneDB can also read a series of Parquet files and treat them as if they were a single table. Note that this only works if the Parquet files have the same schema. You can specify which Parquet files you want to read using a list parameter, glob pattern matching syntax, or a combination of both.

### List Parameter

The `read_parquet` function can accept a list of filenames as the input parameter.

Read three Parquet files and treat them as a single table:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_010" />

### Glob Syntax

Any file name input to the `read_parquet` function can either be an exact filename, or use a glob syntax to read multiple files that match a pattern.

| Wildcard | Description                                               |
| -------- | --------------------------------------------------------- |
| `*`      | Matches any number of any characters (including none)     |
| `**`     | Matches any number of subdirectories (including none)     |
| `?`      | Matches any single character                              |
| `[abc]`  | Matches one character given in the bracket                |
| `[a-z]`  | Matches one character from the range given in the bracket |

Note that the `?` wildcard in globs is not supported for reads over S3 due to HTTP encoding issues.

Here is an example that reads all the files that end with `.parquet` located in the `test` folder:

Read all files that match the glob pattern:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_011" />

### List of Globs

The glob syntax and the list input parameter can be combined to scan files that meet one of multiple patterns.

Read all Parquet files from 2 specific folders.

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_012" />

SereneDB can read multiple CSV files at the same time using either the glob syntax, or by providing a list of files to read.

## Filename

The `filename` argument can be used to add an extra `filename` column to the result that indicates which row came from which file. For example:

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_013" />

<DocCallout type="tip">
The `filename` argument also accepts a string (e.g., `filename = 'input_file'`). When provided, the string is used as the name of the added column. This is useful when the source data already contains a `filename` column and you want to avoid a name collision.
</DocCallout>

## Glob Function to Find Filenames

The glob pattern matching syntax can also be used to search for filenames using the `glob` table function.
It accepts one parameter: the path to search (which may include glob patterns).

Search the current directory for all files.

<SqlLogicTest id="data_import_and_export/multiple_files/overview/example_014" />
