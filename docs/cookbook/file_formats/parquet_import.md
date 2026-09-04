---
layout: docu
redirect_from:
- /docs/guides/file_formats/parquet_import
- /docs/guides/import/parquet_import
- /docs/preview/guides/file_formats/parquet_import
- /docs/stable/guides/file_formats/parquet_import
title: Parquet Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To read data from a Parquet file, use the `read_parquet` function in the `FROM` clause of a query:

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_001" />

Alternatively, you can omit the `read_parquet` function and let SereneDB infer the format from the file name:

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_002" />

To create a new table using the result from a query, use the [`CREATE TABLE ... AS SELECT` statement](../../sql/statements/create_table/index.md#create-table--as-select-ctas):

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_003" />

To load data into an existing table from a query, use `INSERT INTO` from a `SELECT` statement:

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_004" />

Alternatively, use the `COPY` statement to load data from a Parquet file into an existing table:

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_005" />

## Adjusting the Schema on the Fly

You can load a Parquet file into a slightly different schema (e.g., different number of columns, more relaxed types) using the following trick.

Suppose you have a Parquet file with two columns, `c1` and `c2`:

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_006" />

To add another column `c3` that is not present in the file, run:

<SqlLogicTest id="cookbook/file_formats/parquet_import/example_007" />

The first `FROM` clause generates an empty table with *three* columns where `c1` is a `VARCHAR`.
`UNION ALL BY NAME` then appends the rows from the Parquet file, matching them by column name and leaving `c3` as `NULL` since it is absent there.

For additional options, see the [Parquet loading reference](../../data_import_and_export/parquet/overview.md).
