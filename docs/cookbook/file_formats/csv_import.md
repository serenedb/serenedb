---
layout: docu
redirect_from:
- /docs/guides/file_formats/csv_import
- /docs/guides/import/csv_import
- /docs/preview/guides/file_formats/csv_import
- /docs/stable/guides/file_formats/csv_import
title: CSV Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To read data from a CSV file, use the `read_csv` function in the `FROM` clause of a query:

<SqlLogicTest id="cookbook/file_formats/csv_import/example_001" />

Alternatively, you can omit the `read_csv` function and let SereneDB infer the format from the file name:

<SqlLogicTest id="cookbook/file_formats/csv_import/example_002" />

To create a new table using the result from a query, use [`CREATE TABLE ... AS SELECT` statement](../../sql/statements/create_table/index.md#create-table--as-select-ctas):

<SqlLogicTest id="cookbook/file_formats/csv_import/example_003" />

We can use SereneDB's [optional `FROM`-first syntax](../../sql/query_syntax/from_and_join/index.md#from-first-syntax) to omit `SELECT *`:

<SqlLogicTest id="cookbook/file_formats/csv_import/example_004" />

To load data into an existing table from a query, use `INSERT INTO` from a `SELECT` statement:

<SqlLogicTest id="cookbook/file_formats/csv_import/example_005" />

Alternatively, the `COPY` statement can also be used to load data from a CSV file into an existing table:

<SqlLogicTest id="cookbook/file_formats/csv_import/example_006" />

For additional options, see the [CSV import reference](../../data_import_and_export/csv/overview.md) and the [`COPY` statement documentation](../../sql/statements/copy/index.md).
