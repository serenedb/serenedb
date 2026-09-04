---
draft: true
layout: docu
redirect_from:
- /docs/guides/file_formats/excel_import
- /docs/guides/import/excel_import
- /docs/preview/guides/file_formats/excel_import
- /docs/stable/guides/file_formats/excel_import
title: Excel Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports reading Excel `.xlsx` files. However, `.xls` files are not supported.

## Importing Excel Sheets

Use the `read_xlsx` function in the `FROM` clause of a query:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_001" />

Alternatively, you can omit the `read_xlsx` function and let SereneDB infer the format from the file name:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_002" />

However, if you want to be able to pass options to control the import behavior, you should use the `read_xlsx` function.

One such option is the `sheet` parameter, which allows specifying the name of the Excel worksheet:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_003" />

By default, the first sheet is loaded if no sheet is specified.

## Importing a Specific Range

To select a specific range of cells, use the `range` parameter with a string in the format `A1:B2`, where `A1` is the top-left cell and `B2` is the bottom-right cell:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_004" />

For example, to skip the first 5 rows:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_005" />

To skip the first 5 columns:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_006" />

If no range parameter is provided, SereneDB automatically infers the range as the rectangular region of cells between the first row of consecutive non-empty cells and the first empty row spanning the same columns.

By default, if no range is provided, SereneDB will stop reading the Excel file when encountering an empty row. But when a range is provided, the default is to read until the end of the range. This behavior can be controlled with the `stop_at_empty` parameter:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_007" />

## Creating a New Table

To create a new table using the result from a query, use `CREATE TABLE ... AS` from a `SELECT` statement:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_008" />

## Loading to an Existing Table

To load data into an existing table from a query, use `INSERT INTO` from a `SELECT` statement:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_009" />

Alternatively, you can use the `COPY` statement with the `XLSX` format option to import an Excel file into an existing table:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_010" />

When using the `COPY` statement to load an Excel file into an existing table, the types of the columns in the target table will be used to coerce the types of the cells in the Excel sheet.

## Importing a Sheet with/without a Header

To treat the first row as containing the names of the resulting columns, use the `header` parameter:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_011" />

By default, the first row is treated as a header if all the cells in the first row (within the inferred or supplied range) are non-empty strings. To disable this behavior, set `header` to `false`.

## Detecting Types

When not importing into an existing table, SereneDB will attempt to infer the types of the columns in the Excel sheet based on their contents and/or "number format".

- `TIMESTAMP`, `TIME`, `DATE` and `BOOLEAN` types are inferred when possible based on the "number format" applied to the cell.
- Text cells containing `TRUE` and `FALSE` are inferred as `BOOLEAN`.
- Empty cells are considered to be of type `DOUBLE` by default.
- Otherwise cells are inferred as `VARCHAR` or `DOUBLE` based on their contents.

You can adjust this behavior in several ways.

To treat all empty cells as `VARCHAR` instead of `DOUBLE`, set `empty_as_varchar` to `true`:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_012" />

To disable type inference completely and treat all cells as `VARCHAR`, set `all_varchar` to `true`:

<SqlLogicTest id="cookbook/file_formats/excel_import/example_013" />

Additionally, if the `ignore_errors` parameter is set to `true`, SereneDB will silently replace cells that can't be cast to the corresponding inferred column type with `NULL`s.

<SqlLogicTest id="cookbook/file_formats/excel_import/example_014" />

## See Also

SereneDB can also [export Excel files](excel_export.md).
