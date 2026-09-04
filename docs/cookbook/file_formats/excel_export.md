---
draft: true
layout: docu
redirect_from:
- /docs/guides/file_formats/excel_export
- /docs/guides/import/excel_export
- /docs/preview/guides/file_formats/excel_export
- /docs/stable/guides/file_formats/excel_export
title: Excel Export
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports exporting data to Excel `.xlsx` files. Please note that `.xls` files are not supported.

## Exporting Excel Sheets

To export a table to an Excel file, use the `COPY` statement with the `FORMAT xlsx` option:

<SqlLogicTest id="cookbook/file_formats/excel_export/example_002" />

The result of a query can also be directly exported to an Excel file:

<SqlLogicTest id="cookbook/file_formats/excel_export/example_003" />

Or:

<SqlLogicTest id="cookbook/file_formats/excel_export/example_004" />

To write the column names as the first row in the Excel file, use the `HEADER` option:

<SqlLogicTest id="cookbook/file_formats/excel_export/example_005" />

To name the worksheet in the resulting Excel file, use the `SHEET` option:

<SqlLogicTest id="cookbook/file_formats/excel_export/example_006" />

## Type Conversions

Because Excel only really supports storing numbers or strings – the equivalent of `VARCHAR` and `DOUBLE`, the following type conversions are automatically applied when writing XLSX files:

* Numeric types are cast to `DOUBLE`.
* Temporal types (`TIMESTAMP`, `DATE`, `TIME`, etc.) are converted to Excel "serial" numbers, that is the number of days since 1900-01-01 for dates and the fraction of a day for times. These are then styled with a "number format" so that they appear as dates or times when opened in Excel.
* `TIMESTAMP_TZ` and `TIME_TZ` are cast to UTC `TIMESTAMP` and `TIME` respectively, with the timezone information being lost.
* `BOOLEAN`s are converted to `1` and `0`, with a "number format" applied to make them appear as `TRUE` and `FALSE` in Excel.
* All other types are cast to `VARCHAR` and then written as text cells.

But you can of course also explicitly cast columns to a different type before exporting them to Excel:

<SqlLogicTest id="cookbook/file_formats/excel_export/example_007" />

## See Also

SereneDB can also [import Excel files](./excel_import.md).
