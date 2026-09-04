---
layout: docu
redirect_from:
- /docs/guides/file_formats/parquet_export
- /docs/guides/import/parquet_export
- /docs/preview/guides/file_formats/parquet_export
- /docs/stable/guides/file_formats/parquet_export
title: Parquet Export
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To export the data from a table to a Parquet file, use the `COPY` statement:

<SqlLogicTest id="cookbook/file_formats/parquet_export/example_001" />

The result of queries can also be directly exported to a Parquet file:

<SqlLogicTest id="cookbook/file_formats/parquet_export/example_002" />

The flags for setting compression, row group size, etc. are listed in the [Reading and Writing Parquet files](../../data_import_and_export/parquet/overview.md) page.
