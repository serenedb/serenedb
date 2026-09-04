---
layout: docu
redirect_from:
- /docs/guides/file_formats/json_import
- /docs/guides/import/json_import
- /docs/preview/guides/file_formats/json_import
- /docs/stable/guides/file_formats/json_import
title: JSON Import
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To read data from a JSON file, use the `read_json_auto` function in the `FROM` clause of a query:

<SqlLogicTest id="cookbook/file_formats/json_import/example_001" />

To create a new table using the result from a query, use `CREATE TABLE AS` from a `SELECT` statement:

<SqlLogicTest id="cookbook/file_formats/json_import/example_002" />

To load data into an existing table from a query, use `INSERT INTO` from a `SELECT` statement:

<SqlLogicTest id="cookbook/file_formats/json_import/example_003" />

Alternatively, the `COPY` statement can also be used to load data from a JSON file into an existing table:

<SqlLogicTest id="cookbook/file_formats/json_import/example_004" />

For additional options, see the [JSON Loading reference](../../data_import_and_export/json/overview.md) and the [`COPY` statement documentation](../../sql/statements/copy/index.md).
