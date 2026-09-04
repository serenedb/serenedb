---
layout: docu
redirect_from:
- /docs/guides/file_formats/json_export
- /docs/guides/import/json_export
- /docs/preview/guides/file_formats/json_export
- /docs/stable/guides/file_formats/json_export
title: JSON Export
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To export the data from a table to a JSON file, use the `COPY` statement:

<SqlLogicTest id="cookbook/file_formats/json_export/example_001" />

The result of queries can also be directly exported to a JSON file. Reading the file back shows its contents:

<SqlLogicTest id="cookbook/file_formats/json_export/example_002" />

<SqlLogicTest id="cookbook/file_formats/json_export/example_004" />

The JSON export writes JSON lines by default, standardized as [Newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#NDJSON).
The `ARRAY` option can be used to write a single JSON array object instead; reading it back yields the same rows:

<SqlLogicTest id="cookbook/file_formats/json_export/example_003" />

<SqlLogicTest id="cookbook/file_formats/json_export/example_005" />

For additional options, see the [`COPY` statement documentation](../../sql/statements/copy/index.md).
