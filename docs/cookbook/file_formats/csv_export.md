---
layout: docu
redirect_from:
- /docs/guides/file_formats/csv_export
- /docs/guides/import/csv_export
- /docs/preview/guides/file_formats/csv_export
- /docs/stable/guides/file_formats/csv_export
title: CSV Export
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To export the data from a table to a CSV file, use the `COPY` statement:

<SqlLogicTest id="cookbook/file_formats/csv_export/example_001" />

The result of queries can also be directly exported to a CSV file:

<SqlLogicTest id="cookbook/file_formats/csv_export/example_002" />

For additional options, see the [`COPY` statement documentation](../../sql/statements/copy/index.md#csv-options).
