---
layout: docu
redirect_from:
- /docs/guides/file_formats/query_parquet
- /docs/guides/import/query_parquet
- /docs/preview/guides/file_formats/query_parquet
- /docs/stable/guides/file_formats/query_parquet
title: Querying Parquet Files
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

To run a query directly on a Parquet file, use the `read_parquet` function in the `FROM` clause of a query.

<SqlLogicTest id="cookbook/file_formats/query_parquet/example_001" />

The Parquet file will be processed in parallel. Filters will be automatically pushed down into the Parquet scan, and only the relevant columns will be read automatically.
