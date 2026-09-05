---
title: Writing JSON
sidebar_position: 3
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The contents of tables or the result of queries can be written directly to a JSON file using the `COPY` statement.
For example:

<SqlLogicTest id="data_import_and_export/json/writing_json/example_001" />

This writes `cities.json` with one JSON object per line. Reading the file back shows its contents:

<SqlLogicTest id="data_import_and_export/json/writing_json/example_002" />

See the [`COPY` statement](../../sql/statements/copy/index.md#copy--to) for more information.
