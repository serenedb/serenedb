---
layout: docu
redirect_from:
- /docs/guides/database_integration/sqlite
- /docs/guides/import/query_sqlite
- /docs/preview/guides/database_integration/sqlite
- /docs/stable/guides/database_integration/sqlite
title: SQLite Import
draft: true
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Usage

Tables can be queried from SQLite using the `sqlite_scan` function:

<SqlLogicTest id="cookbook/database_integration/sqlite/example_003" />

Alternatively, the entire file can be attached using the `ATTACH` command. This allows you to query all tables stored within a SQLite database file as if they were a regular database.

<SqlLogicTest id="cookbook/database_integration/sqlite/example_004" />
