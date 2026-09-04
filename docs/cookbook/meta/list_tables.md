---
layout: docu
redirect_from:
- /docs/guides/meta/list_tables
- /docs/preview/guides/meta/list_tables
- /docs/stable/guides/meta/list_tables
title: List Tables
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SHOW TABLES` command can be used to obtain a list of all tables within the selected schema.

<SqlLogicTest id="cookbook/meta/list_tables/example_001" />

`SHOW` or `SHOW ALL TABLES` can be used to obtain a list of all tables within **all** attached databases and schemas.

<SqlLogicTest id="cookbook/meta/list_tables/example_002" />

`SHOW TABLES FROM db` can be used to list all tables in a given database or schema.

<SqlLogicTest id="cookbook/meta/list_tables/example_003" />

Or a specific schema.

<SqlLogicTest id="cookbook/meta/list_tables/example_004" />

To view the schema of an individual table, use the [`DESCRIBE` command](./describe.md).

## See Also

The SQL-standard [`information_schema`](../../sql/information_schema.md) views are also defined. Moreover, SereneDB defines `sqlite_master` and many [PostgreSQL system catalog tables](https://www.postgresql.org/docs/18/catalogs.html) for compatibility with SQLite and PostgreSQL respectively.
