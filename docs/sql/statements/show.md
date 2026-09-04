---
title: SHOW
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SHOW` statement inspects the database — it lists tables, databases and schemas, shows the schema of a relation and displays the values of session variables.

## `SHOW`

`SHOW` is an alias for [`DESCRIBE`](describe.md): given a table, view or query it returns that relation's columns and their types.

## `SHOW TABLES`

`SHOW TABLES` lists the tables in the current database and schema:

<SqlLogicTest id="sql/statements/show/example_004" />

Use `SHOW TABLES FROM <database>` to list the tables of another attached database.

## `SHOW DATABASES`

`SHOW DATABASES` lists every attached database. This includes SereneDB's internal catalogs — the transactional store (`__sdb_store`) and the default in-memory database (`memory`) — alongside your own databases:

<SqlLogicTest id="sql/statements/show/example_001" />

## `SHOW SCHEMAS`

`SHOW SCHEMAS` lists the schemas of every attached database, internal catalogs included:

<SqlLogicTest id="sql/statements/show/example_003" />

Each row pairs a database with one of its schemas; the `current` column marks the default schema, set via the [`USE`](use/index.md) statement.

## Session variables

`SHOW <variable>` displays the current value of a session variable:

<SqlLogicTest id="sql/statements/show/example_005" />

`SHOW ALL` displays every variable with its value and description:

<SqlLogicTest id="sql/statements/show/example_006" />

## See also

- [DESCRIBE](describe.md) — `SHOW` is an alias for it
- [SET](set/index.md) — change a session variable
