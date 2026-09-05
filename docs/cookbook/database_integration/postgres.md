---
title: PostgreSQL
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

# PostgreSQL

SereneDB can work with a running PostgreSQL database directly — attach it and read **and write** its tables alongside your local data in the same SQL, with no export or copy step.

## Attach a database

`ATTACH` adds the PostgreSQL database to the catalog. The connection string is a list of `{key}={value}` arguments, and `TYPE postgres` tells SereneDB to open it as PostgreSQL:

<SqlLogicTest id="cookbook/database_integration/postgres_pgscan/example_001" hideResult />

<DocCallout type="attention" title="Attachments are per-session">
Attachment definitions are not persisted — after a restart or a new connection, run `ATTACH` again to make the database available.
</DocCallout>

Once attached, its tables are referenced as `catalog.schema.table` and queried like any local table:

<SqlLogicTest id="cookbook/database_integration/postgres_pgscan/example_002" />

The first argument is a [PostgreSQL connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING). The common arguments are:

| Name | Description | Default |
|---|---|---|
| `host` | Host to connect to | `localhost` |
| `port` | Port number | `5432` |
| `user` | PostgreSQL user name | OS user name |
| `password` | PostgreSQL password | |
| `dbname` | Database name | user name |

Pass `READ_ONLY` after the type — `(TYPE postgres, READ_ONLY)` — to open the database for reading only.

## Scan a single table

To read one table without attaching the whole database, use `postgres_scan(connection_string, schema, table)`:

<SqlLogicTest id="cookbook/database_integration/postgres_pgscan/example_003" />

## Write to the attached database

An attached database is read-write by default. `INSERT`, `UPDATE`, `DELETE` and `CREATE TABLE` work just like local tables — the changes are written straight back to PostgreSQL:

<SqlLogicTest id="cookbook/database_integration/postgres_pgscan/example_005" hideResult />

## Detach

Close the connection when you are done:

<SqlLogicTest id="cookbook/database_integration/postgres_pgscan/example_004" hideResult />

## See also

- [ATTACH AND DETACH](../../sql/statements/attach/postgres.md) — full reference for attaching PostgreSQL and other databases
