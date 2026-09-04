---
title: PostgreSQL
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB can attach an entire PostgreSQL database — no extra setup required. Provide a [PostgreSQL connection string](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING) and the `TYPE postgres` option, and every table in the PostgreSQL database becomes queryable as if it were a regular SereneDB catalog — so you can join live PostgreSQL rows against your local search and analytics in a single query. For the general `ATTACH` and `DETACH` syntax, see [ATTACH AND DETACH](./index.md).

<DocCallout type="tip">
This works with any PostgreSQL-compatible database, not just PostgreSQL itself. Because SereneDB speaks the PostgreSQL wire protocol, you can also attach another SereneDB instance the same way — point the connection string at it and query its tables alongside your local data.
</DocCallout>

Attach a PostgreSQL database using a connection string:

<SqlLogicTest id="sql/statements/attach/postgres_pgscan/example_001" />

Query a table from the attached database using its fully qualified name:

<SqlLogicTest id="sql/statements/attach/postgres_pgscan/example_002" />

Attach a PostgreSQL database in read only mode:

<SqlLogicTest id="sql/statements/attach/postgres_pgscan/example_003" />

Detach the PostgreSQL database:

<SqlLogicTest id="sql/statements/attach/postgres_pgscan/example_004" />

Once attached, the PostgreSQL database behaves like any other catalog: its tables can be read, written and joined against your local data.

## Connection string

The connection string is a list of `{key}={value}` arguments. The most common ones are:

| Name       | Description                          | Default        |
| ---------- | ------------------------------------ | -------------- |
| `host`     | Name of host to connect to           | `localhost`    |
| `hostaddr` | Host IP address                      | `localhost`    |
| `port`     | Port number                          | `5432`         |
| `user`     | PostgreSQL user name                 | [OS user name] |
| `password` | PostgreSQL password                  |                |
| `dbname`   | Database name                        | [user]         |
| `passfile` | Name of file passwords are stored in | `~/.pgpass`    |
