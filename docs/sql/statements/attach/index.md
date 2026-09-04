---
title: ATTACH AND DETACH
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB can attach and detach whole databases at runtime. Attaching adds a database to the catalog so its tables can be read, written and joined alongside your local data in the same query. Once attached, a database is just another catalog you reference as `catalog.schema.table`.

Each kind of database has its own dedicated page:

- [PostgreSQL](./postgres.md) — attach an external PostgreSQL database over a connection string.
- [DuckDB](./duckdb.md) — attach a DuckDB database file, including remote files over HTTP and S3.

Attachment definitions are not persisted between sessions: when a new session is launched, you have to re-attach to all databases. For a **persistent** connection to a PostgreSQL or ClickHouse instance — one that survives restart, has an owner and is guarded by `USAGE` privileges — use [`CREATE SERVER`](../create_server/index.md) instead.

## `ATTACH`

The `ATTACH` statement adds a database to the catalog that can be read from and written to. It lets SereneDB operate on multiple databases at once and transfer data between them.

### `ATTACH` Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

The type of an attached database is either given explicitly with the `TYPE` option or deduced from the path:

- A **file path** attaches a [DuckDB database file](./duckdb.md).
- A **connection string** (e.g. `host=… port=…`) attaches an external [PostgreSQL database](./postgres.md) with `TYPE postgres`.

By default, `ATTACH` opens a database for reading and writing. Pass `READ_ONLY` to open it for reading only.

Use `IF NOT EXISTS` to attach only when the alias is not already in use, or `OR REPLACE` to detach and replace an existing alias. An explicit alias can be given with `AS`; otherwise it is inferred from the database name.

## `DETACH`

The `DETACH` statement closes a previously attached database and releases any locks held on it.

### `DETACH` Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

It is not possible to detach the default database. To do so, first issue the [`USE` statement](../use/index.md) to change the default database to another one. For example, if you are connected to a persistent database, you may switch to an in-memory database:

<SqlLogicTest id="sql/statements/attach/index/example_021" />

## Name Qualification

The fully qualified name of catalog objects contains the _catalog_, the _schema_ and the _name_ of the object. This applies to any attached database, regardless of its type. For example:

Attach the database `new_db`:

<SqlLogicTest id="sql/statements/attach/index/example_022" />

Create the schema `my_schema` in the database `new_db`:

<SqlLogicTest id="sql/statements/attach/index/example_023" />

Create the table `my_table` in the schema `my_schema`:

<SqlLogicTest id="sql/statements/attach/index/example_024" />

Refer to the column `col` inside the table `my_table`:

<SqlLogicTest id="sql/statements/attach/index/example_025" />

Note that often the fully qualified name is not required. When a name is not fully qualified, the system looks for which entries to reference using the _catalog search path_. The default catalog search path includes the system catalog, the temporary catalog and the initially attached database together with the `main` schema.

Also note the rules on [identifiers and database names in particular](../../../compatibility/keywords_and_identifiers.md#database-names).

### Default Database and Schema

When a table is created without any qualifications, the table is created in the default schema of the default database. The default database is the database that is launched when the system is created – and the default schema is `main`.

Create the table `my_table` in the default database:

<SqlLogicTest id="sql/statements/attach/index/example_026" />

### Changing the Default Database and Schema

The default database and schema can be changed using the `USE` command.

Set the default database schema to `new_db.main`:

<SqlLogicTest id="sql/statements/attach/index/example_027" />

Set the default database schema to `new_db.my_schema`:

<SqlLogicTest id="sql/statements/attach/index/example_028" />

### Resolving Conflicts

When providing only a single qualification, the system can interpret this as _either_ a catalog _or_ a schema, as long as there are no conflicts. For example:

<SqlLogicTest id="sql/statements/attach/index/example_029" />

Creates the table `new_db.main.tbl`:

<SqlLogicTest id="sql/statements/attach/index/example_030" />

Creates the table `default_db.my_schema.tbl`:

<SqlLogicTest id="sql/statements/attach/index/example_031" />

If we create a conflict (i.e., we have both a schema and a catalog with the same name) the system requests that a fully qualified path is used instead:

<SqlLogicTest id="sql/statements/attach/index/example_032" />

### Changing the Catalog Search Path

The catalog search path can be adjusted by setting the `search_path` configuration option, which uses a comma-separated list of values that will be on the search path. The following example demonstrates searching in two databases:

<SqlLogicTest id="sql/statements/attach/index/example_033" />

Reference the tables using their fully qualified name:

<SqlLogicTest id="sql/statements/attach/index/example_034" />

Or set the search path and reference the tables using their name:

<SqlLogicTest id="sql/statements/attach/index/example_035" />

## Transactional Semantics

When running queries on multiple databases, the system opens separate transactions per database. The transactions are started _lazily_ by default – when a given database is referenced for the first time in a query, a transaction for that database will be started. `SET immediate_transaction_mode = true` can be toggled to change this behavior to eagerly start transactions in all attached databases instead.

While multiple transactions can be active at a time – the system only supports _writing_ to a single attached database in a single transaction. Attempting to write to a second attached database within the same transaction raises an error.

The reason for this restriction is that the system does not maintain atomicity for transactions across attached databases. Transactions are only atomic _within_ each attached database. By restricting the global transaction to write to only a single attached database the atomicity guarantees are maintained.
