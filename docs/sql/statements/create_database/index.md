---
title: CREATE DATABASE
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE DATABASE` statement creates a new, empty database. A single SereneDB server can host many independent databases (the default is `postgres`); switch between them with [`USE`](../use/index.md) or by connecting to one directly.

## Examples

Create a database named `app_production`:

<SqlLogicTest id="sql/statements/create_database/index/example_001" />

Use `IF NOT EXISTS` so the statement succeeds even when the database already exists, instead of raising an error:

<SqlLogicTest id="sql/statements/create_database/index/example_002" />

Once created, you can connect to it like any other database — for example with `psql`:

```sh
psql -h localhost -p 7890 -d app_production
```

As an alternative to reconnecting, switch to the new database within the current session with the [`USE`](../use/index.md) statement:

<SqlLogicTest id="sql/statements/create_database/index/example_003" />

## Storage options

`WITH` sets how the new database stores its tables:

<SqlLogicTest id="sql/statements/create_database/index/example_004" />

| Option           | Description                                                                         | Default  |
|------------------|-------------------------------------------------------------------------------------|----------|
| `BLOCK_SIZE`     | The block size of the database file in bytes: a power of two from 16384 to 262144. | `262144` |
| `ROW_GROUP_SIZE` | The number of rows in a row group: a multiple of 2048.                              | `122880` |

The options are stored with the database, so the server opens it with them again after a restart. `BLOCK_SIZE` is fixed once the file exists; `ROW_GROUP_SIZE` applies to the row groups written from then on. Any other option is refused.

## See also

- [ATTACH / DETACH](../attach/index.md) — attach an existing database file
- [USE](../use/index.md) — switch the active database

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
