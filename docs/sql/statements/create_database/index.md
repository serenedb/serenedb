---
title: CREATE DATABASE
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

## See also

- [ATTACH / DETACH](../attach/index.md) — attach an existing database file
- [USE](../use/index.md) — switch the active database

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
