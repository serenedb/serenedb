---
title: psql
sidebar_position: 8
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# psql

[psql](https://www.postgresql.org/docs/current/app-psql.html) is PostgreSQL's interactive terminal client. It works directly with SereneDB.

## Connect

```sh
psql -h localhost -p 7890
```

## Useful commands

| Command | Description |
|---|---|
| `\l` | List databases |
| `\d` | List tables |
| `\d table_name` | Describe a table |
| `\d+` | List tables with sizes |
| `\i file.sql` | Execute a SQL file |
| `\timing` | Toggle query timing |
| `\q` | Quit |

## Running queries

Run any SQL statement at the prompt and terminate it with a semicolon:

<SqlLogicTest id="clients/psql/example_001" />

## Importing CSV

Use the server-side `COPY ... FROM` statement to load a CSV file into an existing table:

<SqlLogicTest id="clients/psql/example_002" />

## Tips

- Use **Tab** to auto-complete table and column names
- Press **Ctrl+R** to search command history
- Use `\timing` to benchmark queries
