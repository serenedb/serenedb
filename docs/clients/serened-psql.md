---
title: serened psql
sidebar_position: 9
---

# serened psql

`serened psql` is a PostgreSQL-compatible command-line client built into the `serened` binary — no separate installation required. It connects to a running SereneDB server over the PostgreSQL wire protocol and behaves like the standard [`psql`](./psql.md) client: on startup it attaches to the server and switches to the requested database, so you can run queries interactively or in batch.

To work with a local database file directly, without a running server, use [`serened shell`](./serened-shell.md) instead.

## Connect

```sh
serened psql -h localhost -p 7890 -d postgres
```

:::note
`serened psql` follows the PostgreSQL convention and defaults to port **`5432`**, while a SereneDB server listens on **`7890`** by default. Pass `-p 7890` (or set `PGPORT`) so the client reaches the server.
:::

When an option is omitted, the client falls back to the corresponding environment variable, then a built-in default:

| Setting  | Flag         | Default                              |
| :------- | :----------- | :----------------------------------- |
| Host     | `-h`         | `$PGHOST`, otherwise `localhost`     |
| Port     | `-p`         | `$PGPORT`, otherwise `5432`          |
| User     | `-U`         | `$PGUSER`, `$USER`, otherwise `postgres` |
| Database | `-d`         | `$PGDATABASE`, otherwise the user name |

The first two positional arguments are taken as `DBNAME` and `USERNAME` when the matching flags are not given:

```sh
serened psql -h localhost -p 7890 postgres alice
```

The bootstrap superuser `postgres` connects locally without a password; other roles (and remote connections) authenticate with their [role password](../security/client_authentication.md). `-W` forces a password prompt, `-w` never prompts.

## Running a single command

Use `-c` to run one statement and exit:

```sh
serened psql -h localhost -p 7890 -d postgres -c "SELECT 1 + 1 AS sum;"
```

```text
┌───────┐
│  sum  │
│ int32 │
├───────┤
│     2 │
└───────┘
```

## Connection options

| Option                  | Description                                                        |
| :---------------------- | :----------------------------------------------------------------- |
| `-h`, `--host=HOSTNAME` | Server host (default: `$PGHOST` or `localhost`)                    |
| `-p`, `--port=PORT`     | Server port (default: `$PGPORT` or `5432`)                         |
| `-d`, `--dbname=DBNAME` | Database name (default: `$PGDATABASE` or the user name)            |
| `-U`, `--username=NAME` | User name (default: `$PGUSER`, `$USER`, or `postgres`)             |
| `-w`, `--no-password`   | Never prompt for a password                                        |
| `-W`, `--password`      | Force a password prompt                                            |

## General options

| Option                       | Description                                                  |
| :--------------------------- | :----------------------------------------------------------- |
| `-c`, `--command=COMMAND`    | Run a single command (SQL or dot-command) and exit           |
| `-f`, `--file=FILENAME`      | Execute commands from a file, then exit                      |
| `-l`, `--list`               | List the available databases, then exit                      |
| `-1`, `--single-transaction` | Run the `-c` / `-f` batch inside a single transaction        |
| `-X`, `--no-psqlrc`          | Do not read the startup file (`~/.duckdbrc`)                 |
| `-q`, `--quiet`              | Suppress startup and informational messages                  |
| `-V`, `--version`            | Print the SereneDB version and exit                          |
| `-?`, `--help`               | Show the help message and exit                               |

## Output format options

The default output is the aligned `duckbox` table. These options change how results are rendered:

| Option                          | Description                                              |
| :------------------------------ | :------------------------------------------------------- |
| `-A`, `--no-align`              | Unaligned output                                         |
| `--csv`                         | CSV output                                               |
| `-H`, `--html`                  | HTML output                                              |
| `-t`, `--tuples-only`           | Print rows only, without column headers                  |
| `-x`, `--expanded`              | Expanded output (one value per line)                     |
| `-F`, `--field-separator=STR`   | Field separator for unaligned output (default `\|`)       |
| `-R`, `--record-separator=STR`  | Record separator for unaligned output (default newline)  |
| `-o`, `--output=FILENAME`       | Send query results to a file                             |
| `-P`, `--pset=VAR[=ARG]`        | Set a printing option (`format`, `expanded`, `null`, `tuples_only`, `pager`) |
| `--MODE`                        | Set the output mode directly (`box`, `csv`, `json`, `markdown`, `line`, …) |

## Examples

List the databases on the server:

```sh
serened psql -h localhost -p 7890 -l
```

Run a SQL script and exit:

```sh
serened psql -h localhost -p 7890 -d postgres -f schema.sql
```

Export a query result as CSV:

```sh
serened psql -h localhost -p 7890 -d postgres --csv -c "SELECT 'a' AS x, 1 AS y;"
```

```text
x,y
a,1
```

Run a script as a single transaction, writing the output to a file:

```sh
serened psql -h localhost -p 7890 -d postgres -1 -f migrate.sql -o result.txt
```
