---
title: serenedb psql
sidebar_position: 9
split: headings
---

# serenedb psql

`serenedb psql` is a PostgreSQL-compatible command-line client built into the `serenedb` binary — no separate installation required. It connects to a running SereneDB server over the PostgreSQL wire protocol and behaves like the standard [`psql`](./psql.md) client: on startup it attaches to the server and switches to the requested database, so you can run queries interactively or in batch.

To work with a local database file directly, without a running server, use [`serenedb shell`](./serened-shell.md) instead. Both speak the same [SQL dialect](./serened-shell.md#sql-dialect).

## Connect

```sh
serenedb psql -h localhost -p 7890 -d postgres
```

:::note
`serenedb psql` follows the PostgreSQL convention and defaults to port **`5432`**, while a SereneDB server listens on **`7890`** by default. Pass `-p 7890` (or set `PGPORT`) so the client reaches the server.
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
serenedb psql -h localhost -p 7890 postgres alice
```

The bootstrap superuser `postgres` connects locally without a password; other roles (and remote connections) authenticate with their [role password](../security/client_authentication.md). `serened psql` does not prompt for it: the password comes from the `PGPASSWORD` environment variable or the `~/.pgpass` file, and `-w` and `-W` are accepted and change nothing.

## Running a single command

Use `-c` to run one statement and exit:

```sh
serenedb psql -h localhost -p 7890 -d postgres -c "SELECT 1 + 1 AS sum;"
```

```text
 sum 
-----
   2
(1 row)
```

## Output formats

`serened psql` prints results the way `psql` does. The default is an aligned table: column names centered, numbers aligned right and other values left, a value with a line break continued on the next line with a `+` before the break, and a `(N rows)` footer. Values are printed in their PostgreSQL text form, as a PostgreSQL server sends them: `t` and `f` for booleans, `{1,2}` for an array, dates and timestamps in PostgreSQL's format. `NULL` prints as an empty value, or as the text `\pset null` sets.

| Format                                                           | Flag                  | Command                        |
| :--------------------------------------------------------------- | :-------------------- | :----------------------------- |
| Aligned table, the default                                       | `-P format=aligned`   | `\pset format aligned`         |
| Expanded: a `-[ RECORD n ]` line per row, then a line per column | `-x`, `--expanded`    | `\x`                           |
| Unaligned: fields separated by `\|`, without padding             | `-A`, `--no-align`    | `\a`, `\pset format unaligned` |
| Rows only, without column names and footer                       | `-t`, `--tuples-only` | `\t`                           |
| CSV                                                              | `--csv`               | `\pset format csv`             |

Expanded output:

```sh
serened psql -h localhost -p 7890 -d postgres -x -c "SELECT 1 AS a, 2 AS b;"
```

```text
-[ RECORD 1 ]
a | 1
b | 2
```

Unaligned rows only, ready for a script to split:

```sh
serened psql -h localhost -p 7890 -d postgres -A -t -c "SELECT 1 AS a, 2 AS b;"
```

```text
1|2
```

CSV quotes a value that holds the separator, a double quote or a line break, and an empty string, which a `NULL` (printed as nothing) is not:

```sh
serened psql -h localhost -p 7890 -d postgres --csv -c "SELECT 'hello, world' AS msg;"
```

```text
msg
"hello, world"
```

`\pset NAME VALUE`, or `-P NAME=VALUE` on the command line, sets a printing option:

| Option             | Values                                                                 | Description                                                                                       |
| :----------------- | :--------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------ |
| `format`           | `aligned`, `wrapped`, `unaligned`, `csv`, `html`, `latex` or a DuckDB mode | The output format. `wrapped` prints as `aligned`; `html`, `latex` and the DuckDB modes switch to DuckDB's rendering. |
| `border`           | `0`, `1`, `2`                                                          | The lines between and around the columns of the aligned table, `1` by default.                   |
| `expanded` (`x`)   | `on`, `off`, `auto`                                                    | Expanded output; `auto` expands a result wider than the terminal.                                |
| `tuples_only` (`t`) | `on`, `off`                                                           | Rows only.                                                                                        |
| `footer`           | `on`, `off`                                                            | The `(N rows)` footer.                                                                            |
| `null`             | text                                                                   | The text printed for `NULL`, empty by default.                                                   |
| `fieldsep`         | text                                                                   | The field separator of unaligned output, `\|` by default.                                         |
| `fieldsep_zero`    |                                                                        | A zero byte as the field separator of unaligned output.                                          |
| `recordsep`        | text                                                                   | The record separator of unaligned output, a newline by default.                                  |
| `recordsep_zero`   |                                                                        | A zero byte as the record separator of unaligned output.                                         |
| `csv_fieldsep`     | one character                                                          | The field separator of CSV output, `,` by default.                                                |
| `pager`            | `on`, `off`, `always`                                                  | Whether long output goes through the pager.                                                      |

Without a value, `expanded`, `tuples_only`, `footer` and `pager` switch between on and off, and the other options print their value. `\pset` alone lists them all. On the command line, an unknown option or value prints a warning and is ignored.

```sh
serened psql -h localhost -p 7890 -d postgres -P border=2 -c "SELECT 5 AS v;"
```

```text
+---+
| v |
+---+
| 5 |
+---+
(1 row)
```

`.mode duckbox`, `-P format=box` or any other [DuckDB output mode](./serened-shell.md#output-modes) switches to DuckDB's rendering.

## Browsing the documentation

`.docs` renders this documentation set inside the client, exactly as it does in
[`serenedb shell`](./serened-shell.md#browsing-the-documentation) — same arguments, same
output. The pages come from the index compiled into the `serenedb` binary rather than from
the server, so a newer client shows its own documentation when connected to an older
server. That holds for `--search` too: it ranks in-process and never queries the
connected server.

```sh
serenedb psql -h localhost -p 7890 -d postgres -c ".docs BM25"
```

The [`sdb_docs` tables and functions](../sql/functions/docs.md) answer from that same
copy when you query them in `serenedb psql`, because queries run in the client. To read
the server's copy instead, the version that server actually runs, pass the query
through `postgres_query` with the database you connected to:

```sh
serenedb psql -h localhost -p 7890 -d postgres -c "SELECT path, title FROM postgres_query('postgres', 'SELECT path, title FROM sdb_docs.search(''vacuum'', 5)')"
```

## Dot and backslash commands

`serenedb psql` accepts the same [dot commands](./serened-shell.md#dot-commands) and [backslash commands](./serened-shell.md#backslash-commands) as `serenedb shell`: from `.mode`, `.timer` and `.docs` to `\x`, `\pset` and `\copy`. `.help` and `\?` list them. `COPY ... FROM STDIN` and `COPY ... TO STDOUT` exchange their data with the client as in `serenedb shell`, see [COPY with standard input and output](./serened-shell.md#copy-with-standard-input-and-output).

## Connection options

| Option                  | Description                                                        |
| :---------------------- | :----------------------------------------------------------------- |
| `-h`, `--host=HOSTNAME` | Server host (default: `$PGHOST` or `localhost`)                    |
| `-p`, `--port=PORT`     | Server port (default: `$PGPORT` or `5432`)                         |
| `-d`, `--dbname=DBNAME` | Database name (default: `$PGDATABASE` or the user name)            |
| `-U`, `--username=NAME` | User name (default: `$PGUSER`, `$USER`, or `postgres`)             |
| `-w`, `--no-password`   | Accepted; changes nothing                                          |
| `-W`, `--password`      | Accepted; changes nothing                                          |

## General options

| Option                       | Description                                                  |
| :--------------------------- | :----------------------------------------------------------- |
| `-c`, `--command=COMMAND`    | Run a single command (SQL or dot-command) and exit           |
| `-f`, `--file=FILENAME`      | Execute commands from a file, then exit                      |
| `-l`, `--list`               | List the available databases, then exit                      |
| `-1`, `--single-transaction` | Run the `-c` / `-f` batch inside a single transaction        |
| `-X`, `--no-psqlrc`          | Do not read the startup file (`~/.sdbrc`)                    |
| `-q`, `--quiet`              | Suppress startup and informational messages                  |
| `-V`, `--version`            | Print the SereneDB version and exit                          |
| `-?`, `--help`               | Show the help message and exit                               |

## Output format options

The default output is psql's aligned table, see [Output formats](#output-formats). These options change how results are rendered:

| Option                          | Description                                              |
| :------------------------------ | :------------------------------------------------------- |
| `-A`, `--no-align`              | Unaligned output                                         |
| `--csv`                         | CSV output                                               |
| `-H`, `--html`                  | HTML output                                              |
| `-t`, `--tuples-only`           | Print rows only, without column names and footer         |
| `-x`, `--expanded`              | Expanded output (one line per column)                    |
| `-F`, `--field-separator=STR`   | Field separator for unaligned output (default `\|`)       |
| `-R`, `--record-separator=STR`  | Record separator for unaligned output (default newline)  |
| `-z`, `--field-separator-zero`  | Zero byte as the field separator for unaligned output    |
| `-0`, `--record-separator-zero` | Zero byte as the record separator for unaligned output   |
| `-o`, `--output=FILENAME`       | Send query results to a file                             |
| `-P`, `--pset=VAR[=ARG]`        | Set a printing option, as [`\pset`](#output-formats) does |
| `--MODE`                        | Switch to a DuckDB output mode (`box`, `duckbox`, `json`, `markdown`, `line`, …) |

## Examples

List the databases on the server:

```sh
serenedb psql -h localhost -p 7890 -l
```

Run a SQL script and exit:

```sh
serenedb psql -h localhost -p 7890 -d postgres -f schema.sql
```

Export a query result as CSV:

```sh
serenedb psql -h localhost -p 7890 -d postgres --csv -c "SELECT 'a' AS x, 1 AS y;"
```

```text
x,y
a,1
```

Run a script as a single transaction, writing the output to a file:

```sh
serenedb psql -h localhost -p 7890 -d postgres -1 -f migrate.sql -o result.txt
```
