---
title: serened shell
sidebar_position: 10
---

# serened shell

`serened shell` is a local, embedded SQL shell built into the `serened` binary. It runs queries directly against a DuckDB database file — or an in-memory database — **without starting a server or opening a network connection**. It is handy for ad-hoc queries, exploring data files (CSV, Parquet, DuckDB), scripting, and formatting SQL.

To connect to a running SereneDB server instead, use [`serened psql`](./serened-psql.md).

## Usage

```sh
serened shell [OPTION]... [FILENAME [SQL]]
```

`FILENAME` is the path to a DuckDB database file; a new file is created if it does not exist. Omit it (or pass `:memory:`) for an in-memory database. The optional `SQL` argument is run before the interactive prompt starts.

Open an in-memory shell:

```sh
serened shell
```

Open a database file:

```sh
serened shell my_data.db
```

## Running a single command

Use `-c` to run one statement and exit:

```sh
serened shell :memory: -c "SELECT 1 + 1 AS sum;"
```

```text
┌───────┐
│  sum  │
│ int32 │
├───────┤
│     2 │
└───────┘
```

## Querying data files

The shell reads files directly through table functions such as `read_csv` and `read_parquet`:

```sh
serened shell :memory: -c "SELECT * FROM read_csv('data.csv');"
```

```text
┌───────┬─────────┐
│  id   │  name   │
│ int64 │ varchar │
├───────┼─────────┤
│     1 │ alpha   │
│     2 │ beta    │
└───────┴─────────┘
```

You can also pipe a script via standard input:

```sh
echo "SELECT 'piped' AS src;" | serened shell
```

## Creating objects

Unlike a server connection, the shell does not pre-select a default database. To create objects, first select a database with `USE` (or fully-qualify the name), otherwise SereneDB reports `Catalog Error: no schema has been selected to create in`:

```sh
serened shell :memory: -c "USE memory; CREATE TABLE t (i INTEGER); INSERT INTO t VALUES (1), (2); SELECT count(*) AS rows FROM t;"
```

```text
┌───────┐
│ rows  │
│ int64 │
├───────┤
│     2 │
└───────┘
```

## Output modes

The default output is the aligned `duckbox` table. Switch modes with a `--MODE` flag (`box`, `csv`, `json`, `markdown`, `line`, …) or with `-P format=…`:

```sh
serened shell :memory: --markdown -c "SELECT 1 AS a, 2 AS b;"
```

```text
| a | b |
|--:|--:|
| 1 | 2 |
```

## Formatting SQL

`--format` pretty-prints SQL read from standard input (use `--format-file` for a file):

```sh
echo "select a,b from t where a>1 order by b" | serened shell --format
```

```text
SELECT a, b
FROM t
WHERE a > 1
ORDER BY b
```

## Options

### General options

| Option                       | Description                                                  |
| :--------------------------- | :----------------------------------------------------------- |
| `-c`, `--command=COMMAND`    | Run a single command (SQL or dot-command) and exit           |
| `-s COMMAND`                 | Alias for `-c`                                               |
| `-f`, `--file=FILENAME`      | Execute commands from a file, then exit                      |
| `--cmd=COMMAND`              | Run a command before reading stdin (does not exit)           |
| `--init=FILENAME`            | Pre-initialize from a file (overrides the default `~/.duckdbrc`) |
| `-X`, `--no-init`            | Do not read the startup file                                 |
| `--bail`                     | Stop after hitting an error                                  |
| `-1`, `--single-transaction` | Wrap the `-c` / `-f` batch in `BEGIN` / `COMMIT`             |
| `--format`                   | Format SQL from stdin and write the result to stdout         |
| `--format-file=FILENAME`     | Format SQL in a file and write the result to stdout          |
| `-V`, `--version`            | Print the SereneDB version and exit                          |
| `-?`, `--help`               | Show the help message and exit                               |

### Input and output options

| Option                    | Description                                              |
| :------------------------ | :------------------------------------------------------- |
| `-a`, `--echo-all`        | Echo all input from the script                           |
| `-e`, `--echo-queries`    | Echo commands before execution                           |
| `-o`, `--output=FILENAME` | Send query results to a file                             |
| `-q`, `--quiet`           | Suppress informational messages                          |
| `--interactive`           | Force interactive input                                  |
| `--batch`                 | Force batch input                                        |
| `--no-stdin`              | Exit after processing options instead of reading stdin   |

### Output format options

| Option                         | Description                                             |
| :----------------------------- | :------------------------------------------------------ |
| `-A`, `--no-align`             | Unaligned output                                        |
| `--csv`                        | CSV output                                              |
| `-H`, `--html`                 | HTML output                                             |
| `-t`, `--tuples-only`          | Print rows only, without column headers                 |
| `-x`, `--expanded`             | Expanded output (one value per line)                    |
| `-F`, `--field-separator=STR`  | Field separator for unaligned output (default `\|`)      |
| `--nullvalue=TEXT`             | Text shown for `NULL` values (default `NULL`)           |
| `--MODE`                       | Set the output mode directly (`box`, `csv`, `json`, `markdown`, `line`, …) |

### Database options

| Option                   | Description                                          |
| :----------------------- | :--------------------------------------------------- |
| `--readonly`             | Open the database read-only                          |
| `--safe`                 | Enable safe-mode                                     |
| `--storage-version=VER`  | Storage compatibility version for new database files |
| `--unsigned`             | Allow loading unsigned extensions                    |
| `--ui`                   | Launch a web interface via the `ui` extension        |

:::note
The flags `-h`, `-l` and `-s` differ from [`serened psql`](./serened-psql.md): in the shell, `-h` shows the help message, `-l` selects list output mode, and `-s` is an alias for `-c`.
:::
