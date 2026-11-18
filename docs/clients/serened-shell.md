---
title: serenedb shell
sidebar_position: 10
split: headings
---

# serenedb shell

`serenedb shell` is a local, embedded SQL shell built into the `serenedb` binary. It runs queries directly against a DuckDB database file — or an in-memory database — **without starting a server or opening a network connection**. It is handy for ad-hoc queries, exploring data files (CSV, Parquet, DuckDB), scripting, and formatting SQL.

To connect to a running SereneDB server instead, use [`serenedb psql`](./serened-psql.md).

## Usage

```sh
serenedb shell [OPTION]... [FILENAME [SQL]]
```

`FILENAME` is the path to a DuckDB database file; a new file is created if it does not exist. Omit it (or pass `:memory:`) for an in-memory database. The optional `SQL` argument is run before the interactive prompt starts.

Open an in-memory shell:

```sh
serenedb shell
```

Open a database file:

```sh
serenedb shell my_data.db
```

## Running a single command

Use `-c` to run one statement and exit:

```sh
serenedb shell :memory: -c "SELECT 1 + 1 AS sum;"
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
serenedb shell :memory: -c "SELECT * FROM read_csv('data.csv');"
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
echo "SELECT 'piped' AS src;" | serenedb shell
```

## SQL dialect

The shell speaks the same SQL as a SereneDB server: `/` between two integers truncates, unquoted identifiers fold to lower case, `COPY ... TO` writes a CSV header only with `HEADER`, `COPY ... FROM STDIN` and `COPY ... TO STDOUT` use the `text` format unless `FORMAT` names another, `SHOW <name>` prints a setting, `EXPLAIN` prints plans in the server's format, and a table that a view reads cannot be dropped without `CASCADE`. The PostgreSQL data types, their casts and the SQL functions that need no server, such as `to_char` and the AI functions, work the same way in both, and so do the settings they read: `extra_float_digits`, `bytea_output`, `xmloption` and the `sdb_ai_*` settings.

What lives in a server's catalog or session needs a server: the `pg_catalog` and `information_schema` objects, roles and privileges, search, jobs, replication, and the parameters of the server and its sessions, such as `statement_timeout`, `DateStyle` and `hba`. Extensions are built in, and the shell never installs or loads one on demand.

## Safe mode

`--safe`, or `.safe_mode` inside a session, turns off everything that reaches outside the engine: file and network access from SQL (reading and writing files, `COPY`, attaching database files, remote URLs), the dot commands `.shell`, `.system`, `.open`, `.read`, `.import`, `.output`, `.once`, `.cd` and `.log`, and the backslash commands `\i`, `\o`, `\g` with a file and `\copy`. It also locks the configuration for the rest of the session. The built-in documentation keeps working, since it is part of the binary, so `serenedb shell --safe -c ".docs <topic>"` gives a script or an agent the docs and nothing else.

To let a session read and write a few locations only, allow them first and switch safe mode on afterwards:

```sql
SET allow_persistent_secrets = false;
SET allowed_directories = ['/data/in/', 'https://example.com/datasets/'];
.safe_mode
```

Files under the listed directories and URL prefixes can be read and written; everything else fails with `file system operations are disabled by configuration`, and the list cannot be widened later. Turning off persistent secrets keeps the first remote read from loading stored secrets, which safe mode would refuse.

## Creating objects

The shell creates objects in the database it opened: the database file named on the command line or, without one, an in-memory database named `postgres`, the name of a server's default database:

```sh
serenedb shell -c "CREATE TABLE t (i INTEGER); INSERT INTO t VALUES (1), (2); SELECT count(*) AS rows FROM t;"
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

The default output is DuckDB's `duckbox` table. `.mode MODE` or a `--MODE` flag switches to another DuckDB mode (`box`, `csv`, `json`, `markdown`, `line`, …):

```sh
serenedb shell :memory: --markdown -c "SELECT 1 AS a, 2 AS b;"
```

```text
| a | b |
|--:|--:|
| 1 | 2 |
```

`.mode psql`, `\pset format aligned` (or `unaligned`) and `-P format=aligned` switch to psql's output, the [output formats of `serened psql`](./serened-psql.md#output-formats), with every value in its PostgreSQL text form:

```sh
serened shell <<'EOF'
.mode psql
SELECT 1 AS id, 'abc' AS name, NULL::VARCHAR AS note, 'line1' || chr(10) || 'line2' AS multi, 3.5 AS num;
EOF
```

```text
 id | name | note | multi | num 
----+------+------+-------+-----
  1 | abc  |      | line1+| 3.5
    |      |      | line2 | 
(1 row)
```

In the DuckDB modes, the psql switches pick the closest DuckDB mode: `-x` and `\x` the `line` mode, `-t` and `\t` the `list` mode without column names, `--csv` and `\pset format csv` the `csv` mode. `-A` switches to psql's unaligned output and `\a` to psql's aligned output.

## Formatting SQL

`--format` pretty-prints SQL read from standard input (use `--format-file` for a file):

```sh
echo "select a,b from t where a>1 order by b" | serenedb shell --format
```

```text
SELECT a, b
FROM t
WHERE a > 1
ORDER BY b
```

## Browsing the documentation

The `.docs` dot-command renders this documentation set inside the shell. The pages are
compiled into the `serenedb` binary, so `.docs` needs no server connection, works
offline, and always matches the version you are running.

With no argument it lists the top-level sections:

```sh
serenedb shell -c ".docs"
```

```text
SereneDB documentation

Use .docs <section> to browse and .docs <name> to look up an object.
Ask anything else, like .docs how do I highlight matches, to search every page.

1. benchmarks (2)
   .docs benchmarks

2. clients (188)
   .docs clients

...

11. sql (2036)
    .docs sql

Open one with .docs <number>.
```

The number after each section is how many pages and headings it holds, so it moves with
the release. A section lists its own pages and its subsections, which end in `/` and
open the same way, so every level stays short enough to pick from.

The argument decides what happens:

| Argument           | Example                        | Result                                                     |
| :----------------- | :----------------------------- | :--------------------------------------------------------- |
| none or `?`        | `.docs`                        | List the top-level sections                                |
| a section          | `.docs cookbook`               | List its pages and subsections                             |
| an object name     | `.docs BM25`                   | Render the object, or list the matches when there are several |
| a call             | `.docs date_trunc('day', ts)`  | The same as its name                                       |
| a number           | `.docs 2`                      | Open that item of the last numbered list                   |
| a page path        | `.docs sql/indexes/index.md`   | Render the page                                            |
| a heading path     | `.docs sql/indexes/index.md#Indexes#Index_Types` | Render that section                       |
| a docs link        | `.docs https://serenedb.com/docs/sql/indexes` | Render the page it points at                |
| anything else      | `.docs how do I highlight matches` | Search every page and list the best matches            |
| `--all NAME`       | `.docs --all date_trunc`       | Render every match in one pass                             |
| `--kind KIND NAME` | `.docs --kind type uuid`       | Only the objects of that kind                              |
| `--kind KIND`      | `.docs --kind setting`         | List every object of that kind                             |
| `--search QUERY`   | `.docs --search bm25`          | Search even when the query is also a name                  |
| `--list PREFIX`    | `.docs --list sql/functions/`  | Print matching paths, one per line                         |

A name is looked up among the objects of the
[object catalog](../sql/functions/docs.md#the-object-catalog), by their names and
aliases, and among the sections and pages of the documentation, by their directory and
file names. So `.docs INT8` reaches `BIGINT`, `.docs threads` reaches the setting,
`.docs functions` lists the pages of the function reference and `.docs geometry` offers
both the data type page and the geometry functions. An object documented in a section
of its own renders that section; one documented as a row of a table, like a data type
or a setting, renders a short card with its summary, its aliases and the path to the
table. A name that is none of these falls back to the entries titled with it.

Names are matched case-insensitively. A name carried by several objects or entries,
like the three `date_trunc` overloads, prints them as a numbered menu, and `.docs 2`
opens the second. `--kind` narrows the menu when a name is both a function and a data
type, and `--all` renders every match in one pass instead, which is what a script
wants. A call copied out of a query, like `date_trunc('day', ts)`, is read as the name
it calls.

A page path works without its `.md`, and a file name alone is enough when only one
page has it, so `.docs scoring.md` opens the scoring page. A link copied from
[serenedb.com/docs](https://serenedb.com/docs/) opens the page it points at, anchor
included, so what you read in the browser is one paste away from the shell.

Anything else is searched, so there is no syntax to learn first. A question, a few
keywords or an error message pasted from the terminal lists the best matching
entries, ranked the way [`--search`](#search-syntax) ranks them, and an operator like
`.docs @@` finds the pages that use it. A single word that looks like a misspelled
name prints the closest names instead and exits non-zero, and so does text that matches
nothing, so `-c '.docs nope'` is scriptable. Those candidates are matched by prefix or
within one edit against entry titles first and then across every field, so a typo like
`.docs vacum` still reaches `VACUUM`.

Every list `.docs` prints is numbered the same way: the top-level sections, the menu of
matches, search results, the closest candidates, the pages of a section, the objects of
a kind and the `Sections` footer of a page. `.docs <number>` opens an item of the last list, and
opening a page makes its own sections the list, so a few numbers walk from a page down
to the heading you want. The list lives for the session, so a number means nothing to
`serenedb shell -c`; use the path printed under each item there.

In an interactive terminal these lists open in the completion menu instead of being
printed, the same menu `Tab` shows while you type SQL. `.docs` prints the heading, the
next prompt starts with `.docs ` and the numbered items sit under it with the first one
highlighted. The arrow keys, `Tab` and `Shift+Tab` move through them, and a list longer
than the screen scrolls to keep the highlighted item in view. Typing a number narrows
the items to the ones it starts, `Enter` opens the highlighted item and `Esc` closes the
menu. `Ctrl+U` clears the line, and `Tab` after `.docs ` brings the list back until the
next `.docs` command.
A page's `Sections` footer stays printed under the page, and scripts, pipes and `-c`
get every list printed.

Rendering a page ends with a `Sections` footer listing its immediate subsections, by
number and as pasteable commands:

```sh
serenedb shell -c ".docs sql/indexes/index.md"
```

```text
path: sql/indexes/index.md#Indexes

# Indexes
...

Sections

1. CREATE INDEX and DROP INDEX
   .docs sql/indexes/index.md#Indexes#CREATE_INDEX_and_DROP_INDEX
2. Index Types
   .docs sql/indexes/index.md#Indexes#Index_Types
3. Persistence
   .docs sql/indexes/index.md#Indexes#Persistence
```

Links are numbered the same way. A link's label carries its number and a `Links` footer
lists them after the `Sections`. `.docs <number>` follows a link to the heading it
points at, and an external link prints its URL instead. In a terminal that supports
hyperlinks the label is also clickable and opens the page on
[serenedb.com/docs](https://serenedb.com/docs/) or the external site.

Otherwise `Tab` after `.docs` lists the top-level sections. After a partial name it
completes the name from the object, section and page names, after `--kind` it completes
the kind, and after a partial path it completes the path.

Output is wrapped to the terminal width, colored only when standard output is a
terminal, and paged the same way query results are. `NO_COLOR=1` or `TERM=dumb` turns
the colors off.

The same command is available in [`serenedb psql`](./serened-psql.md) and renders the
same pages, because both read the same source: a prebuilt inverted index compiled into
the binary. Every `.docs` argument is a query against it, browsing and name lookup as
much as search, so none of them needs a connection, and search ranks with the same
tokenizer and BM25 scorer the server uses, returning what the server returns. The
same copy answers SQL too: the [`sdb_docs` tables and functions](../sql/functions/docs.md)
work in the shell exactly as they do on a server, which is what any other client should
use.

Every build carries the documentation and its index unless it is configured with
`-DSDB_EMBEDDED_DOCS=OFF`. Such a build has no `.docs`: it reports that and exits non-zero.

### Search syntax

A search takes plain text: keywords, a question or a pasted error message. Every word
is matched against the titles, breadcrumbs and text of every page. Entries holding the
whole sentence rank first, and each word also reaches its other forms at a lower
weight, so `index` finds `indexes`. Filler words like `how` or `the` are skipped, and
text that is exactly the name of a documented object puts that object first. Text
with no words at all, like `@@` or `->>`, is matched character by character.

Lucene syntax, the same syntax [`to_tsquery`](../sql/functions/search/full-text.md)
accepts, takes over when the query uses it:

| Syntax            | Example                        | Matches                                  |
| :---------------- | :----------------------------- | :--------------------------------------- |
| `"phrase"`        | `.docs --search \"exact phrase\"` | The words adjacent, in order           |
| `+required`       | `.docs --search +vacuum index` | Entries that must contain `vacuum`       |
| `prefix*`         | `.docs --search tokeniz*`      | `tokenizer`, `tokenizing`, `tokenize`    |
| `term~N`          | `.docs --search vacum~1`       | Within N edits, so typos still match     |
| `AND` `OR` `( )`  | `.docs --search (bm25 OR tfidf) AND rank` | Boolean combinations          |

The shell strips quotes from arguments before the command sees them, so a phrase needs
its quotes escaped: `.docs --search \"exact phrase\"`. A query that uses Lucene syntax
but does not parse, like a pasted SQL statement, is searched as plain words instead.

Exclusion (`-term` or `NOT`) is rejected for now: the documentation is searched as three
separate fields, and an exclusion would only apply to whichever field it matched in,
which silently returns entries it was meant to remove.

## Dot commands

A line that starts with a dot is a command for the shell itself instead of SQL. The rest
of the line holds its arguments, separated by spaces and quoted with `'` or `"` when an
argument contains spaces. `.help` lists the commands, `.help <pattern>` shows the ones
that match and `.help --all` explains each in full. The keys of the line editor are
under [Keyboard shortcuts](#keyboard-shortcuts).

Every command is also an object of kind `command` in the
[object catalog](../sql/functions/docs.md#the-object-catalog), so `.docs .timer` shows
one and `.docs --kind command` lists them all.

| Command | Arguments | Description |
| :--- | :--- | :--- |
| `.about` | | Show information about SereneDB. |
| `.auto_format` | `on\|off` | Format SQL before running it. Off by default. |
| `.bail` | `on\|off` | Stop after the first error. Off by default. |
| `.binary` | `on\|off` | Turn binary output on or off. Off by default. |
| `.cd` | `DIRECTORY` | Change the working directory to `DIRECTORY`. |
| `.changes` | `on\|off` | Show the number of rows changed by each SQL statement. |
| `.columns` | | Render query results column by column. |
| `.databases` | | List the names and files of the attached databases. |
| `.decimal_sep` | `SEP` | Set the decimal separator used to render numbers in `duckbox` mode. |
| `.display_colors` | `[bold\|underline]` | Display every terminal color and its name. |
| `.docs` | `?NAME\|PATH\|NUMBER?` | Show the SereneDB documentation, see [Browsing the documentation](#browsing-the-documentation). |
| `.dump` | `?TABLE?` | Render the database content as SQL. |
| `.echo` | `on\|off` | Turn command echo on or off. |
| `.edit` | | Open an external text editor to edit a query. |
| `.excel` | | Show the output of the next command in a spreadsheet. |
| `.exit` | `?CODE?` | Exit with return code `CODE`. |
| `.headers` | `on\|off` | Turn the display of headers on or off. |
| `.help` | `?--all? ?PATTERN?` | Show help for the commands matching `PATTERN`. |
| `.highlight` | `on\|off` | Turn syntax highlighting on or off. |
| `.highlight_colors` | `OPTIONS` | Configure the highlighting colors. |
| `.highlight_errors` | `on\|off` | Turn the highlighting of errors on or off. |
| `.highlight_mode` | `mixed\|dark\|light` | Switch highlighting between dark and light mode. |
| `.highlight_results` | `on\|off` | Turn the highlighting of results on or off. |
| `.history` | `?N?` | Show the command history with syntax highlighting. |
| `.import` | `FILE TABLE` | Import data from `FILE` into `TABLE`. |
| `.indexes` | `?TABLE?` | Show the names of indexes. |
| `.large_number_rendering` | `MODE` | Toggle readable rendering of large numbers in `duckbox` mode. |
| `.last` | | Render the last result in full, including the expanded plan after `EXPLAIN ANALYZE`. |
| `.log` | `FILE\|off` | Turn logging on or off. `FILE` can be `stderr` or `stdout`. |
| `.maxrows` | `COUNT` | Set the maximum number of rows shown in `duckbox` mode, 40 by default. |
| `.maxwidth` | `COUNT` | Set the maximum width in characters in `duckbox` mode. `0` uses the terminal width. |
| `.mode` | `MODE ?TABLE?` | Set the output mode. |
| `.multiline` | | Render in multi-line mode. |
| `.nullvalue` | `STRING` | Show `STRING` in place of `NULL` values. |
| `.once` | `?FILE?` | Send the output of the next SQL command only to `FILE`. |
| `.open` | `?OPTIONS? ?FILE?` | Close the current database and open `FILE`. |
| `.output` | `?FILE?` | Send output to `FILE`, or to standard output when `FILE` is omitted. |
| `.pager` | `OPTIONS` | Control when output goes through the pager. |
| `.print` | `STRING...` | Print `STRING` literally. |
| `.progress_bar` | `OPTIONS` | Configure the progress bar. |
| `.prompt` | `MAIN CONTINUE` | Replace the standard prompts. |
| `.quit` | | Exit the shell. |
| `.read` | `FILE` | Read input from `FILE`. |
| `.read_line_version` | `linenoise\|fallback` | Choose the library that reads interactive input. |
| `.render_completion` | `on\|off` | Turn the display of completion suggestions on or off. |
| `.render_errors` | `on\|off` | Turn the rendering of errors on or off. |
| `.rows` | | Render query results row by row, the default. |
| `.safe_mode` | | Enable [safe mode](#safe-mode) for the rest of the session. |
| `.schema` | `?PATTERN?` | Show the `CREATE` statements matching `PATTERN`. |
| `.separator` | `COL ?ROW?` | Change the column and row separators. |
| `.shell` | `CMD ARGS...` | Run `CMD ARGS...` in a system shell. |
| `.show` | | Show the current values of the shell settings. |
| `.singleline` | | Render in single-line mode. |
| `.startup_text` | `none\|version\|all` | Choose the text shown at start-up. Set it as the first line of `~/.sdbrc`. |
| `.system` | `CMD ARGS...` | Run `CMD ARGS...` in a system shell. |
| `.tables` | `?TABLE?` | List the tables whose names match the `LIKE` pattern `TABLE`. |
| `.thousand_sep` | `SEP` | Set the thousands separator used to render numbers in `duckbox` mode. |
| `.timer` | `on\|off` | Turn the SQL timer on or off. |
| `.ui_command` | `[command]` | Set the command that opens the web interface. |
| `.version` | | Show the version. |
| `.width` | `NUM1 NUM2 ...` | Set the minimum column widths for columnar output. |

## Backslash commands

A line that starts with `\` holds psql-style commands. `serened shell` and [`serened psql`](./serened-psql.md) take the same ones, and `\?` lists them.

| Command | Arguments | Description |
| :--- | :--- | :--- |
| `\?` | | List the backslash commands. |
| `\q`, `\quit` | | Exit, also from a file that `\i` runs. |
| `\g` | `?FILE?` | Run the statement typed so far, or the last statement again when none is pending. With `FILE` or `\|command`, its result goes there. |
| `\gx` | `?FILE?` | `\g` with expanded output. |
| `\copy` | `...` | `COPY` with its data on the client, see [COPY with stdin and stdout](#copy-with-stdin-and-stdout). |
| `\echo` | `?-n? ?TEXT...?` | Print the text on standard output, followed by a newline unless `-n` comes first. |
| `\i`, `\include` | `FILE` | Run the commands in `FILE`, like `.read`. |
| `\o`, `\out` | `?FILE?` | Send the results of the following statements to `FILE` or `\|command`, or back to standard output without `FILE`, like `.output`. |
| `\l`, `\list` | | List the databases, as `SHOW DATABASES` does. |
| `\a` | | Switch between psql's aligned and unaligned output. |
| `\t` | `?on\|off?` | Print rows only, without column names and footer. |
| `\x` | `?on\|off\|auto?` | Expanded output. |
| `\pset` | `?NAME ?VALUE??` | Set a printing option, see the [output formats of `serened psql`](./serened-psql.md#output-formats). |
| `\timing` | `?on\|off?` | Print the run time of each statement, like `.timer`. |

Without an argument, `\t`, `\x` and `\timing` switch between on and off. A line can hold several commands, as in `\a \t on`, and an argument a command does not take is ignored with a warning. Text in single quotes is one argument, in which `\n` and `\t` stand for a line break and a tab. A command typed between the lines of a statement leaves the statement pending, to run at its semicolon or at `\g`, and a backslash inside a string literal is part of the SQL.

Catalog commands such as `\d` and `\dt` are not supported: `\dt: catalog commands are not supported; use .tables, .schema or DESCRIBE instead`. Any other unknown command fails with `invalid command \name`.

`\x on` and a query, with psql's output:

```sh
serened shell <<'EOF'
.mode psql
\x on
SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(n, s);
EOF
```

```text
Expanded display is on.
-[ RECORD 1 ]
n | 1
s | a
-[ RECORD 2 ]
n | 2
s | b
```

`-q` (`--quiet`) leaves out messages such as `Expanded display is on.`

## COPY with stdin and stdout

`COPY ... FROM STDIN` and `COPY ... TO STDOUT` exchange their data with the shell:

- In a script, run with `-f`, `\i` or `.read` or piped to the shell, `COPY ... FROM STDIN` reads the lines that follow it, up to a line holding only `\.` or the end of the script. When the `COPY` fails, those lines are skipped, not run as statements.
- Typed at the prompt, it asks for the lines, up to a line holding only `\.` or the end of input.
- In a command given with `-c`, it reads the standard input of the shell to its end.
- With `FORMAT binary`, it always reads the standard input of the shell.

`COPY ... TO STDOUT` writes to the output of the shell, in order with the results of the statements around it: the terminal, or the file that `.output`, `\o` or `-o` names.

```sh
serened shell <<'EOF'
CREATE TABLE t (i INTEGER, s VARCHAR);
COPY t FROM STDIN (FORMAT csv);
1,one
2,two
\.
COPY (SELECT * FROM t ORDER BY i) TO STDOUT (FORMAT csv);
EOF
```

```text
1,one
2,two
```

`\copy` runs a `COPY` whose data is on the client, as `\copy TABLE [(COLUMNS)] FROM SOURCE [OPTIONS]`, or `\copy TABLE TO TARGET [OPTIONS]` with a table or a query in parentheses. The source or target is one of:

- a file, as a path or a string in single quotes, which the shell reads or writes;
- `stdin` or `stdout`, the streams of `COPY ... FROM STDIN` and `COPY ... TO STDOUT` above;
- `pstdin` or `pstdout`, the standard input and output of the shell process, whatever script the shell runs and wherever `\o` points.

The options are those of `COPY`, after an optional `WITH`. `PROGRAM` is not supported.

```sh
serened shell <<'EOF'
CREATE TABLE t (i INTEGER, s VARCHAR);
\copy t FROM 'in.csv' (FORMAT csv)
\copy (SELECT s FROM t ORDER BY i) TO 'out.csv' WITH (FORMAT csv)
EOF
```

## Keyboard shortcuts

The interactive line editor takes these keys, which `.help shortcuts` also lists.

| Group | Keys | Action |
| :--- | :--- | :--- |
| Control | `Enter`, `Ctrl+J` | Submit the input |
| Control | `Ctrl+C` | Cancel the input or interrupt a running query |
| Control | `Ctrl+D` | Exit the shell when the line is empty |
| Control | `Ctrl+G` | Submit the input in multi-line mode |
| Control | `Ctrl+L` | Clear the screen |
| Control | `Ctrl+Z` | Suspend the shell |
| Control | `Tab` | Complete the input |
| Control | `Ctrl+Q`, then click | Move the cursor to the clicked position |
| Completion menu | `Tab`, `Shift+Tab`, arrow keys | Move through the suggestions |
| Completion menu | letters, digits, `Backspace` | Edit the input and narrow the suggestions |
| Completion menu | `Enter` | Accept the highlighted suggestion and submit |
| Completion menu | `Esc` | Close the menu and keep the input |
| Editing | `Ctrl+D`, `Delete` | Delete the character under the cursor |
| Editing | `Ctrl+H`, `Backspace` | Delete the character before the cursor |
| Editing | `Ctrl+K` | Delete from the cursor to the end of the line |
| Editing | `Ctrl+U`, `Alt+R` | Delete the entire line |
| Editing | `Ctrl+W`, `Alt+Backspace` | Delete the previous word |
| Editing | `Alt+D` | Delete the next word |
| Editing | `Ctrl+T` | Swap the character under the cursor with the previous one |
| Editing | `Alt+T` | Swap the current word with the previous one |
| Editing | `Alt+C`, `Alt+L`, `Alt+U` | Capitalize, lowercase or uppercase the next word |
| Editing | `Alt+\` | Remove the spaces around the cursor |
| Editing | `Ctrl+X` | Insert a newline in multi-line input |
| Navigation | `Ctrl+A`, `Home` | Go to the beginning of the line |
| Navigation | `Ctrl+E`, `End` | Go to the end of the line |
| Navigation | `Ctrl+B`, `Left` | Move the cursor left |
| Navigation | `Ctrl+F`, `Right` | Move the cursor right |
| Navigation | `Alt+B`, `Alt+Left` | Move the cursor one word left |
| Navigation | `Alt+F`, `Alt+Right` | Move the cursor one word right |
| History | `Ctrl+P`, `Up` | Previous history entry |
| History | `Ctrl+N`, `Down` | Next history entry |
| History | `Ctrl+R` | Search the history backwards |
| History | `Ctrl+S` | Search the history forwards |
| History | `Ctrl+Up` | Jump to the first history entry |
| History | `Ctrl+Down` | Jump to the last history entry |

## Options

### General options

| Option                       | Description                                                  |
| :--------------------------- | :----------------------------------------------------------- |
| `-c`, `--command=COMMAND`    | Run a single command (SQL or dot-command) and exit           |
| `-s COMMAND`                 | Alias for `-c`                                               |
| `-f`, `--file=FILENAME`      | Execute commands from a file, then exit                      |
| `--cmd=COMMAND`              | Run a command before reading stdin (does not exit)           |
| `--init=FILENAME`            | Pre-initialize from a file (overrides the default `~/.sdbrc`) |
| `-X`, `--no-init`            | Do not read the startup file                                 |
| `--bail`                     | Stop after hitting an error                                  |
| `-1`, `--single-transaction` | Wrap the `-c` / `-f` batch in `BEGIN` / `COMMIT`             |
| `--format`                   | Format SQL from stdin and write the result to stdout         |
| `--format-file=FILENAME`     | Format SQL in a file and write the result to stdout          |
| `-l`                         | List the attached databases, then exit                       |
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
| `-A`, `--no-align`             | psql's unaligned output                                 |
| `--csv`                        | CSV output (the `csv` mode)                             |
| `-H`, `--html`                 | HTML output                                             |
| `-t`, `--tuples-only`          | Print rows only, without column names (the `list` mode) |
| `-x`, `--expanded`             | Expanded output, a `column = value` line per value (the `line` mode) |
| `-F`, `--field-separator=STR`  | Field separator for unaligned output (default `\|`)      |
| `-P`, `--pset=VAR[=ARG]`       | Set a printing option, as [`\pset`](./serened-psql.md#output-formats) does; `-P format=aligned` switches to psql's output |
| `--nullvalue=TEXT`             | Text shown for `NULL` values (default `NULL`, empty in psql's output) |
| `--MODE`                       | Set the output mode directly (`box`, `csv`, `json`, `markdown`, `line`, …) |

### Database options

| Option                   | Description                                          |
| :----------------------- | :--------------------------------------------------- |
| `--readonly`             | Open the database read-only                          |
| `--safe`                 | Start in [safe mode](#safe-mode)                     |
| `--storage-version=VER`  | Storage compatibility version for new database files |
| `--unsigned`             | Allow loading unsigned extensions                    |
| `--ui`                   | Launch a web interface via the `ui` extension        |

:::note
The flags `-h`, `-s` and `--list` differ from [`serenedb psql`](./serened-psql.md): in the shell, `-h` shows the help message, `-s` is an alias for `-c`, and `--list` selects the `list` output mode. `-l` lists the databases in both.
:::

### Environment variables

| Variable         | Description                                                                                                     |
| :--------------- | :-------------------------------------------------------------------------------------------------------------- |
| `SDB_PAGER`      | The pager command, ahead of `PAGER`                                                                             |
| `SDB_EDITOR`     | The editor `.edit` opens, ahead of `EDITOR` and `VISUAL`                                                        |
| `SDB_HISTORY`    | The history file of an interactive session (default `~/.sdb_history`)                                           |
| `SDB_AGENT_MODE` | `1` or `0` turns agent mode on or off, `auto` leaves it to detection; `-agent` and `-no-agent` take precedence |
