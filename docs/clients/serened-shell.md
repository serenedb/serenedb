---
title: serened shell
sidebar_position: 10
split: headings
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

## Browsing the documentation

The `.docs` dot-command renders this documentation set inside the shell. The pages are
compiled into the `serened` binary, so `.docs` needs no server connection, works
offline, and always matches the version you are running.

With no argument it lists the top-level sections:

```sh
serened shell -c ".docs"
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
`serened shell -c`; use the path printed under each item there.

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
serened shell -c ".docs sql/indexes/index.md"
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

The same command is available in [`serened psql`](./serened-psql.md) and renders the
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
| `.safe_mode` | | Enable safe mode. |
| `.schema` | `?PATTERN?` | Show the `CREATE` statements matching `PATTERN`. |
| `.separator` | `COL ?ROW?` | Change the column and row separators. |
| `.shell` | `CMD ARGS...` | Run `CMD ARGS...` in a system shell. |
| `.show` | | Show the current values of the shell settings. |
| `.singleline` | | Render in single-line mode. |
| `.startup_text` | `none\|version\|all` | Choose the text shown at start-up. Set it as the first line of `~/.duckdbrc`. |
| `.system` | `CMD ARGS...` | Run `CMD ARGS...` in a system shell. |
| `.tables` | `?TABLE?` | List the tables whose names match the `LIKE` pattern `TABLE`. |
| `.thousand_sep` | `SEP` | Set the thousands separator used to render numbers in `duckbox` mode. |
| `.timer` | `on\|off` | Turn the SQL timer on or off. |
| `.ui_command` | `[command]` | Set the command that opens the web interface. |
| `.version` | | Show the version. |
| `.width` | `NUM1 NUM2 ...` | Set the minimum column widths for columnar output. |

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
