---
title: Documentation Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB ships its own documentation inside the server binary, so the reference
material always matches the running version and is available with no network access.
Two tables and two table functions expose it:

| Object | Description |
| :--- | :--- |
| `sdb_docs.docs` | One row per documentation page or section. |
| `sdb_docs.objects` | One row per documented object, such as a function or a data type. |
| `sdb_docs.search(query[, max_hits])` | Full-text search, ranked with BM25. |
| `sdb_docs.object(name[, kind])` | The documentation of an object, looked up by name. |

`sdb_docs.docs` and `sdb_docs.objects` are not catalog tables. They are read straight
out of the index compiled into the binary, so they do not appear in
`information_schema`, and nothing can be created in, altered in or granted on
`sdb_docs`. The schema name itself is reserved: `CREATE SCHEMA sdb_docs` raises an
error.

The same tables and functions work in `psql`, DBeaver, Grafana and anything else that
speaks SQL, and in [`serened shell` and `serened psql`](../../clients/serened-shell.md#browsing-the-documentation),
which also render the documentation with the `.docs` command. Every surface runs the
same search code over the same index, so a query returns the same pages in the same
order wherever it runs.

## Documentation paths

Every row is addressed by a `path`. A page indexed as a whole is addressed by its file
name, and a page split into sections is addressed by its file name followed by the
chain of headings that leads to the section:

```text
sql/indexes/index.md#Indexes
sql/functions/timestamp.md#Timestamp_Functions#Scalar_Timestamp_Functions#date_trunc(part,_timestamp)
```

Spaces in a heading become underscores and a literal `#` in a heading is escaped as
`\#`, so the unescaped `#` characters count the heading depth, which `sdb_docs.docs`
also returns as `depth`. Paths returned by `sdb_docs.search()`, `sdb_docs.object()`
and `sdb_docs.objects` address rows of `sdb_docs.docs`:

```sql
SELECT content FROM sdb_docs.docs WHERE path = 'sql/indexes/index.md#Indexes';
```

A condition on `path` with `=`, `IN` or `starts_with()` reads only the matching rows out
of the index. Any other condition reads the whole corpus first, which is cheap for the
titles and noticeably slower once `content` or `markdown` is selected.

## The object catalog

`sdb_docs.objects` presents the documentation as a catalog of objects rather than a
set of pages. Each row names one documented thing and says what kind of thing it is,
which is what makes a name lookup unambiguous: `create` is documented as a client
method and as part of several statements, but it is not a SQL function, and the
catalog says so.

| Kind | Contents |
| :--- | :--- |
| `function` | Scalar, aggregate, window and table functions. |
| `statement` | SQL statements such as `CREATE TABLE`. |
| `tokenizer` | The dictionary templates with a page under [`CREATE TEXT SEARCH DICTIONARY`](../statements/create_text_search_dictionary/index.md): `keyword`, `pipeline`, `sql` and `union`. Analysis templates such as `split_text` are functions. |
| `type` | Data types, with their aliases. |
| `setting` | Configuration options. |
| `index_type` | Index access methods: `inverted` and `art`. |
| `command` | [Dot commands](../../clients/serened-shell.md#dot-commands) of the shell such as `.timer`, with the bare word as an alias. |

<SqlLogicTest id="sql/functions/docs/example_001" />

Objects are read out of the documentation two ways. A heading whose text is a call
signature — `#### ts_phrase(text, ...)` — becomes a row on its own. So does a row of a
reference table on one of the pages that carry them, whose first column is headed
`Function`, `Aggregate`, `Name` or `Index` — which is how the function tables, the data
type table, the configuration reference and the index list are indexed. A function
documented both ways yields a single row, keeping its own section's path. A table that also carries an `Aliases` or `Alias` column fills in the `aliases`
column, so alternative spellings resolve to the same object:

```sql
SELECT name, signature, aliases
FROM sdb_docs.objects
WHERE kind = 'type' AND name = 'BIGINT';
```

The catalog is read out once, when the documentation index is built, and ships inside
that index, so querying `sdb_docs.objects` costs about as much as reading a small
table.

Because a single page can document one function in both a heading and a table, and a
single table can list several overloads, a name repeats. The row is identified by
`(kind, name, path, signature)`; group by `name` when you want one line per object.

## Reference

#### `sdb_docs.docs`

The whole corpus, one row per page or section. Query it like any table; the
[paths](#documentation-paths) section says which conditions read only matching rows.

| Column | Description | Type |
| :--- | :--- | :--- |
| `path` | The documentation path. | `TEXT` |
| `title` | The page or heading title. | `TEXT` |
| `breadcrumb` | The chain of headings above this row. | `TEXT` |
| `depth` | The number of headings in `path`: 0 for a whole page, 1 for the title row of a split page. | `INTEGER` |
| `content` | The Markdown source as plain Markdown: site components are removed and callouts such as `:::note` become block quotes that open with their label. | `TEXT` |
| `content_text` | The same body with Markdown markup removed. | `TEXT` |
| `markdown` | `content` opening with the row's title as a heading, once: a body that already starts with one keeps it. This is what `.docs` renders. | `TEXT` |

Read one row by its path:

<SqlLogicTest id="sql/functions/docs/example_006" />

List the rows under a prefix. A directory prefix lists the pages beneath it and a page
or section path lists the sections beneath that; add `depth <= 1` to list only pages,
meaning whole pages and the title rows of split pages:

<SqlLogicTest id="sql/functions/docs/example_007" />

Render a row for a terminal with `sdb_md_to_ansi()`, described below:

<SqlLogicTest id="sql/functions/docs/example_010" />

#### `sdb_docs.objects`

One row per documented object. `name` is the bare identifier, `signature` is the full
form as documented, and `path` addresses the documentation for that object. Overloads
appear as separate rows sharing a `name`.

<SqlLogicTest id="sql/functions/docs/example_002" />

| Column | Description | Type |
| :--- | :--- | :--- |
| `kind` | The kind of object, from the table above. | `TEXT` |
| `name` | The bare identifier, for example `date_trunc` or `DECIMAL`. | `TEXT` |
| `signature` | The documented form, for example `date_trunc(part, timestamp)`. | `TEXT` |
| `summary` | A single-line description, or `NULL` when the documentation has no lead paragraph. | `TEXT` |
| `aliases` | Alternative names, for data types and settings that have them. | `TEXT` |
| `path` | The documentation path, a row of `sdb_docs.docs`. | `TEXT` |
| `page` | The file part of `path`. | `TEXT` |
| `category` | For functions, the documentation page they are grouped under. | `TEXT` |
| `breadcrumb` | The chain of headings above the object. | `TEXT` |

Filter it with an ordinary `WHERE` clause:

<SqlLogicTest id="sql/functions/docs/example_003" />

<SqlLogicTest id="sql/functions/docs/example_004" />

#### `sdb_docs.search(query[, max_hits])`

Full-text search over titles, breadcrumbs and body text, ranked with BM25. Returns
`path`, `title`, `breadcrumb`, a `snippet` of up to 400 characters of the body text,
ending in `…` when it is cut, and the `score`, best match first. Without `max_hits`, or
with `max_hits` set to `NULL`, every match is returned.

`query` takes plain text: keywords, a question or a pasted error message. Every word
is matched against every field. Rows that contain the exact sentence rank first, and
each word also reaches its other forms at a lower weight, so `index` finds `indexes`.
When `query` is exactly the name or alias of a cataloged object, that object comes
first. A query with no words at all, such as `@@`, matches its characters literally.

Lucene syntax, the same syntax [`to_tsquery`](./search/full-text.md) accepts, takes
over when the query uses it:

| Syntax | Example | Matches |
| :--- | :--- | :--- |
| `"phrase"` | `"inverted index"` | The words adjacent, in order |
| `+required` | `+vacuum index` | Rows that must contain `vacuum` |
| `prefix*` | `tokeniz*` | `tokenizer`, `tokenizing`, `tokenize` |
| `term~N` | `vacum~1` | Within N edits, so typos still match |
| `AND` `OR` `( )` | `(bm25 OR tfidf) AND rank` | Boolean combinations |
| `path:` | `path:"configuration/limits.md"` | Rows at that exact path |

A query that uses Lucene syntax but does not parse is searched as plain words instead.

Exclusion (`-term` or `NOT`) is rejected: the documentation is searched as three
separate fields, and an exclusion would only apply to whichever field it matched in,
which silently returns rows it was meant to remove.

<SqlLogicTest id="sql/functions/docs/example_005" />

#### `sdb_docs.object(name[, kind])`

Finds the objects of the catalog documented under `name`, matching the name or one of
its aliases without regard to case, so `INT8` finds `BIGINT`. One name can belong to
several kinds at once, a function and a data type for example, and every one of them is
returned. When no object carries the name, the documentation rows whose title is `name`
or `name` applied to arguments are returned instead.

With `kind`, the lookup is restricted to one kind of object, for when a name is both a
function and a data type and only one of them is wanted. It then never falls back to
titles, so a name with no object of that kind returns no rows:
`SELECT path, title FROM sdb_docs.object('uuid', 'type')`.

<SqlLogicTest id="sql/functions/docs/example_008" />

| Column | Description | Type |
| :--- | :--- | :--- |
| `path` | The documentation path, a row of `sdb_docs.docs`. | `TEXT` |
| `title` | The object's signature, or the title of a row matched by its title. | `TEXT` |
| `breadcrumb` | The chain of headings above the object. | `TEXT` |
| `kind` | The kind of object, or `NULL` for a row matched by its title. | `TEXT` |

#### `sdb_md_to_ansi(markdown, width, color, base_path)`

Renders Markdown for a terminal: Markdown structure becomes layout rather than syntax,
prose is wrapped to `width`, SQL examples are highlighted, and tables are drawn with
their columns aligned. Pass a row's `markdown` and `path` to render documentation, or
any Markdown held in your own tables.

| Argument | Description |
| :--- | :--- |
| `markdown` | The Markdown to render. |
| `width` | Column budget for wrapping. `0` means do not wrap, which is what a client that wraps for itself should pass. |
| `color` | `true` emits ANSI escape sequences; `false` emits plain text. |
| `base_path` | The documentation path the Markdown came from, used to resolve relative links. |

`width`, `color` and `base_path` accept `NULL` and fall back to 80, colored, and no link
resolution. A negative `width` is an error.

Code blocks are indented rather than wrapped, so a long SQL line stays intact and the
terminal soft-wraps it. Links keep their label, and a link to another documentation
page is followed by the resolved path so it can be looked up in `sdb_docs.docs`; links
to anywhere else show their label alone. Nested lists keep their indentation, and table
cells keep their formatting but show a link's label alone so the columns stay narrow.

<SqlLogicTest id="sql/functions/docs/example_011" />

## Reading documentation in `psql`

`content` and rendered output contain newlines, which `psql` boxes into its aligned
output by default. Switch to unaligned, untabulated output to read it:

```sql
\pset format unaligned
\pset tuples_only on
\pset pager always
SELECT sdb_md_to_ansi(markdown, 100, true, path)
FROM sdb_docs.docs WHERE path = 'sql/indexes/index.md#Indexes';
```

Pipe the result through a pager that understands escape sequences, such as `less -R`,
when `color` is `true`.

## Plain output for other clients

DBeaver, Grafana and the HTTP API do not interpret ANSI escape sequences, so pass
`color => false` there. What those clients gain from `sdb_md_to_ansi()` is the layout:
wrapped prose, aligned tables and no Markdown syntax. Pass `width => 0` when the client
wraps for itself.

## Documentation for AI agents over MCP

An HTTP listener with `?api=mcp` serves this documentation to AI agents over the
[Model Context Protocol](https://modelcontextprotocol.io). The endpoint is `POST /_mcp`.
It speaks JSON-RPC 2.0 and accepts protocol versions `2025-06-18`, `2025-03-26` and
`2024-11-05`.

```bash
serened ./data --listen 'postgres://127.0.0.1:7890,http://127.0.0.1:8080?api=mcp'
```

`--listen` takes every endpoint in one comma-separated value. A second `--listen`
replaces the first, so keep the PostgreSQL endpoint in the same list.

Point your agent at `http://127.0.0.1:8080/_mcp`. Auth works like every other HTTP API:
Basic auth against the catalog roles. Every request needs the header, from the local
machine too, and the endpoint answers 401 without it. A role without a password only
works from the local machine. To register the endpoint with Claude Code:

```bash
claude mcp add --transport http serenedb http://127.0.0.1:8080/_mcp \
  --header "Authorization: Basic $(printf 'postgres:' | base64)"
```

The first five tools read the tables and functions above, and `check_sql` checks your
own SQL against the server:

| Tool | Arguments | Returns |
| :--- | :--- | :--- |
| `search_docs` | `query`, optional `limit` (default 5, at most 10) | Ranked sections with a path and a snippet, for keywords, a question or pasted text |
| `read_doc` | `path` | One page or section as Markdown. Links in it become paths `read_doc` takes back, and a very long page is cut after listing its sections |
| `list_docs` | optional `prefix` | `path - title` lines: the pages under a directory or the sections under a page |
| `list_objects` | optional `kind` | One line per object with its signature, kind and summary |
| `describe_object` | `name`, optional `kind` | Everything documented under a name or an alias: the section about it, or its summary when a reference table row documents it. A function or setting the docs miss comes from the server's own catalog |
| `check_sql` | `sql` | The plan of one statement or the server's error with its hint |

`check_sql` only plans the statement with `EXPLAIN` and never runs it. It refuses more
than one statement and `EXPLAIN ANALYZE`. Tables resolve in the database the listener
serves, so the plan shows whether an inverted index serves a predicate.

Check that the endpoint answers:

```bash
curl -s -u postgres: http://127.0.0.1:8080/_mcp -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```
