---
title: Documentation Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB ships its own documentation inside the server binary. At startup the server
builds the `sdb_docs` schema from it, so the reference material always matches the
running version and is available with no network access.

The schema holds two tables and a set of functions over them:

| Object | Description |
| :--- | :--- |
| `sdb_docs.docs` | One row per documentation page or section, with a full-text index over it. |
| `sdb_docs.meta` | The hash and layout version of the embedded documentation. |

`sdb_docs` is read-only for every user, including superusers. The server rebuilds it
when the embedded documentation or its layout changes; writing to it raises an error.

The functions on this page are the server-side surface, for `psql`, DBeaver, Grafana and
anything else that speaks SQL. [`serened shell` and `serened psql`](../../clients/serened-shell.md#browsing-the-documentation)
carry the same documentation in the client binary and render it with the `.docs`
command, which works without a connection.

## Documentation paths

Every row is addressed by a `path`. A page indexed as a whole is addressed by its file
name, and a page split into sections is addressed by its file name followed by the
chain of headings that leads to the section:

```text
sql/indexes/index.md#Indexes
sql/functions/timestamp.md#Timestamp_Functions#Scalar_Timestamp_Functions#date_trunc(part,_timestamp)
```

Spaces in a heading become underscores and a literal `#` in a heading is escaped as
`\#`, so the unescaped `#` characters count the heading depth. Paths returned by
`sdb_docs.search()`, `sdb_docs.sections()` and `sdb_docs.objects()` can be passed
directly to `sdb_docs.read()`.

## The object catalog

`sdb_docs.objects()` presents the documentation as a catalog of objects rather than a
set of pages. Each row names one documented thing and says what kind of thing it is,
which is what makes a name lookup unambiguous: `create` is documented as a client
method and as part of several statements, but it is not a SQL function, and the
catalog says so.

| Kind | Contents |
| :--- | :--- |
| `function` | Scalar, aggregate, window and table functions. |
| `statement` | SQL statements such as `CREATE TABLE`. |
| `tokenizer` | Text search dictionary templates such as `stem` and `ngram`. |
| `type` | Data types, with their aliases. |
| `setting` | Configuration options. |
| `index_type` | Index access methods: `inverted` and `art`. |

<SqlLogicTest id="sql/functions/docs/example_001" />

Objects are read out of the documentation two ways. A heading whose text is a call
signature — `#### ts_phrase(text, ...)` — becomes a row on its own. So does a row of any
reference table whose first column is headed `Function`, `Aggregate` or `Name`, which is
how the function tables, the data type table and the configuration reference are
indexed. A table that also carries an `Aliases` or `Alias` column fills in the `aliases`
column, so alternative spellings resolve to the same object:

```sql
SELECT name, signature, aliases
FROM sdb_docs.objects()
WHERE kind = 'type' AND name = 'BIGINT';
```

Because a single page can document one function in both a heading and a table, and a
single table can list several overloads, a name repeats. The row is identified by
`(kind, name, path, signature)`; group by `name` when you want one line per object.

## Reference

#### `sdb_docs.objects()`

Returns one row per documented object. `name` is the bare identifier, `signature` is
the full form as documented, and `path` addresses the documentation for that object.
Overloads appear as separate rows sharing a `name`.

<SqlLogicTest id="sql/functions/docs/example_002" />

| Column | Description | Type |
| :--- | :--- | :--- |
| `kind` | The kind of object, from the table above. | `TEXT` |
| `name` | The bare identifier, for example `date_trunc` or `DECIMAL`. | `TEXT` |
| `signature` | The documented form, for example `date_trunc(part, timestamp)`. | `TEXT` |
| `summary` | A single-line description, or `NULL` when the documentation has no lead paragraph. | `TEXT` |
| `aliases` | Alternative names, for data types and settings that have them. | `TEXT` |
| `path` | The documentation path, for `sdb_docs.read()`. | `TEXT` |
| `page` | The file part of `path`. | `TEXT` |
| `category` | For functions, the documentation page they are grouped under. | `TEXT` |
| `breadcrumb` | The chain of headings above the object. | `TEXT` |

Filter it with an ordinary `WHERE` clause:

<SqlLogicTest id="sql/functions/docs/example_003" />

<SqlLogicTest id="sql/functions/docs/example_004" />

#### `sdb_docs.search(query, max_hits)`

Full-text search over titles, breadcrumbs and body text, ranked with BM25. Returns
`path`, `title`, `breadcrumb`, a `snippet` of the first 400 characters and the `score`.

<SqlLogicTest id="sql/functions/docs/example_005" />

#### `sdb_docs.read(path)`

Returns the Markdown source of one page or section, or `NULL` when the path does not
exist.

<SqlLogicTest id="sql/functions/docs/example_006" />

#### `sdb_docs.sections(prefix)`

Lists every documentation row whose path starts with `prefix`, ordered by path. A
directory prefix lists the pages beneath it, and a page or section path lists the
sections beneath that.

<SqlLogicTest id="sql/functions/docs/example_007" />

#### `sdb_docs.reference(name)`

Finds documentation rows whose title is `name` or whose title is `name` applied to
arguments. It matches on title text alone and has no notion of object kind; prefer
`sdb_docs.objects()` when the kind matters.

<SqlLogicTest id="sql/functions/docs/example_008" />

#### `sdb_docs.render(path, width, color)`

Renders a page or section for a terminal: Markdown structure becomes layout rather
than syntax, prose is wrapped to `width`, SQL examples are highlighted, and tables are
drawn with their columns aligned. Returns `NULL` when the path does not exist.

All three arguments are required, because the server has no way to know how wide the
consumer is or whether it understands escape sequences.

<SqlLogicTest id="sql/functions/docs/example_010" />

| Argument | Description |
| :--- | :--- |
| `path` | A documentation path, as returned by `sdb_docs.objects()` or `sdb_docs.search()`. |
| `width` | Column budget for wrapping. `0` means do not wrap, which is what a client that wraps for itself should pass. |
| `color` | `true` emits ANSI escape sequences; `false` emits plain text. |

Code blocks are indented rather than wrapped, so a long SQL line stays intact and the
terminal soft-wraps it. Links keep their label, and a link to another documentation
page is followed by the resolved path so it can be passed straight back to `render()`;
links to anywhere else show their label alone.

#### `sdb_md_to_ansi(markdown, width, color, base_path)`

The renderer behind `sdb_docs.render()`, taking Markdown directly rather than a
documentation path. Useful for rendering Markdown held in your own tables.

`base_path` is the documentation path the Markdown came from, used to resolve relative
links; pass `NULL` when there is nothing to resolve against. `width`, `color` and
`base_path` accept `NULL` and fall back to 80, colored, and no resolution. A negative
`width` is an error.

<SqlLogicTest id="sql/functions/docs/example_011" />

#### `sdb_docs.summary(content)`

Reduces a documentation body to a single-line description: the lead paragraph with
whitespace collapsed and link markup removed, falling back to the `Description` cell
for pages that open with a table. Returns `NULL` for a body with no prose of its own,
such as a heading that only groups other headings.

<SqlLogicTest id="sql/functions/docs/example_009" />

## Reading documentation in `psql`

`sdb_docs.read()` and `sdb_docs.render()` return text containing newlines, which `psql`
boxes into its aligned output by default. Switch to unaligned, untabulated output to
read it:

```sql
\pset format unaligned
\pset tuples_only on
\pset pager always
SELECT sdb_docs.render('sql/indexes/index.md#Indexes', 100, true);
```

Pipe the result through a pager that understands escape sequences, such as `less -R`,
when `color` is `true`.

## Plain output for other clients

DBeaver, Grafana and the HTTP API do not interpret ANSI escape sequences, so pass
`color => false` there. What those clients gain from `render()` is the layout: wrapped
prose, aligned tables and no Markdown syntax. Pass `width => 0` when the client wraps
for itself.
