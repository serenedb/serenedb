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

#### `sdb_docs.summary(content)`

Reduces a documentation body to a single-line description: the lead paragraph with
whitespace collapsed and link markup removed, falling back to the `Description` cell
for pages that open with a table. Returns `NULL` for a body with no prose of its own,
such as a heading that only groups other headings.

<SqlLogicTest id="sql/functions/docs/example_009" />

## Reading documentation in `psql`

`sdb_docs.read()` returns Markdown containing newlines, which `psql` boxes into its
aligned output by default. Switch to unaligned, untabulated output to read it:

```sql
\pset format unaligned
\pset tuples_only on
\pset pager always
SELECT sdb_docs.read('sql/indexes/index.md#Indexes');
```
