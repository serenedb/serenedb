---
title: USE
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `USE` statement sets the default database and/or schema for the current session. Objects referenced without a `database.schema.` qualifier resolve against this default and are created in it. `current_database()` and `current_schema()` report the active selection.

## Examples

Select a database as the default; unqualified names then resolve inside it:

<SqlLogicTest id="sql/statements/use/index/example_001" />

Select a schema within the current database:

<SqlLogicTest id="sql/statements/use/index/example_002" />

Select a database and schema together — `USE shop.analytics` sets the database to `shop` and the schema to `analytics`:

<SqlLogicTest id="sql/statements/use/index/example_003" />

## Relationship to `search_path`

`USE` is a convenience wrapper over the [`search_path`](../set/index.md) session variable: it switches the current database and **replaces `search_path` with the single schema you selected**:

<SqlLogicTest id="sql/statements/use/index/example_004" />

The "default schema" is just the head of `search_path` — what `current_schema()` returns and where unqualified objects are created. Selecting a database alone resets `search_path` to that database's own default schema; to keep several schemas on the path, set [`search_path`](../set/index.md) directly instead of using `USE`.

An unqualified name is looked up in the session's temporary objects first, then among the built-in functions, types and system catalogs (`pg_catalog`), and only then in the schemas of `search_path`, so a function named like a built-in one, or a table named like a `pg_catalog` table or view, does not replace it for sessions that have its schema on the path. A `search_path` that lists `pg_catalog` itself, such as `SET search_path = myschema, pg_catalog`, searches the built-ins at that position instead, as `current_schemas(true)` shows.

The effect of each form on the session:

| Statement | `current_database()` | `current_schema()` | `search_path` |
|---|---|---|---|
| *(session default)* | `postgres` | `public` | `"$user", public` |
| `USE shop` | `shop` | *(shop's default)* | *(shop's default)* |
| `USE analytics` | *(unchanged)* | `analytics` | `analytics` |
| `USE shop.analytics` | `shop` | `analytics` | `analytics` |

<DocCallout type="tip">

A bare `USE name` is resolved schema-first. It looks for a schema with that name in the current database before it looks for a database. So when a database and a schema share a name, `USE` selects the schema and the database is shadowed. 

To select the database instead, qualify it as `USE <database>.<schema>`.

</DocCallout>

## See also

- [CREATE DATABASE](../create_database/index.md) — create a database to `USE`
- [SET](../set/index.md) — set `search_path` (and other variables) directly

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />
