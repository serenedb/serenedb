---
title: COMMENT ON
draft: true
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `COMMENT ON` statement allows adding metadata to catalog entries (tables, columns, etc.).
It follows the [PostgreSQL syntax](https://www.postgresql.org/docs/16/sql-comment.html).

## Examples

Create a comment on a `TABLE`:

<SqlLogicTest id="sql/statements/comment_on/index/example_001" />


Create a comment on a `COLUMN`:

<SqlLogicTest id="sql/statements/comment_on/index/example_002" />


To unset a comment, set it to `NULL`, e.g.:

<SqlLogicTest id="sql/statements/comment_on/index/example_009" />


Commenting on other catalog objects — views, indexes, sequences, types and macros — is not yet supported and currently returns an error.

Commenting on a `VIEW`:

<SqlLogicTest id="sql/statements/comment_on/index/example_003" />


Commenting on an `INDEX`:

<SqlLogicTest id="sql/statements/comment_on/index/example_004" />


Commenting on a `SEQUENCE`:

<SqlLogicTest id="sql/statements/comment_on/index/example_005" />


Commenting on a `TYPE`:

<SqlLogicTest id="sql/statements/comment_on/index/example_006" />


Commenting on a `MACRO`:

<SqlLogicTest id="sql/statements/comment_on/index/example_007" />


Commenting on a `MACRO TABLE`:

<SqlLogicTest id="sql/statements/comment_on/index/example_008" />


## Reading Comments

Comments can be read by querying the `comment` column of the respective [metadata functions](../../functions/duckdb_table_functions.md):

List comments on `TABLE`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_010" />


List comments on `COLUMN`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_011" />


List comments on `VIEW`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_012" />


List comments on `INDEX`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_013" />


List comments on `SEQUENCE`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_014" />


List comments on `TYPE`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_015" />


List comments on `MACRO`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_016" />


List comments on `MACRO TABLE`s:

<SqlLogicTest id="sql/statements/comment_on/index/example_017" />


## Limitations

The `COMMENT ON` statement currently has the following limitations:

-   Comments are only supported on tables and columns. Comments on views, indexes, sequences, types and macros are not yet supported.
-   It is not possible to comment on schemas or databases.
-   It is not possible to comment on things that have a dependency (e.g., a table with an index).

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />
