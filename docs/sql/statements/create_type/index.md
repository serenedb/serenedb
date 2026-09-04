---
title: CREATE TYPE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE TYPE` statement defines a new type in the catalog.

## Examples

Create a simple `ENUM` type:

<SqlLogicTest id="sql/statements/create_type/index/example_001" />

Create a simple `STRUCT` type:

<SqlLogicTest id="sql/statements/create_type/index/example_002" />

Create a simple `UNION` type:

<SqlLogicTest id="sql/statements/create_type/index/example_003" />

Create a type alias:

<SqlLogicTest id="sql/statements/create_type/index/example_004" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

The `CREATE TYPE` clause defines a new data type available to this SereneDB instance.
These new types can then be inspected in the [`duckdb_types` table](../../functions/duckdb_table_functions.md#duckdb_types).

## Limitations

-   Extending types to support custom operators (such as the PostgreSQL `&&` operator) is not possible via plain SQL.

-   The `CREATE TYPE` clause does not support the `OR REPLACE` modifier.
