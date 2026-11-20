---
title: CREATE SCHEMA
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE SCHEMA` statement creates a schema in the catalog. The default schema is `main`.

## Examples

Create a schema:

<SqlLogicTest id="sql/statements/create_schema/index/example_001" />

Create a schema if it does not exist yet:

<SqlLogicTest id="sql/statements/create_schema/index/example_002" />

Create a schema or replace a schema if it exists:

<SqlLogicTest id="sql/statements/create_schema/index/example_003" />

Create table in the schemas:

<SqlLogicTest id="sql/statements/create_schema/index/example_004" />

Compute a join between tables from two schemas:

<SqlLogicTest id="sql/statements/create_schema/index/example_005" />

`AUTHORIZATION role` makes `role` the owner of the new schema; the creating role must be able to `SET ROLE` to it. Without a schema name, the schema is named after the role. `CURRENT_USER`, `CURRENT_ROLE` and `SESSION_USER` name the role the session acts as or logged in as.

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
