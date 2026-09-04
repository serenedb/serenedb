---
title: CREATE SCHEMA
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

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
