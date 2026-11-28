---
title: CALL
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `CALL` statement invokes the given [table function](../../query_syntax/from_and_join/index.md#table-functions) and returns the results.

`CALL` also runs a procedure created with `CREATE PROCEDURE`, and only `CALL` does: a query that calls a procedure fails with `name() is a procedure`. A function created with `CREATE FUNCTION` is called from a query; `CALL` refuses it with `name() is not a procedure`. When a procedure and a function share a name, the arguments pick the overload and its kind decides.

## Examples

Invoke the 'sdb_functions' table function:

<SqlLogicTest id="sql/statements/call/index/example_001" />


Invoke the 'pragma_table_info' table function:

<SqlLogicTest id="sql/statements/call/index/example_002" />


Select only the functions where the name starts with `ST_`:

<SqlLogicTest id="sql/statements/call/index/example_003" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />
