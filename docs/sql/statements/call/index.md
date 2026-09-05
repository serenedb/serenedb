---
title: CALL
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `CALL` statement invokes the given [table function](../../query_syntax/from_and_join/index.md#table-functions) and returns the results.

## Examples

Invoke the 'duckdb_functions' table function:

<SqlLogicTest id="sql/statements/call/index/example_001" />


Invoke the 'pragma_table_info' table function:

<SqlLogicTest id="sql/statements/call/index/example_002" />


Select only the functions where the name starts with `ST_`:

<SqlLogicTest id="sql/statements/call/index/example_003" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />
