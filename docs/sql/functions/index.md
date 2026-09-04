---
title: Overview
redirect_from:
- /docs/sql/functions/overview
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## Function Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## Function Chaining via the Dot Operator

SereneDB supports the dot syntax for function chaining. This allows the function call `fn(arg1, arg2, arg3, ...)` to be rewritten as `arg1.fn(arg2, arg3, ...)`. For example, take the following use of the [`replace` function](../../sql/functions/text.md#replacestring-source-target):

<SqlLogicTest id="sql/functions/index/example_001" />

This can be rewritten as follows:

<SqlLogicTest id="sql/functions/index/example_002" />

### Using with Literals and Arrays

Function chaining also works on literals and on the result of array access. Wrapping the argument in parentheses is optional, but makes the intent explicit:

<SqlLogicTest id="sql/functions/index/example_003" />

<SqlLogicTest id="sql/functions/index/example_004" />

<SqlLogicTest id="sql/functions/index/example_005" />

### Limitations

Function chaining via the dot operator is limited to _scalar_ functions and is not supported for _table_ functions.
For example, the following call returns a `Parser Error`:

<SqlLogicTest id="sql/functions/index/example_006" />

Additionally, the functions `coalesce` and `ifnull` cannot be used with function chaining for the time being:

<SqlLogicTest id="sql/functions/index/example_007" />

## Query Functions

The `duckdb_functions()` table function shows the list of functions currently built into the system.

<SqlLogicTest id="sql/functions/index/example_008" />

In addition to the columns shown above, `duckdb_functions()` exposes the `parameters` and `description` columns, which provide the parameter names and a human-readable description (where available) for each function.
