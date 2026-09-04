---
title: SET / RESET VARIABLE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports the definition of SQL-level variables using the `SET VARIABLE` and `RESET VARIABLE` statements.

## Variable Scopes

SereneDB supports two levels of variable scopes:

| Scope     | Description                                                                                                                                                                                                                                                                                              |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SESSION` | Variables with a `SESSION` scope are local to you and only affect the current session.                                                                                                                                                                                                                   |
| `GLOBAL`  | Variables with a `GLOBAL` scope are specific [configuration option variables](../../../configuration/overview.md#global-configuration-options) that affect the entire SereneDB instance and all sessions. For example, see [Set a Global Variable](../../statements/set/index.md#set-a-global-variable). |

## `SET VARIABLE`

The `SET VARIABLE` statement assigns a value to a variable, which can be accessed using the `getvariable` call:

<SqlLogicTest id="sql/statements/set_variable/index/example_001" />

If `SET VARIABLE` is invoked on an existing variable, it will overwrite its value:

<SqlLogicTest id="sql/statements/set_variable/index/example_002" />

Variables can have different types:

<SqlLogicTest id="sql/statements/set_variable/index/example_003" />

Variables can also be assigned to results of queries:

<SqlLogicTest id="sql/statements/set_variable/index/example_004" />

If a variable is not set, the `getvariable` function returns `NULL`:

<SqlLogicTest id="sql/statements/set_variable/index/example_005" />

The `getvariable` function can also be used in a [`COLUMNS` expression](../../expressions/star/index.md#columns-expression):

<SqlLogicTest id="sql/statements/set_variable/columns_expression/example_006" />

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

## `RESET VARIABLE`

The `RESET VARIABLE` statement unsets a variable.

<SqlLogicTest id="sql/statements/set_variable/index/example_007" />

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />
