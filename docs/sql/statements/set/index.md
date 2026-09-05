---
title: SET / RESET
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SET` statement modifies the provided SereneDB [configuration option](../../../configuration/overview.md) at the specified scope.

## Examples

Update the `memory_limit` configuration value:

<SqlLogicTest id="sql/statements/set/index/example_001" />

Configure the system to use `1` thread:

<SqlLogicTest id="sql/statements/set/index/example_002" />

Or use the `TO` keyword:

<SqlLogicTest id="sql/statements/set/index/example_003" />

Change configuration option to default value:

<SqlLogicTest id="sql/statements/set/index/example_004" />

Retrieve configuration value:

<SqlLogicTest id="sql/statements/set/index/example_005" />

Set the default collation for the session:

<SqlLogicTest id="sql/statements/set/index/example_006" />

### Set a Global Variable

Set the default sort order globally:

<SqlLogicTest id="sql/statements/set/index/example_007" />

Set the default threads globally:

<SqlLogicTest id="sql/statements/set/index/example_008" />

`GLOBAL` settings persist across the whole instance and outlive the session that set them, so reset them with `RESET GLOBAL` once the override is no longer needed:

<SqlLogicTest id="sql/statements/set/index/example_009" />

<SqlLogicTest id="sql/statements/set/index/example_010" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

`SET` updates a SereneDB configuration option to the provided value.

## `RESET`

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

The `RESET` statement changes the given SereneDB configuration option to the default value.

## Scopes

Configuration options can have different scopes:

-   `GLOBAL`: Configuration value is used (or reset) across the entire SereneDB instance.
-   `SESSION`: Configuration value is used (or reset) only for the current session attached to a SereneDB instance.
-   `LOCAL`: Not yet implemented.

When not specified, the default scope for the configuration option is used. For most options this is `GLOBAL`.

## Configuration

See the [Configuration](../../../configuration/overview.md) page for the full list of configuration options.
