---
title: SET / RESET
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SET` statement modifies the provided SereneDB [configuration option](../../../configuration/overview.md) at the specified scope.

## Examples

Update the `memory_limit` configuration value:

<SqlLogicTest id="sql/statements/set/index/example_001" />

Run the queries of the current session on `1` thread:

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

Size the thread pool of the whole instance to `4` threads:

<SqlLogicTest id="sql/statements/set/index/example_008" />

`GLOBAL` settings persist across the whole instance and outlive the session that set them, so reset them with `RESET GLOBAL` once the override is no longer needed:

<SqlLogicTest id="sql/statements/set/index/example_009" />

<SqlLogicTest id="sql/statements/set/index/example_010" />

### Threads

What `threads` changes depends on the scope it is set in. `SET GLOBAL threads` sizes the pool of worker threads the whole instance runs queries on. A session value (`SET threads`, `SET SESSION threads`) or a transaction value (`SET LOCAL threads`) leaves the pool alone and caps how many of its threads each query of that session or transaction runs on; a cap larger than the pool has no effect until the pool grows. `SHOW threads` returns the cap when one is set and the pool size otherwise. `RESET threads` drops the session's cap, `RESET GLOBAL threads` sizes the pool back to its default.

Run the queries of one transaction on at most `2` threads:

<SqlLogicTest id="sql/statements/set/index/example_011" />

### Async threads

`async_threads` caps how many async tasks, such as reads of remote files, a query runs at once. `-1`, the default, picks the cap automatically: four times the number of CPU cores, at most `256`. `0` runs the I/O of a query on the threads that run the query. It takes the same three scopes as `threads`: `SET GLOBAL async_threads` sets the value for every session that has not set its own, and a session value (`SET async_threads`, `SET SESSION async_threads`) or a transaction value (`SET LOCAL async_threads`) sets it for the queries of that session or transaction. `SHOW async_threads` returns the value in effect. `RESET async_threads` drops the session's value, `RESET GLOBAL async_threads` sets the global value back to `-1`.

Run the I/O of one transaction's queries without async tasks:

<SqlLogicTest id="sql/statements/set/index/example_012" />

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
-   `LOCAL`: Configuration value is used only until the current transaction ends, then the session value returns. `SET LOCAL` outside a transaction block is an error.

When not specified, the default scope for the configuration option is used; the Scope column of the [Configuration Reference](../../../configuration/overview.md#configuration-reference) gives it for each option. The `sdb_` search settings and `integer_division` default to the session. Engine options such as `default_null_order` and `preserve_insertion_order` default to `GLOBAL`, so a plain `SET` changes them for every session on the server; write `SET SESSION` to keep the change to your own. A few options, such as `memory_limit`, exist only globally: `SET SESSION` and `SET LOCAL` refuse them with `option "<name>" cannot be set locally`. `threads` and `async_threads` take all three scopes, see [Threads](#threads) and [Async threads](#async-threads).

## Configuration

See the [Configuration](../../../configuration/overview.md) page for the full list of configuration options.
