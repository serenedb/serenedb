---
title: Pragmas
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

<!-- markdownlint-disable MD001 -->

`PRAGMA` statements are issued like regular SQL statements. They read information about the server and its databases, or change settings that influence the subsequent execution or behavior of the engine.

`PRAGMA` statements that assign a value to an option can also be issued using the [`SET` statement](../sql/statements/set/index.md) and the value of an option can be retrieved using `SELECT current_setting(option_name)`.

For SereneDB's built in configuration options, see the [Configuration Reference](../configuration/overview.md#configuration-reference).

This page contains the supported `PRAGMA` settings.

## Metadata

#### Schema Information

List all databases:

<SqlLogicTest id="configuration/pragmas/example_001" />

List all tables:

<SqlLogicTest id="configuration/pragmas/example_002" />

List all tables, with extra information, similarly to [`DESCRIBE`](../cookbook/meta/describe.md):

<SqlLogicTest id="configuration/pragmas/example_003" />

To list all functions:

<SqlLogicTest id="configuration/pragmas/example_004" />

For queries targeting non-existing schemas, SereneDB generates "did you mean..." style error messages. When there are thousands of attached databases, these errors can take a long time to generate. To limit the number of schemas SereneDB looks through, use the `catalog_error_max_schemas` option:

<SqlLogicTest id="configuration/pragmas/example_005" />

#### Table Information

Get info for a specific table:

<SqlLogicTest id="configuration/pragmas/example_006" />

`table_info` returns information about the columns of the table with name `table_name`. The exact format of the table returned is given below:

<SqlLogicTest id="configuration/pragmas/example_007" hideResult />

#### Database Size

Get the file and memory size of each database:

<SqlLogicTest id="configuration/pragmas/example_008" />

`database_size` returns information about the file and memory size of each database. A database whose size cannot be read, such as an attached PostgreSQL database whose server is unreachable, is left out, and the other databases are still listed. The column types of the returned results are given below:

<SqlLogicTest id="configuration/pragmas/example_009" hideResult />

#### Storage Information

To get storage information:

<SqlLogicTest id="configuration/pragmas/example_010" />

This call returns the following information for the given table:

| Name           | Type      | Description                                                                                                                                        |
| -------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `row_group_id` | `BIGINT`  |                                                                                                                                                    |
| `column_name`  | `VARCHAR` |                                                                                                                                                    |
| `column_id`    | `BIGINT`  |                                                                                                                                                    |
| `column_path`  | `VARCHAR` |                                                                                                                                                    |
| `segment_id`   | `BIGINT`  |                                                                                                                                                    |
| `segment_type` | `VARCHAR` |                                                                                                                                                    |
| `start`        | `BIGINT`  | The start row id of this chunk                                                                                                                     |
| `count`        | `BIGINT`  | The amount of entries in this storage chunk                                                                                                        |
| `compression`  | `VARCHAR` | Compression type used for this column                                                                                                              |
| `stats`        | `VARIANT` |                                                                                                                                                    |
| `has_updates`  | `BOOLEAN` |                                                                                                                                                    |
| `persistent`   | `BOOLEAN` | `false` if temporary table                                                                                                                         |
| `block_id`     | `BIGINT`  | Empty unless persistent                                                                                                                            |
| `block_offset` | `BIGINT`  | Empty unless persistent                                                                                                                            |
| `segment_info` | `VARCHAR` | Only from `pragma_storage_info('<table>', include_segment_info := true)` |
| `additional_block_ids` | `BIGINT[]` | Empty unless persistent |

#### Show Databases

The following statement is equivalent to the [`SHOW DATABASES` statement](../sql/statements/show.md#show-databases):

<SqlLogicTest id="configuration/pragmas/example_011" />

## Resource Management

#### Memory Limit

Set the memory limit for the buffer manager:

<SqlLogicTest id="configuration/pragmas/example_012" />

<DocCallout type="attention">
    The specified memory limit is only applied to the buffer manager. For most queries, the buffer manager handles the majority of the data processed. However, certain in-memory data structures such as vectors and query results are allocated outside of the buffer manager. Additionally, [aggregate functions](../sql/functions/aggregates/index.md) with complex state (e.g., `list`, `mode`, `quantile`, `string_agg`, and `approx` functions) use memory outside of the buffer manager. Therefore, the actual memory consumption can be higher than the specified memory limit.
</DocCallout>

#### Threads

Cap the threads the queries of the current session run on:

<SqlLogicTest id="configuration/pragmas/example_013" />

## Collations

List all available collations:

<SqlLogicTest id="configuration/pragmas/example_014" />

Set the default collation to one of the available ones:

<SqlLogicTest id="configuration/pragmas/example_015" />

## Default Ordering for NULLs

Set the default ordering for NULLs to be either `NULLS_FIRST`, `NULLS_LAST`, `NULLS_FIRST_ON_ASC_LAST_ON_DESC` or `NULLS_LAST_ON_ASC_FIRST_ON_DESC`:

<SqlLogicTest id="configuration/pragmas/example_016" />

Set the default result set ordering direction to `ASCENDING` or `DESCENDING`:

<SqlLogicTest id="configuration/pragmas/example_017" />

## Ordering by Non-Integer Literals

By default, ordering by non-integer literals is not allowed:

<SqlLogicTest id="configuration/pragmas/example_018" />

To allow this behavior, use the `order_by_non_integer_literal` option:

<SqlLogicTest id="configuration/pragmas/example_019" />

## Information on SereneDB

#### Version

`PRAGMA version` shows the version of the query engine library built into SereneDB, not the SereneDB release. `SELECT version()` returns the release, for example `PostgreSQL 18.3 (SereneDB 26.10.0)`.

<SqlLogicTest id="configuration/pragmas/example_020" />

#### Platform

`platform` returns an identifier for the platform the current SereneDB executable has been compiled for, `linux_amd64` or `linux_arm64`:

<SqlLogicTest id="configuration/pragmas/example_021" />

#### User Agent

The following statement returns the user agent information, `duckdb/<engine library version>(<platform>) cpp`:

<SqlLogicTest id="configuration/pragmas/example_022" />

#### Metadata Information

The following statement returns information on the metadata store (`block_id`, `total_blocks`, `free_blocks`, and `free_list`):

<SqlLogicTest id="configuration/pragmas/example_023" />

## Progress Bar

A progress bar is printed to the standard output of the `serened` process, not to the client. Every session tracks the progress of its queries, and none prints it by default.

Track the progress of running queries:

<SqlLogicTest id="configuration/pragmas/example_024" />

Print a progress bar for long queries:

<SqlLogicTest id="configuration/pragmas/example_025" />

Stop tracking the progress of running queries:

<SqlLogicTest id="configuration/pragmas/example_026" />

Stop printing the progress bar:

<SqlLogicTest id="configuration/pragmas/example_027" />

## EXPLAIN Output

The output of [`EXPLAIN`](../sql/statements/profiling.md) can be configured to show only the physical plan.

The default configuration of `EXPLAIN`:

<SqlLogicTest id="configuration/pragmas/example_028" />

To only show the optimized query plan:

<SqlLogicTest id="configuration/pragmas/example_029" />

To show all query plans:

<SqlLogicTest id="configuration/pragmas/example_030" />

## Profiling

### Enable Profiling

The following query enables profiling with the default format, `query_tree`. Independent of the format, `enable_profiling` is **mandatory** to enable profiling.

<SqlLogicTest id="configuration/pragmas/example_031" />

### Profiling Coverage

By default, the profiling coverage is set to `SELECT`. `SELECT` runs the profiler for each operator in the physical plan of a `SELECT` statement.

<SqlLogicTest id="configuration/pragmas/example_032" />

By default, the profiler **does not** emit profiling information for other statement types (`INSERT INTO`, `ATTACH`, etc.). To run the profiler for all statement types, change this setting to `ALL`.

<SqlLogicTest id="configuration/pragmas/example_033" />

### Profiling Format

The format of `enable_profiling` can be specified as `query_tree`, `query_tree_optimizer`, `json`, `text`, `html`, `graphviz`, `yaml`, `mermaid`, `compact` or `no_output`. Each format prints its output to the configured output, except `no_output`.

The default format is `query_tree`. It prints the physical query plan and the metrics of each operator in the tree.

<SqlLogicTest id="configuration/pragmas/example_034" />

Alternatively, `json` returns the physical query plan as JSON:

<SqlLogicTest id="configuration/pragmas/example_035" />

To return the physical query plan, including optimizer and planner metrics:

<SqlLogicTest id="configuration/pragmas/example_036" />

Database drivers and other applications can also access profiling information through API calls, in which case users can disable any other output. Even though the parameter reads `no_output`, it is essential to note that this **only** affects printing to the configurable output. When accessing profiling information through API calls, it is still crucial to enable profiling:

<SqlLogicTest id="configuration/pragmas/example_037" />

### Profiling Output

Without `profiling_output`, the profile is printed to the standard error of the `serened` process, and the client receives nothing; [`EXPLAIN ANALYZE`](../sql/statements/profiling.md) returns a profile to the client instead. To write the profile to a file on the server, set `profiling_output` to its path:

<DocCallout type="attention">
    The file contents will be overwritten for every newly issued query. Hence, the file will only contain the profiling information of the last run query:
</DocCallout>

<SqlLogicTest id="configuration/pragmas/example_038" />

### Disable Profiling

To disable profiling:

<SqlLogicTest id="configuration/pragmas/example_043" />

## Query Optimization

#### Optimizer

To disable the query optimizer:

<SqlLogicTest id="configuration/pragmas/example_044" />

To enable the query optimizer:

<SqlLogicTest id="configuration/pragmas/example_045" />

#### Selectively Disabling Optimizers

The `disabled_optimizers` option allows selectively disabling optimization steps. For example, to disable `filter_pushdown` and `statistics_propagation`, run:

<SqlLogicTest id="configuration/pragmas/example_046" />

The available optimizations can be queried using the [`sdb_optimizers()` table function](../sql/functions/metadata.md#sdb_optimizers).

To re-enable the optimizers, run:

<SqlLogicTest id="configuration/pragmas/example_047" />

<DocCallout type="attention">
    The `disabled_optimizers` option should only be used for debugging performance issues and should be avoided in production.
</DocCallout>

## Logging

Log the text of every statement the session runs to a file on the server:

<SqlLogicTest id="configuration/pragmas/example_048" />

Disable query logging:

<SqlLogicTest id="configuration/pragmas/example_049" />

## Checkpointing

#### Compression

During checkpointing, the existing column data + any new changes get compressed. There exist a couple of pragmas to influence which compression functions are considered.

##### Force Compression

Prefer using this compression method over any other method if possible:

<SqlLogicTest id="configuration/pragmas/example_056" />

##### Disabled Compression Methods

Avoid using any of the listed compression methods from the comma separated list:

<SqlLogicTest id="configuration/pragmas/example_057" />

#### Force Checkpoint

When `CHECKPOINT` is called when no changes are made, force a checkpoint regardless:

<SqlLogicTest id="configuration/pragmas/example_058" />

#### Checkpoint on Shutdown

Run a `CHECKPOINT` on successful shutdown and delete the WAL, to leave only a single database file behind:

<SqlLogicTest id="configuration/pragmas/example_059" />

Don't run a `CHECKPOINT` on shutdown:

<SqlLogicTest id="configuration/pragmas/example_060" />

## Temp Directory for Spilling Data to Disk

By default, the server spills to disk in the `tmp` directory of its data directory (`<datadir>/tmp`). To change this, use:

<SqlLogicTest id="configuration/pragmas/example_061" />

## Returning Errors as JSON

The `errors_as_json` setting makes the [`serened` shell](../clients/serened-shell.md) report errors as raw JSON instead of a formatted message, which is easier to process programmatically. For certain errors it includes extra, decomposed fields:

<SqlLogicTest id="configuration/pragmas/example_062" />

With the setting enabled, a failing query in the shell prints a JSON object such as:

```console
{"exception_type":"Catalog","exception_message":"Table with name nonexistent_tbl does not exist!","type":"Table","name":"nonexistent_tbl","error_subtype":"MISSING_ENTRY"}
```

Over the PostgreSQL wire protocol (for example via `psql` or any PostgreSQL driver), errors are always returned as standard PostgreSQL error messages, so `errors_as_json` has no visible effect on the client.

## IEEE Floating-Point Operation Semantics

SereneDB follows IEEE floating-point operation semantics, so a floating-point division by zero returns `Infinity`, `NaN` or `-Infinity`:

<SqlLogicTest id="configuration/pragmas/example_071" />

If you would like to turn this off, run:

<SqlLogicTest id="configuration/pragmas/example_064" />

Then a floating-point division by zero fails, because `error_on_division_by_zero` is on by default; with `error_on_division_by_zero` off, it returns `NULL`:

<SqlLogicTest id="configuration/pragmas/example_072" />

## Query Verification (for Development)

The following `PRAGMA`s are mostly used for development and internal testing.

Enable force parallel query processing:

<SqlLogicTest id="configuration/pragmas/example_067" />

Disable force parallel query processing:

<SqlLogicTest id="configuration/pragmas/example_068" />

## Block Sizes

When persisting a database to disk, SereneDB writes to a dedicated file containing a list of blocks holding the data. In the case of a file that only holds very little data, e.g., a small table, the default block size of 256 kB might not be ideal. Therefore, SereneDB's storage format supports different block sizes.

There are a few constraints on possible block size values.

-   Must be a power of two.
-   Must be greater or equal to 16384 (16 kB).
-   Must be lesser or equal to 262144 (256 kB).

You can set the default block size for all new SereneDB files created by an instance like so:

<SqlLogicTest id="configuration/pragmas/example_069" />

A database can also be created with its own block size, see [`CREATE DATABASE`](../sql/statements/create_database/index.md).
