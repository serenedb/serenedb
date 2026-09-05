---
title: Pragmas
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

<!-- markdownlint-disable MD001 -->

The `PRAGMA` statement is a SQL extension adopted by SereneDB from SQLite. `PRAGMA` statements can be issued in a similar manner to regular SQL statements. `PRAGMA` commands may alter the internal state of the database engine, and can influence the subsequent execution or behavior of the engine.

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

For queries targeting non-existing schemas, SereneDB generates “did you mean...” style error messages.
When there are thousands of attached databases, these errors can take a long time to generate.
To limit the number of schemas SereneDB looks through, use the `catalog_error_max_schemas` option:

<SqlLogicTest id="configuration/pragmas/example_005" />

#### Table Information

Get info for a specific table:

<SqlLogicTest id="configuration/pragmas/example_006" />

`table_info` returns information about the columns of the table with name `table_name`. The exact format of the table returned is given below:

<SqlLogicTest id="configuration/pragmas/example_007" hideResult />

#### Database Size

Get the file and memory size of each database:

<SqlLogicTest id="configuration/pragmas/example_008" />

`database_size` returns information about the file and memory size of each database. The column types of the returned results are given below:

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
| `stats`        | `VARCHAR` |                                                                                                                                                    |
| `has_updates`  | `BOOLEAN` |                                                                                                                                                    |
| `persistent`   | `BOOLEAN` | `false` if temporary table                                                                                                                         |
| `block_id`     | `BIGINT`  | Empty unless persistent                                                                                                                            |
| `block_offset` | `BIGINT`  | Empty unless persistent                                                                                                                            |

#### Show Databases

The following statement is equivalent to the [`SHOW DATABASES` statement](../sql/statements/attach/index.md):

<SqlLogicTest id="configuration/pragmas/example_011" />

## Resource Management

#### Memory Limit

Set the memory limit for the buffer manager:

<SqlLogicTest id="configuration/pragmas/example_012" />

<DocCallout type="attention">
    The specified memory limit is only applied to the buffer manager. For most queries, the buffer manager handles the majority of the data processed. However, certain in-memory data structures such as vectors and query results are allocated outside of the buffer manager. Additionally, [aggregate functions](../sql/functions/aggregates/index.md) with complex state (e.g., `list`, `mode`, `quantile`, `string_agg`, and `approx` functions) use memory outside of the buffer manager. Therefore, the actual memory consumption can be higher than the specified memory limit.
</DocCallout>

#### Threads

Set the amount of threads for parallel query execution:

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

Show SereneDB version:

<SqlLogicTest id="configuration/pragmas/example_020" />

#### Platform

`platform` returns an identifier for the platform the current SereneDB executable has been compiled for, e.g., `osx_arm64`.
The format of this identifier matches the platform name:

<SqlLogicTest id="configuration/pragmas/example_021" />

#### User Agent

The following statement returns the user agent information, e.g., `duckdb/v0.0.1(linux_arm64) cpp`:

<SqlLogicTest id="configuration/pragmas/example_022" />

#### Metadata Information

The following statement returns information on the metadata store (`block_id`, `total_blocks`, `free_blocks`, and `free_list`):

<SqlLogicTest id="configuration/pragmas/example_023" />

## Progress Bar

Show progress bar when running queries:

<SqlLogicTest id="configuration/pragmas/example_024" />

Or:

<SqlLogicTest id="configuration/pragmas/example_025" />

Don't show a progress bar for running queries:

<SqlLogicTest id="configuration/pragmas/example_026" />

Or:

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

The following query enables profiling with the default format, `query_tree`.
Independent of the format, `enable_profiling` is **mandatory** to enable profiling.

<SqlLogicTest id="configuration/pragmas/example_031" />

### Profiling Coverage

By default, the profiling coverage is set to `SELECT`.
`SELECT` runs the profiler for each operator in the physical plan of a `SELECT` statement.

<SqlLogicTest id="configuration/pragmas/example_032" />

By default, the profiler **does not** emit profiling information for other statement types (`INSERT INTO`, `ATTACH`, etc.).
To run the profiler for all statement types, change this setting to `ALL`.

<SqlLogicTest id="configuration/pragmas/example_033" />

### Profiling Format

The format of `enable_profiling` can be specified as `query_tree`, `json`, `query_tree_optimizer`, or `no_output`.
Each format prints its output to the configured output, except `no_output`.

The default format is `query_tree`.
It prints the physical query plan and the metrics of each operator in the tree.

<SqlLogicTest id="configuration/pragmas/example_034" />

Alternatively, `json` returns the physical query plan as JSON:

<SqlLogicTest id="configuration/pragmas/example_035" />

To return the physical query plan, including optimizer and planner metrics:

<SqlLogicTest id="configuration/pragmas/example_036" />

Database drivers and other applications can also access profiling information through API calls, in which case users can disable any other output.
Even though the parameter reads `no_output`, it is essential to note that this **only** affects printing to the configurable output.
When accessing profiling information through API calls, it is still crucial to enable profiling:

<SqlLogicTest id="configuration/pragmas/example_037" />

### Profiling Output

By default, SereneDB prints profiling information to the standard output.
However, if you prefer to write the profiling information to a file, you can use `PRAGMA` `profiling_output` to specify a filepath.

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

The `disabled_optimizers` option allows selectively disabling optimization steps.
For example, to disable `filter_pushdown` and `statistics_propagation`, run:

<SqlLogicTest id="configuration/pragmas/example_046" />

The available optimizations can be queried using the [`duckdb_optimizers()` table function](../sql/functions/duckdb_table_functions.md#duckdb_optimizers).

To re-enable the optimizers, run:

<SqlLogicTest id="configuration/pragmas/example_047" />

<DocCallout type="attention">
    The `disabled_optimizers` option should only be used for debugging performance issues and should be avoided in production.
</DocCallout>

## Logging

Set a path for query logging:

<SqlLogicTest id="configuration/pragmas/example_048" />

Disable query logging:

<SqlLogicTest id="configuration/pragmas/example_049" />

## Object Cache

Enable caching of objects for e.g., Parquet metadata:

<SqlLogicTest id="configuration/pragmas/example_054" />

Disable caching of objects:

<SqlLogicTest id="configuration/pragmas/example_055" />

## Checkpointing

#### Compression

During checkpointing, the existing column data + any new changes get compressed.
There exist a couple of pragmas to influence which compression functions are considered.

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

By default, SereneDB uses a temporary directory named `⟨database_file_name⟩.tmp`{:.language-sql .highlight} to spill to disk, located in the same directory as the database file. To change this, use:

<SqlLogicTest id="configuration/pragmas/example_061" />

## Returning Errors as JSON

The `errors_as_json` setting makes the [`serened` shell](../clients/serened-shell.md) report errors as raw JSON instead of a formatted message, which is easier to process programmatically. For certain errors it includes extra, decomposed fields:

<SqlLogicTest id="configuration/pragmas/example_062" />

With the setting enabled, a failing query in the shell prints a JSON object such as:

```console
{"exception_type":"Catalog","exception_message":"Table with name nonexistent_tbl does not exist!","type":"Table","name":"nonexistent_tbl","error_subtype":"MISSING_ENTRY"}
```

Over the PostgreSQL wire protocol, errors are always returned as standard PostgreSQL error messages, regardless of this setting.

<DocCallout type="note">

The JSON error format applies to SereneDB's native interface. When connected over the PostgreSQL wire protocol (for example via `psql` or any PostgreSQL driver), errors are always delivered through the standard PostgreSQL error fields, so `errors_as_json` has no visible effect on the client.

</DocCallout>

## IEEE Floating-Point Operation Semantics

SereneDB follows IEEE floating-point operation semantics. If you would like to turn this off, run:

<SqlLogicTest id="configuration/pragmas/example_064" />

In this case, floating point division by zero (e.g., `1.0 / 0.0`, `0.0 / 0.0` and `-1.0 / 0.0`) will all return `NULL`.

## Query Verification (for Development)

The following `PRAGMA`s are mostly used for development and internal testing.

Enable query verification:

<SqlLogicTest id="configuration/pragmas/example_065" />

Disable query verification:

<SqlLogicTest id="configuration/pragmas/example_066" />

Enable force parallel query processing:

<SqlLogicTest id="configuration/pragmas/example_067" />

Disable force parallel query processing:

<SqlLogicTest id="configuration/pragmas/example_068" />

## Block Sizes

When persisting a database to disk, SereneDB writes to a dedicated file containing a list of blocks holding the data.
In the case of a file that only holds very little data, e.g., a small table, the default block size of 256 kB might not be ideal.
Therefore, SereneDB's storage format supports different block sizes.

There are a few constraints on possible block size values.

-   Must be a power of two.
-   Must be greater or equal to 16384 (16 kB).
-   Must be lesser or equal to 262144 (256 kB).

You can set the default block size for all new SereneDB files created by an instance like so:

<SqlLogicTest id="configuration/pragmas/example_069" />

It is also possible to set the block size on a per-file basis, see [`ATTACH`](../sql/statements/attach/index.md) for details.
