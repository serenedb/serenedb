---
layout: docu
redirect_from:
- /docs/preview/operations_manual/logging/overview
- /docs/stable/operations_manual/logging/overview
- /docs/contribution/logging/overview
- /docs/configuration/logging/overview
title: Logging
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB implements a logging mechanism that provides users with detailed information about events such as query execution, performance metrics and system events.

## Basics

The SereneDB logging mechanism can be enabled or disabled using a special function, `enable_logging`. Logs are exposed through a table function named `sdb_logs`, which can be queried like any standard table while the log is kept in the `memory` or `file` [log storage](#log-storages). The server logs to `stdout` by default, which `sdb_logs` cannot read, so the example switches the storage to `memory`.

Example:

<SqlLogicTest id="configuration/logging/index/example_001" />

To disable logging, run

<SqlLogicTest id="configuration/logging/index/example_002" />

To clear the current log, run

<SqlLogicTest id="configuration/logging/index/example_003" />

## Log Level

SereneDB supports different logging levels that control the verbosity of the logs:

* `FATAL`: Only logs fatal errors
* `ERROR`: Logs errors
* `WARNING`: Logs warnings and errors
* `INFO`: Logs general information, warnings and errors (default)
* `DEBUG`: Logs detailed debugging information
* `TRACE`: Logs very detailed tracing information

Each level also logs the levels above it in this list. Level names are case-insensitive.

The log level can be set using:

<SqlLogicTest id="configuration/logging/index/example_004" />

At the default `INFO` level the log holds the server's timeline and what needs attention. The timeline is a few lines per lifecycle step: `starting SereneDB <version>`, one `listening on …` line per endpoint, a recovery summary when recovery replayed the WAL, the ready line, and at shutdown `received shutdown signal` and `SereneDB is shut down`. The first start of a data directory adds the lines for what it creates, such as the database directory, the initial password from `POSTGRES_PASSWORD` and the OpenTelemetry schema. What needs attention is warnings and errors from background work, such as index refresh and compaction, checkpoints, recovery and cleanup, and failures that no statement reports. Routine events are logged at `DEBUG`, among them per-index recovery when nothing was replayed, the authentication mode, extension loads and credential refreshes.

A warning about a client's own statement, such as a deprecated setting or a file a reader skipped, is sent to that client as a PostgreSQL `WARNING` notice instead of being written to the log.

## Log Types

In SereneDB, log messages can have an associated log type. Log types allow two main things:

* Fine-grained control over log message generation
* Support for structured logging

### Logging-Specific Types

To log only messages of a specific type:

<SqlLogicTest id="configuration/logging/index/example_005" />

The above function will automatically set the correct log level and will add the `HTTP` type to the `enabled_log_types` settings. This ensures only log messages of the `HTTP` type will be written to the log.

To enable multiple log types, simply pass:

<SqlLogicTest id="configuration/logging/index/example_006" />

### Structured Logging

Some log types like `HTTP` will have an associated message schema. To make SereneDB automatically parse the message, use the `sdb_logs_parsed(log_type)` macro, passing the type positionally. For example:

<SqlLogicTest id="configuration/logging/index/example_007" />

To view the schema of each structure log type simply run:

<SqlLogicTest id="configuration/logging/index/example_008" />

### List of Available Log Types

This is a (non-exhaustive) list of the available log types in SereneDB.

| Log Type     | Description                                              | Structured |
|--------------|----------------------------------------------------------|------------|
| `QueryLog`   | Logs which queries are executed in SereneDB              | No         |
| `FileSystem` | Logs all FileSystem interaction with SereneDB's Filesystem | Yes        |
| `HTTP`       | Logs all HTTP traffic from SereneDB's internal HTTP client | Yes        |
| `PostgresQueryLog` | Logs every query sent to a PostgreSQL server, with `query` and `duration_ms` | Yes |
| `ClickHouseQueryLog` | Logs every query sent to a ClickHouse server, with `query` and `duration_ms`; a failed query is logged too | Yes |

### SereneDB Server Log Types

Beyond the core types above, the SereneDB server (`serened`) emits its own log types from its subsystems. These types are not accepted as a filter argument to `enable_logging`, which only recognizes the core types. Instead, enable logging without a type filter and select the subsystem you are interested in through the `type` column of `sdb_logs`, for example `SELECT * FROM sdb_logs WHERE type = 'Search'` to see only search-engine activity.

| Log Type    | Description                                                                                  |
|-------------|----------------------------------------------------------------------------------------------|
| `Startup`   | Server startup and initialization: endpoints, role creation, readiness                       |
| `Search`    | Inverted-index (search engine) activity: background refresh and compaction, index maintenance |
| `Replication` | Logical replication: the apply workers of [subscriptions](../../sql/statements/create_subscription/index.md) |
| `IResearch` | Low-level events from the underlying [IResearch](https://github.com/serenedb/serenedb/tree/main/iresearch) engine |
| `HTTP`      | Errors of the server's own HTTP endpoints, such as a request handler that failed; not HTTP traffic. The name is shared with the core `HTTP` type above |

Server log messages that are not assigned a type fall into the default (empty) type, so they appear in `sdb_logs` with an empty `type` and are always shown when logging is enabled without a type filter.

## Log Storages

By default, SereneDB logs to `stdout`. The `--log_storage` option of `serened` picks the log storage at start (`--log_path` gives the path of `file`) and `--log_level` its level, and `enable_logging(storage = ...)` switches the storage at runtime. SereneDB supports the following log storages:

| Log Storage | Description                                               |
|-------------|-----------------------------------------------------------|
| `memory`    | Log to an in-memory buffer                                |
| `stdout`    | (default) Log to the stdout of the `serened` process, tab-separated |
| `file`      | Log to (a) csv file(s)                                    |


Note that the `sdb_logs` table function is automatically updated to target the currently active log storage. This means that switching the log storage may influence what is returned by the `sdb_logs` function. With `stdout`, `sdb_logs` returns no rows.

### Logging to stdout

<SqlLogicTest id="configuration/logging/index/example_009" />

### Logging to File 

<SqlLogicTest id="configuration/logging/index/example_010" />

or using the equivalent shorthand:

<SqlLogicTest id="configuration/logging/index/example_011" />

## Advanced Usage

### Normalized vs. Denormalized Logging

SereneDB's log storages can log in two ways: normalized vs. denormalized.

In denormalized logging, the log context information is appended directly to each log entry, while in normalized logging the log entries are stored separately with context_ids referencing the context information.

| Log Storage | Normalized   |
|-------------|--------------|
| `memory`    | yes          |
| `file`      | configurable |
| `stdout`    | no           |

For file storage, a path ending in `.csv` writes one denormalized file at that path, and any other path names a directory that receives the normalized `duckdb_log_contexts.csv` and `duckdb_log_entries.csv`. Normalized logging writes each context once instead of on every entry, so its files are smaller. To configure normalization of `file` log storage:

<SqlLogicTest id="configuration/logging/index/example_012" />

Note that the difference between normalized and denormalized is typically hidden from users through `sdb_logs`, which automatically joins normalized tables into a single unified result. To illustrate, both configurations above will be queryable using `FROM sdb_logs;` and will produce identical results.

### Buffer Size

Log storages buffer messages before they write them, so a message reaches the storage some time after it is logged, and the messages logged just before a crash may never be written. The buffer size can be configured as follows:

<SqlLogicTest id="configuration/logging/index/example_013" />

or using the equivalent shorthand:

<SqlLogicTest id="configuration/logging/index/example_014" />

Note that the default buffer size is different for different log storages:

| Log Storage | Default buffer size           |
|-------------|-------------------------------|
| `memory`    | `2048`                        |
| `file`      | `2048`                        |
| `stdout`    | `1` (no buffering)            |

A buffer size of `0` or `1` writes every message as soon as it is logged. To make `stdout` logging faster, enable buffering:

<SqlLogicTest id="configuration/logging/index/example_015" />

To keep the messages logged just before a crash, disable the buffering of the `file` storage:

<SqlLogicTest id="configuration/logging/index/example_016" />

### Syntactic Sugar

SereneDB contains some syntactic sugar to make common paths easier. For example, the following statements are all equal:

<SqlLogicTest id="configuration/logging/index/example_017" />
