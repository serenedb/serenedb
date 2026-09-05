---
layout: docu
redirect_from:
- /docs/preview/operations_manual/logging/overview
- /docs/stable/operations_manual/logging/overview
- /docs/contribution/logging/overview
- /docs/configuration/logging/overview
title: Logging
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB implements a logging mechanism that provides users with detailed information about events such as query execution,
performance metrics and system events.

## Basics

The SereneDB logging mechanism can be enabled or disabled using a special function, `enable_logging`. Logs are exposed through a
table function named `duckdb_logs`, which can be queried like any standard table.

Example:

<SqlLogicTest id="configuration/logging/index/example_001" />

To disable logging, run

<SqlLogicTest id="configuration/logging/index/example_002" />

To clear the current log, run

<SqlLogicTest id="configuration/logging/index/example_003" />

## Log Level

SereneDB supports different logging levels that control the verbosity of the logs:

* `ERROR`: Only logs error messages
* `WARN`: Logs warnings and errors
* `INFO`: Logs general information, warnings and errors (default)
* `DEBUG`: Logs detailed debugging information
* `TRACE`: Logs very detailed tracing information

The log level can be set using:

<SqlLogicTest id="configuration/logging/index/example_004" />

## Log Types

In SereneDB, log messages can have an associated log type. Log types allow two main things:

* Fine-grained control over log message generation
* Support for structured logging

### Logging-Specific Types

To log only messages of a specific type:

<SqlLogicTest id="configuration/logging/index/example_005" />

The above function will automatically set the correct log level and will add the `HTTP` type to the `enabled_log_types` settings. This ensures
only log messages of the 'HTTP' type will be written to the log.

To enable multiple log types, simply pass:

<SqlLogicTest id="configuration/logging/index/example_006" />

### Structured Logging

Some log types like `HTTP` will have an associated message schema. To make SereneDB automatically parse the message, use the `duckdb_logs_parsed()` macro. For example:

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

### SereneDB Server Log Types

Beyond the core types above, the SereneDB server (`serened`) emits its own log types from its subsystems. These types are not accepted as a filter argument to `enable_logging` (which only recognizes the core types listed earlier). Instead, enable logging without a type filter and select the subsystem you are interested in through the `type` column of `duckdb_logs` — for example `SELECT * FROM duckdb_logs WHERE type = 'Search'` to see only search-engine activity.

| Log Type    | Description                                                                                  |
|-------------|----------------------------------------------------------------------------------------------|
| `Startup`   | Server startup and initialization: endpoints, role creation, readiness                       |
| `Search`    | Inverted-index (search engine) activity: background refresh and compaction, index maintenance |
| `IResearch` | Low-level events from the underlying [IResearch](https://github.com/serenedb/serenedb/tree/main/libs/iresearch) engine |
| `Storage`   | Storage-engine events                                                                        |
| `SSL`       | TLS/SSL configuration and connection events                                                  |
| `HTTP`      | HTTP traffic (shared with the core `HTTP` type above)                                        |

Server log messages that are not assigned a type fall into the default (empty) type, so they appear in `duckdb_logs` with an empty `type` and are always shown when logging is enabled without a type filter.

## Log Storages

By default, SereneDB logs to an in-memory log storage (`memory`). SereneDB supports different types of log storage. Currently,
the following log storage types are implemented in core SereneDB.

| Log Storage | Description                                               |
|-------------|-----------------------------------------------------------|
| `memory`    | (default) Log to an in-memory buffer                      |
| `stdout`    | Log to the stdout of the current process (in CSV format)  |
| `file`      | Log to (a) csv file(s)                                    |


Note that the `duckdb_logs` table function is automatically updated to target the currently active log storage. This means that switching
the log storage may influence what is returned by the `duckdb_logs` function.

### Logging to stdout

<SqlLogicTest id="configuration/logging/index/example_009" />

### Logging to File 

<SqlLogicTest id="configuration/logging/index/example_010" />

or using the equivalent shorthand:

<SqlLogicTest id="configuration/logging/index/example_011" />

## Advanced Usage

### Normalized vs. Denormalized Logging

SereneDB's log storages can log in two ways: normalized vs. denormalized.

In denormalized logging, the log context information is appended directly to each log entry, while in normalized logging
the log entries are stored separately with context_ids referencing the context information.

| Log Storage | Normalized   |
|-------------|--------------|
| `memory`    | yes          |
| `file`      | configurable |
| `stdout`    | no           |

For file storage, you can switch between normalized and denormalized by providing a path ending in .csv (for normalized)
or without .csv (for denormalized). For file logging, denormalized is generally recommended since this increases performance 
and reduces the total size of the logs. To configure normalization of `file` log storage:

<SqlLogicTest id="configuration/logging/index/example_012" />

Note that the difference between normalized and denormalized is typically hidden from users through the 'duckdb_logs' function,
which automatically joins normalized tables into a single unified result. To illustrate, both configurations above will be
queryable using `FROM duckdb_logs;` and will produce identical results.

### Buffer Size

The log storage in SereneDB implements a buffering mechanism to optimize logging performance. This implementation
introduces a potential delay between message logging and storage writing. This delay can obscure the actual message writing time,
which is particularly problematic when debugging crashes, as messages generated immediately before a crash might not be
written. To address this, the buffer size can be configured as follows:

<SqlLogicTest id="configuration/logging/index/example_013" />

or using the equivalent shorthand:

<SqlLogicTest id="configuration/logging/index/example_014" />

Note that the default buffer size is different for different log storages:

| Log Storage | Default buffer size           |
|-------------|-------------------------------|
| `memory`    | `STANDARD_VECTOR_SIZE` (2048) |
| `file`      | `STANDARD_VECTOR_SIZE` (2048) |
| `stdout`    | Disabled (0)                  |

So for example, if you want to increase your `stdout` logging performance, simply enable buffering to greatly (>10x) speed up 
your logging:

<SqlLogicTest id="configuration/logging/index/example_015" />

Or imagine you are debugging a crash in SereneDB and you want to use the `file` logger to understand what's going on:
Simply disable the
buffering using:

<SqlLogicTest id="configuration/logging/index/example_016" />

### Syntactic Sugar

SereneDB contains some syntactic sugar to make common paths easier. For example, the following statements are all equal:

<SqlLogicTest id="configuration/logging/index/example_017" />
