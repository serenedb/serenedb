---
layout: docu
redirect_from:
- /oom
title: Out-of-Memory Issues
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

### Configuration Options

A statement that needs more memory than `memory_limit` allows fails with an out-of-memory error that names the size it could not get, then the memory in use and the limit, for example `failed to allocate data of size 32.0 KiB (1.2 MiB/976.5 KiB used)`. If you are experiencing out-of-memory issues with SereneDB, try tweaking the following configuration options:

* Reduce the number of threads using the `SET threads = ...` command.
* Raise the memory limit with `SET GLOBAL memory_limit = '...GB'` when the machine has memory to spare. `SET GLOBAL memory_limit` and `RESET GLOBAL memory_limit` work even while the limit is too low for any other statement; a limit below the memory already in use is refused.
* Setting the memory limit lower than the default 80% can help prevent out of memory errors. While this configuration sounds counter-intuitive, it helps because some of SereneDB's operations circumvent the database's buffer manager and thus they can reserve more memory than allowed by the memory limit. If this happens (e.g., SereneDB is killed by the operating system or an OOM reaper process), set the memory limit to just 50-60% of the total system memory by using the `SET memory_limit = '...'` statement. The limit takes an absolute size (e.g., `SET memory_limit = '4GB'`), so compute the value from your system memory — a percentage literal such as `'60%'` is not accepted.
* Writing to, building and merging search and vector indexes counts against the memory limit: a statement such as `CREATE INDEX` or `VACUUM` that needs more fails with an out-of-memory error instead of running past the limit, and background compaction retries at its next interval. Raise the limit for the build, or lower [`segment_memory_max`](../../sql/indexes/inverted/maintenance.md#write-memory) so writes produce smaller segments.
* An `INSERT` or `UPDATE` on an indexed table also reserves the memory its index needs to write the change into its files, and fails before it commits when that doesn't fit; the table and its index stay as they were. See [write memory](../../sql/indexes/inverted/maintenance.md#write-memory) for what a write reserves.
* If your query reads a large amount of data from a file or writes a large amount of data, try setting the `preserve_insertion_order` option to `false`: `SET preserve_insertion_order = false`.
* Keep a temporary directory: with [`SET temp_directory = ''`](../../configuration/overview.md) unused data cannot be offloaded to disk, so statements run out of memory sooner. By default it is the `tmp` directory inside the data directory.

In short, try the settings:

<SqlLogicTest id="cookbook/performance/oom/example_001" />

### Schema and Indexes

If you run in a memory-constrained environment, using smaller data types (e.g., `TINYINT`) can reduce the amount of memory and disk space required to complete a query. SereneDB’s bitpacking compression means small values stored in larger data types will not take up larger sizes on disk, but they will take up more memory during processing – hence, using the most restrictive types possible when creating columns is necessary to reduce memory consumption.

Indexes can significantly increase memory consumption. Additionally, they are [currently not buffer-managed](./indexing.md#indexes-and-memory). Avoid using indexes if possible. If your workload requires indexing large tables, consider reducing the memory limit to accommodate this.
