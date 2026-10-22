---
layout: docu
title: Limits
split: page
---

This page contains SereneDB's built-in limit values. To check the value of a setting on your system, use the `current_setting` function.

## Limit Values

| Limit | Default value | Configuration option | Comment |
|---|---|---|---|
| Array size | 100000 | - | The size `n` of a fixed-size array type `type[n]`; a list (`type[]`) has no such limit. |
| BLOB size | 4 GB | - | |
| Expression depth | 1000 | [`max_expression_depth`](./overview.md) | |
| Memory allocation for a vector | 128 GB | - | |
| Memory use | 80% of RAM | [`memory_limit`](./pragmas.md#memory-limit) | Note: This limit only applies to the buffer manager. |
| Message from a client | 64 MiB | `--pg_max_message_bytes` server flag | One message, such as the text of a statement or a bound parameter value. A larger message closes the connection with `incoming message exceeds maximum size`. `COPY ... FROM STDIN` streams its data and is not limited. |
| Row sent to a client | 2147483647 bytes | - | A larger row fails with `row exceeds maximum allowed message length (2147483647 bytes)`. In text format, a `BLOB` takes about twice its size, as `bytea_output` is `hex`. |
| Search-table write memory | 256 MB | [`segment_memory_max`](../sql/indexes/inverted/maintenance.md#write-memory) | Per table a transaction writes; plus [`checkpoint_threshold`](./overview.md) (16 MB) for a serial write, x threads for a parallel one. |
| String size | 4 GB | - | |
| Temporary directory size | 90% of available disk space | [`max_temp_directory_size`](./overview.md) | Unlimited when the available disk space cannot be read. |

## Size of Database Files

A database file has no size limit of its own: the disk and the file system that hold it set the limit.
