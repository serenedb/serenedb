---
layout: docu
redirect_from:
- /docs/contribution/limits
- /docs/operations_manual/limits
- /docs/preview/operations_manual/limits
- /docs/stable/operations_manual/limits
title: Limits
---

This page contains SereneDB's built-in limit values.
To check the value of a setting on your system, use the `current_setting` function.

## Limit Values

| Limit | Default value | Configuration option | Comment |
|---|---|---|---|
| Array size | 100000 | - | |
| BLOB size | 4 GB | - | |
| Expression depth | 1000 | [`max_expression_depth`](./overview.md) | |
| Memory allocation for a vector | 128 GB | - | |
| Memory use | 80% of RAM | [`memory_limit`](./pragmas.md#memory-limit) | Note: This limit only applies to the buffer manager. |
| String size | 4 GB | - | |
| Temporary directory size | unlimited | [`max_temp_directory_size`](./overview.md) | |

## Size of Database Files

SereneDB doesn't have a practical limit for the size of a single SereneDB database file.
We have database files using 15 TB+ of disk space and they work fine.
However, connecting to such a huge database may take a few seconds and checkpointing can be slower.
