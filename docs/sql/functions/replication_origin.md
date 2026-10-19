---
title: Replication Origin Functions
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

A replication origin records how far changes from some remote source have been applied, as the LSN of that source. Every [subscription](../statements/create_subscription/index.md) has one, named `pg_<subscription oid>`, and you can create your own to track any other replication you run. Origins belong to the whole server, like roles, and are listed in `pg_replication_origin`, with their progress in `pg_replication_origin_status`. The progress is stored durably: it commits with the transaction that set it.

The functions work as in PostgreSQL, and only superusers may call them.

| Function | Description |
| :------- | :---------- |
| `pg_replication_origin_create(node_name)` | Creates an origin and returns its oid. Names `any`, `none` and names starting with `pg_` are reserved. |
| `pg_replication_origin_drop(node_name)` | Drops an origin. A subscription's origin goes away with the subscription. |
| `pg_replication_origin_oid(node_name)` | Returns the oid of an origin, or `NULL` when there is none. |
| `pg_replication_origin_progress(node_name, flush)` | Returns the LSN up to which the origin has applied, or `NULL` before it has any. Progress is always durable, so `flush` makes no difference. |
| `pg_replication_origin_advance(node_name, lsn)` | Sets the origin's progress to `lsn`, also backwards. The origin must not be in use by a session or by a running subscription. |
| `pg_replication_origin_session_setup(node_name [, pid])` | Makes the origin the current session's origin. With `pid`, shares the origin that session `pid` has set up. |
| `pg_replication_origin_session_reset()` | Releases the current session's origin. |
| `pg_replication_origin_session_is_setup()` | Whether the current session has an origin. |
| `pg_replication_origin_session_progress(flush)` | The progress of the current session's origin. |
| `pg_replication_origin_xact_setup(origin_lsn, origin_timestamp)` | Makes the current transaction advance the session's origin to `origin_lsn` when it commits. |
| `pg_replication_origin_xact_reset()` | Undoes `pg_replication_origin_xact_setup()` in the current transaction. |

#### Creating and advancing an origin

<SqlLogicTest id="sql/functions/replication_origin/example_001" />

#### Recording progress with the applied rows

A session that applies changes from a source sets up the origin once, then marks each transaction with the source position it applies. The position commits together with the rows, so after a crash the origin shows exactly what was applied.

<SqlLogicTest id="sql/functions/replication_origin/example_002" />

`local_lsn` in `pg_replication_origin_status` is always `0/0`. Marking a transaction with an origin does not filter its changes from anything, since SereneDB does not publish changes.
