---
title: CREATE SUBSCRIPTION
split: headings
---

import DocCallout from "@site/src/components/DocCallout";

A **subscription** makes SereneDB a logical replication subscriber of a PostgreSQL publisher. SereneDB connects to the publisher with the logical replication protocol (`pgoutput`), copies the existing rows of the published tables, and then applies every committed `INSERT`, `UPDATE`, `DELETE` and `TRUNCATE` to local tables of the same name, the same way one PostgreSQL server subscribes to another.

```sql
-- On the PostgreSQL publisher (wal_level = logical):
CREATE TABLE orders (id integer PRIMARY KEY, customer text, total numeric(10, 2));
CREATE PUBLICATION shop_pub FOR TABLE orders;

-- On SereneDB:
CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total DECIMAL(10, 2));
CREATE SUBSCRIPTION shop_sub
  CONNECTION 'host=pg.internal port=5432 dbname=shop user=replicator password=secret'
  PUBLICATION shop_pub;
```

Subscriptions belong to the database they are created in, and rows are applied to tables of that database.

## `CREATE SUBSCRIPTION`

```sql
CREATE SUBSCRIPTION name
  CONNECTION 'conninfo'
  PUBLICATION publication_name [, ...]
  [ WITH ( option [= value] [, ...] ) ]
```

`CONNECTION` is a libpq connection string, in keyword/value or URI form (`postgresql://user:secret@pg.internal:5432/shop?sslmode=require`), parsed the way libpq parses it. SereneDB uses `host` (or `hostaddr`, a comma-separated list is tried in order), `port`, `user`, `password`, `dbname`, `application_name`, `connect_timeout`, `sslmode`, `sslrootcert`, `sslcert`, `sslkey` and `sslsni`. `user` defaults to the name of the subscription's owner and `application_name` to the subscription name.

The publisher may require `trust`, `password`, `md5` or `scram-sha-256` authentication. With `sslmode` `prefer` (the default), `require`, `verify-ca` or `verify-full` the connection uses TLS when the publisher offers it; `verify-ca` and `verify-full` check the publisher's certificate against `sslrootcert` (`system` means the system trust store), and `verify-full` also checks the host name.

`CREATE SUBSCRIPTION` connects to the publisher, reads the tables of its publications, and creates the replication slot before it returns, the same way PostgreSQL does. A publication that does not exist on the publisher is reported as a warning. Every published table must already exist locally under the same schema and table name, otherwise the statement fails; columns are matched by name.

### Options

| Option | Default | Meaning |
|---|---|---|
| `connect` | `true` | `false` creates the subscription without contacting the publisher, so it has no tables until `ALTER SUBSCRIPTION ... REFRESH PUBLICATION`. It also sets `enabled`, `create_slot` and `copy_data` to `false`, and asking for any of them to be `true` alongside it is an error. |
| `enabled` | `true` | Start replicating as soon as the subscription is committed. |
| `create_slot` | `true` | Create the replication slot on the publisher. With `false`, the slot must already exist. |
| `slot_name` | the subscription name | Name of the publisher's replication slot. `NONE` means no slot, and requires `enabled = false` and `create_slot = false`. |
| `copy_data` | `true` | Copy the rows that already exist in the published tables before streaming changes. Row filters and column lists of the publications are honored. |
| `binary` | `false` | Ask the publisher to send values in binary format. |
| `origin` | `any` | `none` asks the publisher to send only changes that did not themselves arrive through replication. |
| `disable_on_error` | `false` | Disable the subscription when applying a change fails, instead of reconnecting and retrying. |
| `password_required` | `true` | `false` lets a subscription connect without a password; only a superuser may set it. |
| `streaming` | `parallel` | `on` or `parallel` lets the publisher stream large transactions before they commit; SereneDB spools them and applies each one when it commits. `parallel` behaves like `on`. `off` makes the publisher hold a transaction until it commits. Needs a PostgreSQL 14 or later publisher. |
| `run_as_owner` | `false` | `false` applies the changes to each table as the table's owner, which the subscription owner must be able to `SET ROLE` to. `true` applies them as the subscription owner. |
| `failover` | `false` | Create the slot as a failover slot (PostgreSQL 17 or later publisher). |
| `synchronous_commit` | `off` | Stored for compatibility; every applied transaction is durable before its position is reported to the publisher. |
| `two_phase` | `false` | Only `false` is accepted. |

Creating a subscription requires `CREATE` on the current database. A non-superuser must put a password in `CONNECTION` unless `password_required` is `false`. When `create_slot` is `true`, `CREATE SUBSCRIPTION` cannot run inside a transaction block, because a replication slot on the publisher cannot be rolled back.

## `ALTER SUBSCRIPTION`

```sql
ALTER SUBSCRIPTION name ENABLE
ALTER SUBSCRIPTION name DISABLE
ALTER SUBSCRIPTION name CONNECTION 'conninfo'
ALTER SUBSCRIPTION name SET PUBLICATION publication_name [, ...] [ WITH ( refresh = bool ) ]
ALTER SUBSCRIPTION name ADD PUBLICATION publication_name [, ...] [ WITH ( refresh = bool ) ]
ALTER SUBSCRIPTION name DROP PUBLICATION publication_name [, ...] [ WITH ( refresh = bool ) ]
ALTER SUBSCRIPTION name REFRESH PUBLICATION
ALTER SUBSCRIPTION name SET ( option = value [, ...] )
ALTER SUBSCRIPTION name SKIP ( lsn = 'X/Y' | NONE )
ALTER SUBSCRIPTION name RENAME TO new_name
ALTER SUBSCRIPTION name OWNER TO new_owner
```

Only the owner of a subscription may alter it. Every change takes effect when the transaction commits: a running subscription reconnects with the new settings, a disabled one stays disconnected until `ENABLE`.

- `SET (...)` accepts `slot_name`, `binary`, `streaming`, `origin`, `disable_on_error`, `password_required`, `run_as_owner`, `failover` and `synchronous_commit`. `slot_name = NONE` is only allowed on a disabled subscription. `failover` is only allowed on a disabled subscription with a slot, and changes the slot on the publisher.
- `SET`, `ADD` and `DROP PUBLICATION` change the publications the subscription asks for. With the default `refresh = true` the subscription must be enabled, the statement cannot run inside a transaction block, and the tables are refreshed as by `REFRESH PUBLICATION`; `refresh = false` only records the change. A subscription keeps at least one publication.
- `REFRESH PUBLICATION [ WITH ( copy_data = bool ) ]` reads the tables of the publications again. Tables that are new to the subscription are copied (unless `copy_data = false`) and then replicated; tables no longer published stop being replicated.
- `SKIP (lsn = 'X/Y')` makes the subscription skip the remote transaction that finishes at that LSN, for example one that fails to apply because of a constraint violation. The LSN must be past the subscription's current position, and only a superuser may set it. `NONE` clears it.
- `OWNER TO` requires being able to `SET ROLE` to the new owner.

## `DROP SUBSCRIPTION`

```sql
DROP SUBSCRIPTION [ IF EXISTS ] name [ CASCADE | RESTRICT ]
```

Dropping a subscription stops its apply worker and drops its replication slot on the publisher before the statement returns. When the subscription has a slot, `DROP SUBSCRIPTION` cannot run inside a transaction block, and it fails if the publisher cannot be reached. Use `ALTER SUBSCRIPTION ... DISABLE` and then `ALTER SUBSCRIPTION ... SET (slot_name = NONE)` first to drop it without touching the publisher, for example when the publisher is gone.

A database that has subscriptions cannot be dropped, and a role that owns a subscription cannot be dropped.

## Applying changes

Each subscription has one apply worker. When it starts, it first copies the tables that are not synchronized yet: it opens one snapshot on the publisher, copies every such table from it in one local transaction, and records the publisher position of that snapshot with each table. Changes to those tables up to that position are already in the copy, so the stream skips them. Then the worker streams changes.

Remote transactions are applied atomically: other sessions see all of a remote transaction or none of it. While the worker is catching up, it applies consecutive remote transactions in one local transaction (up to 1000 of them or 100 ms) and commits when it runs out of received changes, so under load several remote transactions become visible together and a durable commit is paid once per group instead of once per remote transaction. Changes of the same kind to the same table are applied as one batched statement, the way `COPY` loads rows, without building SQL per row; within a group, changes to tables that are not linked by foreign keys are batched per table.

Every applied remote transaction records the publisher position it reached, its LSN, in the same commit as its rows. The position is reported to the publisher only after that commit is durable, and after a restart or a crash the subscription resumes from the recorded position. A remote transaction is therefore applied exactly once: never lost, never applied twice. A transaction streamed while still in progress is kept in a spill buffer that goes to disk when it outgrows memory; it is applied when it commits and thrown away, or partly thrown away for a rolled-back subtransaction, when it aborts.

Conflicts follow PostgreSQL: an `UPDATE` or `DELETE` whose row does not exist locally is skipped and counted as `update_missing` or `delete_missing`; an `INSERT` or `UPDATE` that hits an existing unique key fails the apply as `insert_exists` or `update_exists`. When applying fails, the worker reconnects after `wal_retrieve_retry_interval` (5 seconds by default) and resumes from the recorded position, unless `disable_on_error` is set, in which case the subscription is disabled. A write conflict with a concurrent local transaction is retried right away.

A partitioned table on the publisher is replicated the way its publication sends it. With `publish_via_partition_root = true` its changes and its initial copy arrive under the name of the partitioned table, so create one local table with that name. Otherwise they arrive under the names of its partitions, so create a local table for each partition.

Generated columns of the publisher are sent only by a publication with `publish_generated_columns = stored`. Their values land in plain local columns of the same name. A local generated column cannot take published values: the subscription fails with `logical replication target relation "..." has incompatible generated column`. A local generated column that the publication does not send is computed locally.

A remote `TRUNCATE` is applied like PostgreSQL applies it: `CASCADE` also truncates the local tables that reference the truncated ones through foreign keys, and `RESTART IDENTITY` restarts the sequences their columns own.

The worker reports its position to the publisher every `wal_receiver_status_interval` (10 seconds by default). When the publisher sends nothing for `wal_receiver_timeout` (60 seconds by default), the worker first asks it for a reply halfway through and then drops the connection with `terminating logical replication worker due to timeout` and reconnects. The settings are described in [Configuration](../../../configuration/overview.md#logical-replication).

<DocCallout type="attention">

Local tables are not created for you. Create every published table locally before creating the subscription, with the same name and column names, and with types that can hold the published values.

</DocCallout>

## Monitoring

| Relation | Shows |
|---|---|
| `pg_subscription` | Every subscription of the current database and its settings. `subskiplsn` is the pending `SKIP` position. |
| `pg_subscription_rel` | Every table of a subscription with its state: `i` while waiting for its initial copy, `r` once it is replicated, and in `srsublsn` the publisher position of its copy. |
| `pg_stat_subscription` | One row per connected apply worker, with `received_lsn` (the latest publisher position received), `latest_end_lsn` (the latest position durably applied), and the times of the last message sent by the publisher, received, and reported back. |
| `pg_stat_subscription_stats` | Per subscription, `apply_error_count`, `sync_error_count` and the `confl_*` conflict counters since the server started. |
| `pg_replication_origin`, `pg_replication_origin_status` | One origin `pg_<subscription oid>` per subscription, with the publisher position it has durably applied in `remote_lsn`. |

```sql
SELECT subname, received_lsn, latest_end_lsn, last_msg_receipt_time FROM pg_stat_subscription;
```

## Limitations

- Prepared transactions (`two_phase`) are not replicated as such; they are applied when they commit.
- A change that violates a local constraint other than a unique key fails the apply, and the worker retries it until the conflict is fixed locally or the transaction is skipped with `SKIP`.
- Changes are applied to the local table with the published table's schema and name; there is no routing of rows into local partitions.
- `pg_subscription_rel` shows only the states `i` and `r`, because all pending tables are copied in one snapshot; `pg_stat_subscription` has no table synchronization or parallel apply rows.
- The counters in `pg_stat_subscription_stats` are kept in memory and start from zero when the server restarts; `pg_stat_reset_subscription_stats()` is not available, and `confl_update_origin_differs`, `confl_delete_origin_differs` and `confl_multiple_unique_conflicts` stay zero.
- The replication origin functions (`pg_replication_origin_advance()` and the like) are not available, and `local_lsn` in `pg_replication_origin_status` is always `0/0`.
