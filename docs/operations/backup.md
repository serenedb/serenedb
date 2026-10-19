---
title: Backup and restore
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

SereneDB takes a consistent backup of a running server with the PostgreSQL functions `pg_backup_start` and `pg_backup_stop`. Between the two calls you copy the whole data directory with any file copy tool, or let [pgBackRest](#pgbackrest) do it. The server keeps accepting reads and writes the whole time. The backup contains every transaction that committed before `pg_backup_start` returned and none that committed after.

## Taking a backup

1. Connect as a superuser over the PostgreSQL protocol and start the backup:

   <SqlLogicTest id="operations/backup/start" />

2. Copy the entire data directory, every file and subdirectory, while that session stays open. Files can be copied in any order, and files that change during the copy are fine.

3. In the same session, stop the backup:

   <SqlLogicTest id="operations/backup/stop" hideResult />

4. Store the `labelfile` text next to the copy. A restore needs it.

The backup belongs to the session that started it. `pg_backup_stop` must run in that session, and if the session ends first, the server aborts the backup and logs a warning: the copy taken so far cannot be restored. If `pg_backup_stop` fails, the copy cannot be restored either.

Only one backup runs at a time. A second `pg_backup_start` fails with `a backup is already in progress` and names the session that holds it.

## Restoring

1. Put the copied files into an empty data directory, keeping their layout.
2. Write the `labelfile` text as the file `backup_label` in the top level of that directory.
3. Start the server on it.

At startup the server sees `backup_label`, brings the copy to the state it had when `pg_backup_start` returned, and renames the label to `backup_label.old`. If the server stops while doing so, start it again: the restore resumes.

<DocCallout type="attention">
Never start a copy without its `backup_label`. The server cannot tell such a copy apart from a data directory that crashed, and may refuse to start or start on inconsistent data.
</DocCallout>

## Functions

| Function | Returns | Description |
|---|---|---|
| `pg_backup_start(label text, fast boolean DEFAULT false)` | `pg_lsn` | Starts a backup and returns its start position. `label` is written into the backup label. |
| `pg_backup_stop(wait_for_archive boolean DEFAULT true)` | `lsn pg_lsn, labelfile text, spcmapfile text` | Stops the backup started in this session and returns the backup label. `spcmapfile` is always empty. |
| `pg_walfile_name(pg_lsn)` | `text` | The PostgreSQL WAL file name of a position, as backup tools expect it. |

Arguments can be passed by position or by name:

<SqlLogicTest id="operations/backup/named" />

<SqlLogicTest id="operations/backup/stop_named" hideResult />

Differences from PostgreSQL:

- One backup at a time, owned by the session that started it.
- `fast` and `wait_for_archive` are accepted and ignored: `pg_backup_start` does not start a checkpoint, and there is no WAL archiving.
- `pg_backup_start` waits for the transactions that are committing at that moment to finish, and holds new commits back while it records the backup position.
- Both functions require a superuser. `pg_backup_start` also requires a PostgreSQL-protocol connection.

## During a backup

Queries and writes run as usual. These commands behave differently until `pg_backup_stop`:

| Command | During a backup |
|---|---|
| `CHECKPOINT`, `FORCE CHECKPOINT` | Fail with `cannot checkpoint while a backup is in progress`. Run them again after `pg_backup_stop`. A `pg_backup_start` issued while a checkpoint runs waits for it. |
| Automatic checkpoints | Skipped. |
| `VACUUM (COMPACT_*)` | Waits until the backup ends, then compacts. It can be canceled while it waits. |
| `VACUUM (REFRESH_*)` | Runs. The index files it would normally clean up are kept until after the backup. |
| `DROP TABLE`, `DROP INDEX`, `DROP DATABASE`, `DETACH` | Commit at once. The files of the dropped objects are removed after `pg_backup_stop`. |
| Background compaction of indexes and search tables | Paused. |

Because nothing is checkpointed, the write-ahead logs keep growing, and dropped objects keep their disk space until `pg_backup_stop`. Plan disk space for the length of the backup.

## Monitoring

`pg_stat_progress_basebackup` shows a `pg_backup_start` that is running:

<SqlLogicTest id="operations/backup/progress" />

`phase` is one of:

| Phase | Meaning |
|---|---|
| `initializing` | Starting. |
| `waiting for checkpoint to finish` | A checkpoint is running; the backup starts when it finishes. |
| `waiting for search maintenance to stop` | Index and search-table compactions are being paused. |
| `waiting for in-flight commits` | Transactions that are committing are finishing; the backup position is recorded next. |

`backup_total`, `backup_streamed`, `tablespaces_total` and `tablespaces_streamed` are always 0. Once `pg_backup_start` has returned, the view no longer shows the backup.

## Subscriptions after a restore

A restored server starts with every [subscription](../sql/statements/create_subscription/index.md) disabled, and logs a warning for each one. Their publishers' replication slots have moved past the restored position, so resuming as they are would skip the transactions in between, and the server the backup was taken from may still be using those slots. To resume a subscription, either check that its slot on the publisher can still provide what the restored server needs, or create a new slot and resynchronize, then run `ALTER SUBSCRIPTION ... ENABLE`. The subscription stays disabled across restarts until you do.

## Databases with a failed checkpoint

When a checkpoint of a database has failed since the server started, `pg_backup_start` still succeeds, but sends a warning naming that database and logs it. After a restore, that database recovers as it does after a crash. Check it before relying on the backup, or restart the server and take the backup again.

## pgBackRest

[pgBackRest](https://pgbackrest.org) takes full backups of SereneDB and restores them. It runs on the server's host, reads the data directory directly, and connects over TCP. A minimal configuration:

```ini
[global]
repo1-path=/var/lib/pgbackrest

[serenedb]
pg1-path=/var/lib/serenedb/data
pg1-port=5432
pg1-socket-path=127.0.0.1
pg1-user=postgres
```

- `pg1-path` is the server's data directory.
- `pg1-socket-path` is the address the server listens on. SereneDB does not listen on a Unix socket, so this makes pgBackRest connect over TCP.

Create the stanza once, then back up and restore:

```bash
pgbackrest --stanza=serenedb stanza-create
pgbackrest --stanza=serenedb --type=full --no-archive-check backup
pgbackrest --stanza=serenedb --type=none restore
```

- `backup` needs `--no-archive-check`: SereneDB has no WAL archiving.
- `restore` needs `--type=none`, and restores into an empty data directory. Start the server on it afterwards; pgBackRest has already written `backup_label`.

Limitations:

- Only full backups (`--type=full`) are supported.
- `pgbackrest check` cannot pass, because it waits for archived WAL.
- Restoring single databases (`--db-include`) and point-in-time recovery are not supported.
