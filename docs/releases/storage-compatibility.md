---
title: Storage compatibility
split: page
---

# Storage compatibility

This page describes which SereneDB releases can read the server directory, meaning the server's snapshot, its log and the index files of its tables and inverted indexes, written by other releases.

Index files hold the rows of tables and the documents of inverted indexes. They follow these rules:

- **Newer release, older index:** a release reads index files written by earlier releases since the last break.
- **Older release, newer index:** an older release reads an index file written by a newer one as long as the file uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A server that refuses an index file does not start; its log names the file and why it cannot be read, and the files are left as they are.
- **Breaks:** a release that can't read index files from earlier releases says so in its release notes, together with the steps to move affected tables and indexes to it.
- **Integrity:** every index file records CRC32C checksums of its data and of its footer, a record at the end of the file. Opening a file verifies only the footer's checksum, so a damaged footer makes the file fail to open. Damage anywhere else in the file is not detected, and queries on that table or index can return wrong results.

The log holds every committed transaction of the server: the changes to the catalog, meaning the definitions of roles, tablespaces, databases, schemas, tables, text search dictionaries, foreign servers, indexes, sequences and persistent secrets, and the rows written to tables until their tables write them into index files. It follows these rules:

- **Newer release, older log:** a release reads a log written by earlier releases since the last break.
- **Older release, newer log:** an older release reads a log written by a newer one as long as the log uses no feature the older release lacks. Otherwise the older release refuses it; it never reads it wrongly. A text search dictionary or an index definition that uses an option the older release does not know is refused the same way.
- **Breaks:** a release that can't read logs from earlier releases says so in its release notes.
- **Integrity:** every log record records a CRC32C checksum. A record cut off by a crash at the end of the log is dropped when the server starts. The server replays the log when it starts, so a record whose checksum does not match, or an intact record that the release cannot read, stops the server from starting; its log names the file and the error.
- **Foreign servers:** a server keeps the options it was created with. An option that a later release adds takes its default for servers created before it. If the connector no longer accepts an option a server has, that server fails to connect at startup and the error is logged; the rest of the server starts, and the foreign server can be dropped and created again.

## Server directory

A server keeps its state in its server directory, the directory it is started with:

- `snapshot`: the catalog and the state of every table as of a position in the log. A new snapshot replaces the file whole: it is written beside it as `snapshot.tmp` and renamed over it, so a crash leaves either the previous snapshot or the new one. A snapshot records the storage version it was written in, and a release refuses a snapshot of another version.
- `logs/`: the log of the commits after the snapshot, in files of 16 MiB named by their position in the log, the next one prepared ahead. The `--log_directory` flag keeps the log in another directory, for example on a faster disk; a running server then keeps a `LOCK` file in it locked, so that no second server writes the same log.
- `segments/`: the index files of every table and every inverted index on a view, of every database, side by side. Each file is named `<segment>.<kind>`: a segment number, 16 hexadecimal digits that the server never gives twice, and the kind of data the file holds. The snapshot and the log record which segments each table and index uses; `sdb_metrics` counts the files of each of them in `num_files`.
- `backups/`: the snapshot that a running [backup](../operations/backup.md) keeps, removed when the backup stops or, after a crash, when the server starts again.
- `pg_hba.conf`: the [client authentication rules](../security/client_authentication.md#the-hba-ruleset) that `SET hba` writes, unless the `--hba_config` flag names another file.
- `global/` and `PG_VERSION`: the files PostgreSQL backup tools look for; see [Backup](../operations/backup.md). A running server holds an exclusive lock on `PG_VERSION`, so that a second server does not start on the same server directory, and a read-only attachment through `serenedb_fdw` holds a shared one, so that neither starts while the other has the directory.
- `temp/`: temporary files of queries that spill to disk; the `--temp_directory` flag moves them.

A [tablespace](../sql/statements/create_tablespace/index.md) keeps the index files of the tables and indexes placed in it in `<location>/segments/`, and `<location>/OWNER` names the server the location belongs to. On an object store, the location and `segments` are also empty objects named after them with a trailing slash, which the store's console shows as folders.

The snapshot, the log and `segments/` hold everything a server needs when it starts again except the authentication rules and the tables and indexes placed in tablespaces, so a copy of the server directory needs `pg_hba.conf` and the location of every tablespace as well. `temp/` holds nothing a server needs when it starts again. Another server reads a database of the server directory with [`serenedb_fdw`](../sql/statements/create_server/index.md#read-only-serenedb-databases).

Dropping a table or an index, or the database that holds it, removes its files once no query uses them any more, in the tablespace that holds it. If the server stops before that or in the middle of it, or crashes while an object is being created, it removes the segment files that no table or index uses when it starts again; a file whose name is not a segment's is left as it is.

A server started on a server directory without a snapshot creates a new server there and writes its first snapshot before it accepts connections. The `--require_existing` flag refuses to start instead, for a deployment that must never start an empty server over a directory it failed to mount or name. A server directory that holds segments but no snapshot is refused. A server also refuses to start when its snapshot needs log files that are missing, when the `segments` directory of a tablespace that holds a table or an index is missing, or when a file of a table or an index is; its log names the directory or the file and the object.

The `--checkpoint_at_stop` flag ends every clean stop with a snapshot of everything committed and an empty log directory. The snapshot alone then holds the server, so it starts again from its server directory with a new, empty log directory, on another machine for example. A server that stopped cleanly starts without listing `segments/`.

For the meaning of version numbers and release lines, see [Versioning](./versioning.md).
