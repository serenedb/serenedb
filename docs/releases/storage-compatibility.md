---
title: Storage compatibility
split: page
---

# Storage compatibility

This page describes which SereneDB releases can read the data directory, meaning the server's log and the index files of its tables and inverted indexes, written by other releases.

Index files hold the rows of tables and the documents of inverted indexes. They follow these rules:

- **Newer release, older index:** a release reads index files written by earlier releases since the last break.
- **Older release, newer index:** an older release reads an index file written by a newer one as long as the file uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A server that refuses an index file does not start; its log names the file and why it cannot be read, and the files are left as they are.
- **Breaks:** a release that can't read index files from earlier releases says so in its release notes, together with the steps to move affected tables and indexes to it.
- **Integrity:** every index file records CRC32C checksums of its data and of its footer, a record at the end of the file, and the log records the size of each file and of its footer. Opening a file verifies only its size and the footer's checksum, so a damaged footer makes the file fail to open. Damage anywhere else in the file is not detected, and queries on that table or index can return wrong results.

The log holds every committed transaction of the server: the changes to the catalog, meaning the definitions of roles, tablespaces, databases, schemas, tables, text search dictionaries, foreign servers, indexes, sequences and persistent secrets, and the rows written to tables until their tables write them into index files. It follows these rules:

- **Newer release, older log:** a release reads a log written by earlier releases since the last break.
- **Older release, newer log:** an older release reads a log written by a newer one as long as the log uses no feature the older release lacks. Otherwise the older release refuses it; it never reads it wrongly. A text search dictionary or an index definition that uses an option the older release does not know is refused the same way.
- **Breaks:** a release that can't read logs from earlier releases says so in its release notes.
- **Integrity:** every log record records a CRC32C checksum. A record cut off by a crash at the end of the log is dropped when the server starts. The server replays the log when it starts, so a record whose checksum does not match, or an intact record that the release cannot read, stops the server from starting; its log names the file and the error.
- **Foreign servers:** a server keeps the options it was created with. An option that a later release adds takes its default for servers created before it. If the connector no longer accepts an option a server has, that server fails to connect at startup and the error is logged; the rest of the server starts, and the foreign server can be dropped and created again.

## Data directory

A server keeps its log and its tables under `engine_v1` in its data directory:

- `engine_v1/wal/`: the log, in files of 16 MiB named by their position in the log, the next one prepared ahead; the snapshot, which holds the catalog and the state of every table as of the oldest log file kept, in a file named `<number>.snapshot`; and `current_snapshot`, a record of the snapshot's number. Each snapshot is written once, as a new file under the next number, and `current_snapshot` moves to it once it is complete, so no file is ever renamed; the previous snapshot is removed after that.
- `engine_v1/store/`: the index files of every table and every inverted index on a view, of every database, side by side. Each file is named `<segment>.<kind>`: a segment number, 16 hexadecimal digits that the server never gives twice, and the kind of data the file holds. The log records which segments each table and index uses; `sdb_metrics` counts the files of each of them in `num_files`.

A [tablespace](../sql/statements/create_tablespace/index.md) keeps the index files of the tables and indexes placed in it at its location, in `<location>/<system identifier>/engine_v1/store/`, named by the system identifier in the data directory's `global/pg_control`. On an object store, each directory down to `store` is also an empty object named after it with a trailing slash, such as `store/`, which the store's console shows as a folder.

Beside `engine_v1`, the root of the data directory holds:

- `pg_hba.conf`: the [client authentication rules](../security/client_authentication.md#the-hba-ruleset) that `SET hba` writes, unless the `--hba_config` flag names another file.
- `global/` and `PG_VERSION`: the files PostgreSQL backup tools look for; see [Backup](../operations/backup.md). A running server holds an exclusive lock on `PG_VERSION`, so that a second server does not start on the same data directory, and a read-only attachment through `serenedb_fdw` holds a shared one, so that neither starts while the other has the directory.
- `tmp/`: temporary files of queries that spill to disk (the `temp_directory` setting).

`engine_v1` holds everything a server needs when it starts again except the authentication rules and the tables and indexes placed in tablespaces, so a copy of the data directory needs `pg_hba.conf` and the directory of every tablespace as well. `tmp/` holds nothing a server needs when it starts again. Another server reads a database of the data directory with [`serenedb_fdw`](../sql/statements/create_server/index.md#read-only-serenedb-databases).

Dropping a table or an index, or the database that holds it, removes its files once no query uses them any more, in the tablespace that holds it. If the server stops before that or in the middle of it, or crashes while an object is being created, it removes the segment files that no table or index uses when it starts again; a file whose name is not a segment's is left as it is.

A server started on a data directory without `PG_VERSION` creates a new instance there; `PG_VERSION` appears once the catalog log holds the instance's first commit, so a first start that stops before that starts over the next time. A data directory with `PG_VERSION` but no catalog log in `engine_v1/wal` is refused instead of starting empty. A server also refuses to start when the `store` directory of a tablespace that holds a table or an index is missing; its log names the directory and the object. A missing file of a table or an index fails the queries that read it, and the error names the file.

For the meaning of version numbers and release lines, see [Versioning](./versioning.md).
