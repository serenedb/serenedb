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
- **Integrity:** every index file records CRC32C checksums of its data and of its footer, a record at the end of the file. Opening a file verifies only the footer's checksum, so a damaged footer makes the file fail to open. Damage anywhere else in the file is not detected, and queries on that table or index can return wrong results.

The log holds every committed transaction of the server: the changes to the catalog, meaning the definitions of roles, databases, schemas, tables, text search dictionaries, foreign servers, indexes, sequences and persistent secrets, and the rows written to tables until their tables write them into index files. It follows these rules:

- **Newer release, older log:** a release reads a log written by earlier releases since the last break.
- **Older release, newer log:** an older release reads a log written by a newer one as long as the log uses no feature the older release lacks. Otherwise the older release refuses it; it never reads it wrongly. A text search dictionary or an index definition that uses an option the older release does not know is refused the same way.
- **Breaks:** a release that can't read logs from earlier releases says so in its release notes.
- **Integrity:** every log record records a CRC32C checksum. A record cut off by a crash at the end of the log is dropped when the server starts. The server replays the log when it starts, so a record whose checksum does not match, or an intact record that the release cannot read, stops the server from starting; its log names the file and the error.
- **Foreign servers:** a server keeps the options it was created with. An option that a later release adds takes its default for servers created before it. If the connector no longer accepts an option a server has, that server fails to connect at startup and the error is logged; the rest of the server starts, and the foreign server can be dropped and created again.

## Data directory

A server keeps its log and its databases under `engine_v1` in its data directory:

- `engine_v1/log/`: the log, in files of 16 MiB named by their position in the log; `history`, which keeps the catalog changes of the log files the server has removed; and `spare`, the next log file, prepared ahead.
- `engine_v1/<oid>/`: one database, named by its `pg_database.oid`. It holds one directory for each table and each inverted index on a view, named by the object's `pg_class.oid`, with that object's index files.

Beside `engine_v1`, the root of the data directory holds:

- `pg_hba.conf`: the [client authentication rules](../security/client_authentication.md#the-hba-ruleset) that `SET hba` writes, unless the `--hba_config` flag names another file.
- `global/` and `PG_VERSION`: the files PostgreSQL backup tools look for; see [Backup](../operations/backup.md).
- `tmp/`: temporary files of queries that spill to disk (the `temp_directory` setting).
- `extensions/`: the extension directory (the `extension_directory` setting).
- `LOCK`: held while the server runs, so that a second server does not start on the same data directory.

`engine_v1` holds everything a server needs when it starts again except the authentication rules, so a copy of the data directory needs `pg_hba.conf` as well. `tmp/`, `extensions/` and `LOCK` hold nothing a server needs when it starts again. Another server reads a database of the data directory with [`serenedb_fdw`](../sql/statements/create_server/index.md#read-only-serenedb-databases).

Dropping a database, a table or an index removes its directory once no query uses it any more. If the server stops before that or in the middle of it, or crashes while an object is being created, it removes what is left of the directory when it starts again. A server refuses to start when `engine_v1/log` is missing but database directories hold data, instead of starting empty beside them.

For the meaning of version numbers and release lines, see [Versioning](./versioning.md).
