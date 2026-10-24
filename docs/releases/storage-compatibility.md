---
title: Storage compatibility
split: page
---

# Storage compatibility

This page describes which SereneDB releases can read search indexes, meaning inverted indexes and search tables, written by other releases.

- **Newer release, older index:** a release reads indexes written by earlier releases since the last break.
- **Older release, newer index:** an older release reads an index written by a newer one as long as the index uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A server that refuses an index does not start; its log names the index file and why it cannot be read, and the index files are left as they are.
- **Breaks:** a release that can't read indexes from earlier releases says so in its release notes, together with the steps to move affected indexes and search tables to it.
- **Integrity:** every index file records CRC32C checksums of its data and of its footer, a record at the end of the file. Opening a file verifies only the footer's checksum, so a damaged footer makes the file fail to open. Damage anywhere else in the file is not detected, and queries on that index can return wrong results.

Search tables write their changes to the write-ahead log of the database file they belong to, so the rules for database files below cover those changes too. Rows that a transaction has already written out as index files, as a parallel insert does, or a serial write once its buffered rows exceed `checkpoint_threshold`, are logged only by the names of those files, so the index-file rules above apply to them.

Database files hold the rows of tables. The catalog, meaning the definitions of roles, databases, schemas, tables, text search dictionaries, foreign servers, indexes and sequences, is one log for the whole instance: `engine_v1/catalog.wal` in the data directory, written in the same format as the write-ahead log of a database file. Database files, their write-ahead logs and the catalog log follow these rules:

- **Newer release, older database:** a release opens database files written by earlier releases since the last break, and upgrades them when it writes to them. An upgraded file can no longer be opened by the older release.
- **Older release, newer database:** an older release opens a database file written by a newer one as long as the file uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A text search dictionary or an index definition that uses an option the older release does not know is refused the same way.
- **Breaks:** a release that can't open database files from earlier releases says so in its release notes.
- **Integrity:** every write-ahead log entry records a checksum. An entry cut off by a crash at the end of the log is dropped when the database opens. The server replays the write-ahead log of every database when it starts, so an entry whose checksum does not match, or an intact entry that the release cannot read, stops the server from starting; its log names the file and the error.
- **Foreign servers:** a server keeps the options it was created with. An option that a later release adds takes its default for servers created before it. If the connector no longer accepts an option a server has, that server fails to connect at startup and the error is logged; the rest of the database opens, and the server can be dropped and created again.

The catalog log belongs to the whole instance: if a release refuses it or cannot read an intact entry of it, the server does not start.

## Data directory

A server keeps its catalog and its databases under `engine_v1` in its data directory:

- `engine_v1/catalog.wal`: the catalog log.
- `engine_v1/<oid>/`: one database, named by its `pg_database.oid`. It holds the database file `data.db` with its write-ahead log, which also logs the changes of the database's search tables, the write-ahead logs that checkpoints [kept for search tables](../sql/indexes/inverted/maintenance.md#durability) (`data.db.wal.retained.<n>`), and one directory for each search table and each inverted index on a table or view, named by the object's `pg_class.oid`.

Beside `engine_v1`, the root of the data directory holds:

- `pg_hba.conf`: the [client authentication rules](../security/client_authentication.md#the-hba-ruleset) that `SET hba` writes, unless the `--hba_config` flag names another file.
- `secrets/`: the [persistent secrets](../configuration/secrets_manager.md#persistent-secrets), unencrypted (the `secret_directory` setting).
- `tmp/`: temporary files of queries that spill to disk (the `temp_directory` setting).
- `extensions/`: the extension directory (the `extension_directory` setting).
- `LOCK`: held while the server runs, so that a second server does not start on the same data directory.

`engine_v1` holds neither the persistent secrets nor the authentication rules, so a copy of the data directory needs `pg_hba.conf` and `secrets/` as well. `tmp/`, `extensions/` and `LOCK` hold nothing a server needs when it starts again.

Dropping a database, a table or an index removes its directory once no query uses it any more. If the server stops before that or in the middle of it, or crashes while an object is being created, it removes what is left of the directory when it starts again. A server refuses to start when the catalog log is missing but database directories hold data, instead of starting empty beside them.

For the meaning of version numbers and release lines, see [Versioning](./versioning.md).
