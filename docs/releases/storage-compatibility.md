---
title: Storage compatibility
split: page
---

# Storage compatibility

This page describes which SereneDB releases can read search indexes, meaning inverted indexes and search tables, written by other releases.

- **Newer release, older index:** a release reads indexes written by earlier releases since the last break.
- **Older release, newer index:** an older release reads an index written by a newer one as long as the index uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A server that refuses an index does not start; its log names the index file and why it cannot be read, and the index files are left as they are.
- **Breaks:** a release that can't read indexes from earlier releases says so in its release notes, together with the steps to move affected indexes and search tables to it.
- **Integrity:** every index file records CRC32C checksums. The metadata of each file is verified whenever the file is opened, and a damaged file fails to open instead of returning wrong results.

The write-ahead log of search tables follows the rules of the database file it belongs to.

Database files hold the rows of tables. The catalog, meaning the definitions of roles, databases, schemas, tables, text search dictionaries, foreign servers, indexes and sequences, is one log for the whole instance: `engine_v1/catalog.wal` in the data directory, written in the same format as the write-ahead log of a database file. Database files, their write-ahead logs and the catalog log follow these rules:

- **Newer release, older database:** a release opens database files written by earlier releases since the last break, and upgrades them when it writes to them. An upgraded file can no longer be opened by the older release.
- **Older release, newer database:** an older release opens a database file written by a newer one as long as the file uses no feature the older release lacks. Otherwise the older release refuses to open it; it never reads it wrongly. A text search dictionary or an index definition that uses an option the older release does not know is refused the same way.
- **Breaks:** a release that can't open database files from earlier releases says so in its release notes.
- **Integrity:** every write-ahead log entry records a checksum. An entry cut off by a crash is dropped when the database opens; an intact entry that the release cannot read stops the database from opening instead.
- **Foreign servers:** a server keeps the options it was created with. An option that a later release adds takes its default for servers created before it. If the connector no longer accepts an option a server has, that server fails to connect at startup and the error is logged; the rest of the database opens, and the server can be dropped and created again.

The catalog log belongs to the whole instance: if a release refuses it or cannot read an intact entry of it, the server does not start.

## Data directory

A server keeps everything it stores under `engine_v1` in its data directory:

- `engine_v1/catalog.wal`: the catalog log.
- `engine_v1/<oid>/`: one database, named by its `pg_database.oid`. It holds the database file `data.db` with its write-ahead log, the write-ahead log of the database's search tables, and one directory for each search table and each inverted index on a table or view, named by the object's `pg_class.oid`.

Dropping a database, a table or an index removes its directory once no query uses it any more. If the server stops before that, or crashes while an object is being created, it removes the leftover directory when it starts again. A server refuses to start when the catalog log is missing but database directories hold data, instead of starting empty beside them.

Data directories written by earlier releases keep their files in `engine_catalog`, `engine_duckdb` and `engine_search`. This release does not read them: a server started on such a directory starts with an empty catalog and leaves those files alone.

For the meaning of version numbers and release lines, see [Versioning](./versioning.md).
