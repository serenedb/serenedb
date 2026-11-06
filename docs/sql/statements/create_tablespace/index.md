---
title: CREATE TABLESPACE
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

A tablespace is a named location for files: a local directory, a prefix in an S3 bucket or an S3-compatible store, or a prefix in an Azure Blob Storage container. Search tables, inverted indexes and whole databases can be placed in a tablespace, so that their files live on a faster disk, a larger one, or an object store.

## Examples

Create a tablespace on a local disk:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_001" />

Put a search table in it:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_002" />

Put an inverted index in it:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_003" />

Put every search table and index a session creates in it, unless `CREATE` names another tablespace:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_004" />

See where each object is placed:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_005" />

## Syntax

```sql
CREATE TABLESPACE name [ OWNER { new_owner | CURRENT_ROLE | CURRENT_USER | SESSION_USER } ] LOCATION 'location'
```

- `name`: the tablespace's name. Names starting with `pg_` are reserved for the built-in tablespaces.
- `OWNER`: the role that owns the tablespace; the role running the statement by default.
- `LOCATION`: an absolute path of a local directory, or an object store URL such as `s3://bucket/prefix` or `az://container/prefix`.

Only a superuser can create a tablespace. `CREATE TABLESPACE` cannot run inside a transaction block. With `enable_external_access` off, the location must be inside `allowed_directories`; tablespaces created before keep working.

## Locations

A location is kept in normal form, with backslashes read as slashes, repeated and trailing slashes dropped and `.` and `..` resolved: `LOCATION 's3://bucket//prefix/'` is `s3://bucket/prefix` and `LOCATION '/mnt/nvme\serenedb/'` is `/mnt/nvme/serenedb`. A tablespace keeps its files in a directory of its own under `engine_v1` at its location, named by the server's system identifier and the tablespace's OID, so several tablespaces, of one server or of several, can share a location. A local directory is created when it does not exist; an object store location is listed, so `CREATE TABLESPACE` fails when the store cannot be reached or the credentials do not allow listing it. A location with an `engine_v1` directory in its path, such as one inside the files of another tablespace or of the data directory, is refused:

```text
ERROR:  tablespace location "/mnt/nvme/serenedb/engine_v1/inside" is inside the files of a tablespace
```

A `memory://` location is refused, since files there are lost when the server stops:

```text
ERROR:  tablespace location cannot be in memory
DETAIL:  Files under memory:// are lost when the server stops.
```

An object store URL has to name a bucket or a container:

```text
ERROR:  tablespace location must name a bucket or container
```

The server reaches an object store location with the credentials of a persistent secret whose scope covers its normal form:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_011" />

See [Index Storage](../../../configuration/index_storage.md) for the credentials, the caching and the requests that index files on an object store cost.

On S3, the parts of an upload that the server stops in the middle of stay in the bucket, unlisted and billed, until the upload is aborted: give the bucket a lifecycle rule that aborts incomplete multipart uploads.

A local location has to exist on the server's machine at the same path for as long as the tablespace holds files.

A [backup](../../../operations/backup.md) copies only the data directory, so `pg_backup_start` fails while a tablespace other than `pg_default` and `pg_global` exists.

The built-in tablespaces are always there:

| Tablespace | Location |
| ---------- | -------- |
| `pg_default` | the data directory; where objects go when nothing names another tablespace |
| `pg_global` | reserved; nothing can be placed in it |

`pg_tablespace` lists the tablespaces and `pg_tablespace_location(oid)` returns a tablespace's location, an empty string for the built-in ones:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_006" />

`pg_tablespace_size(name)` or `pg_tablespace_size(oid)` returns the bytes of a tablespace's files, which psql's `\db+` shows: for `pg_default` the databases' files in the data directory, for `pg_global` the catalog the cluster shares. On an object store each call costs a listing of the tablespace's directory.

## Placing objects

`TABLESPACE name` places a new object in a tablespace:

| Statement | Placed in the tablespace |
| --------- | ------------------------ |
| `CREATE TABLE ... WITH (storage = 'search') TABLESPACE name` | the files of the search table |
| `CREATE TABLE ... TABLESPACE name AS query` | the same, for a search table created from a query |
| `CREATE INDEX ... USING inverted (...) [WITH (...)] TABLESPACE name` | the files of the inverted index |
| `CREATE DATABASE name [WITH] TABLESPACE [=] name` | the database's file and write-ahead log, and by default its search tables and indexes |

An object created without `TABLESPACE` goes to the tablespace that the `default_tablespace` setting names, and when that is empty, to the tablespace of its database. `default_tablespace` takes the name of an existing tablespace; a tablespace dropped while a session names it is skipped.

Tables without `storage = 'search'` and indexes other than inverted indexes live in the database file, and an inverted index on a search table lives with the table, so `TABLESPACE` with any tablespace other than the database's is refused for them, and `default_tablespace` does not apply to them:

```text
ERROR:  only a table created WITH (storage = 'search') can be placed in a tablespace
```

A table or an index in a database other than a SereneDB database, such as `memory` or an attached DuckDB file, takes no `TABLESPACE` clause:

```text
ERROR:  TABLESPACE is not supported for tables in a duckdb catalog
```

Create a database in a tablespace:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_007" />

A database created in a tablespace on an object store keeps its database file and write-ahead log in the data directory, and its search tables and indexes in the tablespace.

`pg_class.reltablespace` is the tablespace of a search table or an inverted index, `0` for the tablespace of its database, and an inverted index on a search table, stored with the table, shows the table's; `pg_database.dattablespace` is the tablespace of a database. An object stays in its tablespace for its whole life: moving it to another tablespace is not supported.

## Privileges

Placing an object in a tablespace other than its database's needs the `CREATE` privilege on the tablespace, which the owner and superusers hold:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_008" />

`has_tablespace_privilege` checks it. The built-in tablespaces cannot be granted on, renamed or given to another owner; `CREATE` on `pg_default` is held by superusers only, and every role can place objects in the tablespace of their database.

## ALTER TABLESPACE

```sql
ALTER TABLESPACE name RENAME TO new_name
ALTER TABLESPACE name OWNER TO { new_owner | CURRENT_ROLE | CURRENT_USER | SESSION_USER }
```

Renaming a tablespace or changing its owner needs ownership of it. The objects in a tablespace stay where they are when it is renamed:

<SqlLogicTest id="sql/statements/create_tablespace/index/example_009" />

## DROP TABLESPACE

```sql
DROP TABLESPACE [ IF EXISTS ] name
```

Only the owner of a tablespace, or a superuser, can drop it, and only when it is empty: no database was created in it and no search table or inverted index is placed in it. While a database that the server left unattached at startup with `--missing_database=skip` exists, no tablespace can be dropped:

```text
ERROR:  tablespace "fast" cannot be dropped while database "lost" is not attached
HINT:  Restore the files of the database and restart the server, or drop the database.
```

Dropping a tablespace removes its directory once the transactions that began before the drop have ended; the location and its `engine_v1` stay, and a new tablespace can use the location at once. If the server stops before the directory is removed, it removes the directory when it starts again. `DROP TABLESPACE` cannot run inside a transaction block.

When one transaction places an object in a tablespace while another drops the tablespace, the one that commits second fails, as with any object that another depends on. A drop fails with `tablespace "name" is not empty` when the placement committed before the drop looked, and with `Could not commit DROP of "name" because a dependency was created after the transaction started` when it commits later; a placement that commits after the drop fails with `Could not commit creation of dependency, subject "name" has been deleted`; the files of an object that fails this way are removed with the tablespace.

<SqlLogicTest id="sql/statements/create_tablespace/index/example_010" />

## See also

- [Index Storage](../../../configuration/index_storage.md) — object store credentials, caching and requests
- [CREATE DATABASE](../create_database/index.md)
- [CREATE INDEX](../create_index/index.md)
