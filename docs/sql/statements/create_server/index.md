---
title: CREATE SERVER
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import DocCallout from "@site/src/components/DocCallout";

A **foreign server** names a remote database — ClickHouse or PostgreSQL — or a database on disk — a DuckDB file or the directory of a SereneDB database — together with what is needed to reach it, and stores that definition in the catalog. Once created, the server is a catalog you query like any other: `server_name.schema.table`.

The difference from [`ATTACH`](../attach/index.md) is persistence and access control. An attachment is session-level state that disappears when the server process restarts, and it carries no privileges of its own. A foreign server is **catalog DDL**: it survives restart, re-connects automatically on boot, has an owner, and is protected by `USAGE` privileges.

| | [`ATTACH`](../attach/index.md) | `CREATE SERVER` |
|---|---|---|
| Survives restart | No — re-attach every session | Yes — replayed on boot |
| Credentials | In the connection string | In the server's `OPTIONS` |
| Owner | None | The creating role |
| Access control | None | `USAGE` on the server |
| Catalog visibility | `SHOW DATABASES` | `pg_foreign_server` + `SHOW DATABASES` |

## `CREATE SERVER`

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

Create a server over ClickHouse:

```sql
CREATE SERVER analytics FOREIGN DATA WRAPPER clickhouse_fdw
  OPTIONS (host 'clickhouse.internal', port '9000', database 'events');
```

…and over PostgreSQL:

```sql
CREATE SERVER orders FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'pg.internal', port '5432', database 'shop', user 'reader', password 'secret');
```

### Foreign-data wrappers

The wrapper name selects the connector. Five are implemented:

| `FOREIGN DATA WRAPPER` | Connects to |
|---|---|
| `clickhouse_fdw` | ClickHouse, over its native protocol |
| `duckdb_fdw` | A DuckDB database file; see [DuckDB files](#duckdb-files) |
| `iceberg_fdw` | An Iceberg REST catalog; its options and authentication are on [Iceberg Catalog Authentication](../../../configuration/iceberg_authentication.md) |
| `postgres_fdw` | PostgreSQL, over the wire protocol |
| `serenedb_fdw` | The directory of a database written by another SereneDB server, read-only; see [Read-only SereneDB databases](#read-only-serenedb-databases) |

Any other wrapper name is rejected at `CREATE SERVER` time.

### Server names

A server name is a **bare identifier** — servers are not schema-qualified, so `CREATE SERVER a.b` is a syntax error rather than a server named `a.b`. Reserved keywords must be quoted (`CREATE SERVER "select" …`).

The name is also the **attach alias**, and that alias is instance-wide: it shares a namespace with SereneDB databases and with the aliases created by `ATTACH`. Two servers cannot share a name, and a server cannot take the name of an existing database.

### `OPTIONS`

`OPTIONS` is a list of `key 'value'` pairs. Keys are identifiers — unquoted or quoted, and normalized to lower case, so `"PASSWORD"` and `password` are the same key. Values are always **single-quoted strings**, including numeric ones like `port '9000'`.

Common keys, with the aliases each connector accepts:

| Option | Aliases | Meaning |
|---|---|---|
| `host` | `hostname` | Remote host |
| `port` | | Remote port |
| `user` | `username` | Remote role |
| `password` | `passwd` | Password for that role |
| `database` | `dbname`, `db` | Remote database |
| `secure` | `ssl` (ClickHouse) | Use TLS |

Keys beyond this list are passed through to the connector, so any connection parameter the underlying connector understands (for example `sslpassword`, `connect_timeout`) can be set the same way. An option the connector does not recognize surfaces as a connection error at `CREATE SERVER`. A `clickhouse_fdw` server also takes `compression` (`none`, `zstd` or `lz4`), and takes every option it leaves out from the unnamed `clickhouse` [secret](../../../configuration/secrets_manager.md#clickhouse-secrets), when there is one.

Values are stored verbatim and are not parsed further, so a value containing spaces (`password 'pass word'`) round-trips intact.

### Connectivity is validated eagerly

`CREATE SERVER` connects to the remote **before** persisting anything. If the connection fails — wrong host, unknown role, bad password — the statement raises the connector's error and leaves behind neither a catalog row nor an attachment:

```sql
-- Rejected: no such role on the remote. Nothing is persisted.
CREATE SERVER orders FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'pg.internal', port '5432', database 'shop', user 'nosuchrole');
```

This makes a successful `CREATE SERVER` a positive statement about reachability, not just a recorded intention.

### `IF NOT EXISTS`

`IF NOT EXISTS` makes the statement a no-op when the name is already taken. The check happens **before** the connection attempt, so the `OPTIONS` of a skipped `CREATE SERVER` are never applied — an existing server is not reconfigured by re-running `CREATE SERVER IF NOT EXISTS` with different options.

<DocCallout type="attention">

There is no `CREATE OR REPLACE SERVER` and no `ALTER SERVER`. To change a server's options, `DROP SERVER` and create it again.

</DocCallout>

## Querying through a server

A server behaves as a catalog. Reference remote objects with the server name in the catalog position:

```sql
SELECT id, val FROM analytics.events.pageviews ORDER BY id;
```

Where the connector supports writes, DDL and DML work through the server too — the PostgreSQL connector, for example, accepts schema and table creation:

```sql
CREATE SCHEMA orders.staging;
CREATE TABLE orders.staging.import (id INTEGER PRIMARY KEY, val TEXT);
INSERT INTO orders.staging.import VALUES (1, 'first');
```

Filter pushdown applies as it does for an attachment: equality, `IN` and other supported predicates are translated into the remote query rather than filtered locally.

To see the queries sent to the remote, enable the `PostgresQueryLog` or `ClickHouseQueryLog` [log type](../../../configuration/logging/index.md#list-of-available-log-types) with the `memory` [log storage](../../../configuration/logging/index.md#log-storages), and read them, with their durations, from `sdb_logs_parsed`. The server logs to `stdout` by default, which `sdb_logs_parsed` cannot read. Both choices apply to the whole server: until `CALL enable_logging(storage = 'stdout')` switches back, its log holds only this log type, in memory.

```sql
CALL enable_logging('ClickHouseQueryLog', storage = 'memory');
SELECT id, val FROM analytics.events.pageviews ORDER BY id;
SELECT query, duration_ms FROM sdb_logs_parsed('ClickHouseQueryLog');
```

## DuckDB files

`duckdb_fdw` attaches a DuckDB database file — the file [`ATTACH`](../attach/duckdb.md) opens — as a server, so it is re-attached on boot and guarded by `USAGE`. `path` names the file and is required; `read_only 'true'` opens it for reading only:

```sql
CREATE SERVER sales FOREIGN DATA WRAPPER duckdb_fdw
  OPTIONS (path '/data/sales.duckdb', read_only 'true');

SELECT count(*) FROM sales.main.orders;
```

Without `read_only` a local file is opened for reading and writing, and a remote one (`https://`, `s3://`, …) for reading only.

## Read-only SereneDB databases

`serenedb_fdw` attaches the directory of a database written by another SereneDB server — a copy of its data directory, a disk snapshot, or a volume of a server that is stopped — and serves it read-only. Its tables, views, inverted indexes and search tables are queried as usual:

```sql
CREATE SERVER archive FOREIGN DATA WRAPPER serenedb_fdw
  OPTIONS (path '/mnt/snapshots/shop/engine_v1/16390');

SELECT id FROM archive.public.docs WHERE body @@ 'invoice';
```

`path` is required and names the database's directory inside the other server's data directory, `engine_v1/<database oid>`; on that server, `SELECT oid FROM pg_database WHERE datname = 'shop'` gives the oid. The database's definitions are read from that server's catalog log, `engine_v1/catalog.wal`, so a copy must keep it beside the database directory, both copied at the same moment. `ATTACH '<path>' AS archive (TYPE serenedb, READ_ONLY)` attaches the same directory until `DETACH` or a restart of the server.

### What a read-only database does

- **Nothing on disk changes.** The database file and the catalog log are opened read-only, and inverted indexes and search tables without a writer. No refresh, compaction or cleanup runs on them, and nothing is written when the server stops.
- **Search finds refreshed rows.** Rows not yet refreshed into a search table or an inverted index when the directory was last written are not returned by it. Run `VACUUM (REFRESH_TABLE)` before stopping the server to include them.
- **Writes are refused.** `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `MERGE`, other DDL and `nextval` fail as on any read-only attachment, and `CREATE INDEX`, `VACUUM`, `REINDEX` and `es_refresh` fail with `it is attached read-only`.
- **`USAGE` on the server is the only privilege checked on the database's own objects.** Owners and grants recorded in the directory belong to the other server's roles and are not used, so a view of the database reads with the privileges of the role that runs the query: a table outside the database that the view reads needs that role's `SELECT`. `has_table_privilege` and the other privilege functions report every privilege on the database's own objects. The system catalogs in its `pg_catalog` and `information_schema` keep the privileges they have in any database.
- **`DROP SERVER` only detaches.** It never removes files from the directory.

### Many readers or one writer

A database file and the catalog log beside it are locked for reading by every read-only attachment and for writing by the server that owns them, so the two exclude each other:

- Any number of read-only attachments, in one server or in several, can read the same directory at once.
- `CREATE SERVER` fails while a server runs on the data directory the database belongs to.
- A server does not start while any database directory inside its data directory is attached read-only, in this server or another.
- A server never attaches its own databases read-only, also through a symbolic link, a hard-linked copy or another mount of the same files.

The lock is the file system's: on a file system without POSIX locks nothing detects a running server, so attach a copy, a disk snapshot or the directory of a stopped server there.

`CREATE SERVER` refuses a directory:

- inside the data directory of a running server,
- without the catalog log, `catalog.wal`, beside it,
- missing the directory of one of its search tables or inverted indexes,
- whose name is not a database oid, or
- that belongs to the attaching server itself.

## Persistence and boot replay

A foreign server is stored in the catalog, so it survives a restart — including an unclean one. On boot, SereneDB replays its foreign servers and re-connects each one, so queries through the server work again without any re-`ATTACH`:

```sql
CREATE SERVER analytics FOREIGN DATA WRAPPER clickhouse_fdw
  OPTIONS (host 'clickhouse.internal', port '9000', database 'events');
-- restart serened, then:
SELECT count(*) FROM analytics.events.pageviews;   -- still works
```

If a remote is unreachable at boot, the replay for that server is skipped rather than failing startup; the catalog row remains and queries through the server error until the remote returns.

## `DROP SERVER`

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />

`DROP SERVER` removes the catalog row **and** detaches the live connection, so the name becomes immediately reusable:

```sql
DROP SERVER analytics;
```

After the drop the server is gone from `pg_foreign_server` and from `SHOW DATABASES`, and querying through it errors. `DROP SERVER IF EXISTS` on a missing server is a no-op.

`RESTRICT` is the default. `CASCADE` is accepted for PostgreSQL compatibility; because nothing in SereneDB can depend on a foreign server — there are no user mappings and no foreign tables — the two behave identically today.

### What `DROP SCHEMA` and `DROP DATABASE` do

Foreign servers are children of the **database**, not of a schema (matching PostgreSQL, whose `pg_foreign_server` has no namespace column). That determines which cascades reach them:

| Statement | Effect on foreign servers |
|---|---|
| `DROP SCHEMA … CASCADE` | None — servers are untouched and keep serving |
| `DROP DATABASE …` | Removes the database's servers and detaches their connections |
| `DROP SERVER …` | Removes that server and detaches it |

```sql
-- The server outlives the schema, cascade or not.
DROP SCHEMA public CASCADE;
SELECT count(*) FROM analytics.events.pageviews;   -- still served
```

## Catalog visibility

Created servers appear in `pg_foreign_server`, scoped to the current database:

```sql
SELECT srvname FROM pg_foreign_server WHERE srvname = 'analytics';
```

`srvowner` holds the owning role, and `srvoptions` is a `text[]` of `key=value` entries in the order they were given:

```sql
SELECT r.rolname, s.srvoptions
FROM pg_foreign_server s JOIN pg_roles r ON s.srvowner = r.oid
WHERE s.srvname = 'analytics';
```

<DocCallout type="attention">

**`pg_foreign_server` is superuser-only.** Because server `OPTIONS` carry credentials, and option values are shown **verbatim with no redaction**, the whole catalog is restricted rather than filtered — a non-superuser reading it gets `permission denied for table pg_foreign_server`. This mirrors PostgreSQL's treatment of `pg_user_mapping`, not its world-readable `pg_foreign_server`.

</DocCallout>

Two gaps are worth knowing: `srvfdw` is always `0` — the wrapper name is not currently exposed through the catalog — and `pg_foreign_data_wrapper` is an empty compatibility stub, so wrappers cannot be enumerated from SQL. See [System Table Compatibility](../../../compatibility/system-table-compatibility.md).

## Privileges

A foreign server is owned by the role that created it and carries a single privilege, `USAGE`.

| Action | Requirement |
|---|---|
| `CREATE SERVER` | `CREATE` on the current **database** |
| `DROP SERVER` | Ownership of the server, or superuser |
| Querying through the server | `USAGE` on the server (owner and superusers exempt) |
| `GRANT` / `REVOKE USAGE` | Ownership, `WITH GRANT OPTION`, or superuser |

Because servers are database children, the create gate is a **database** grant — granting `CREATE` on a schema does not allow it:

```sql
GRANT CREATE ON DATABASE shop TO analyst;
```

`USAGE` is checked at query time, so granting and revoking it takes effect on the next statement:

```sql
GRANT USAGE ON FOREIGN SERVER analytics TO analyst;
REVOKE USAGE ON FOREIGN SERVER analytics FROM analyst;
```

Without it, a query through the server fails with `permission denied for foreign server analytics`. `USAGE` is the only privilege the object accepts — `GRANT SELECT ON FOREIGN SERVER …` is rejected as an invalid privilege type.

<DocCallout type="tip">

The `USAGE` check is enforced wherever the server is referenced, including from a **different database**. The attach alias is instance-wide, so a session connected elsewhere can name the same foreign catalog — and needs `USAGE` there too.

</DocCallout>

## Differences from PostgreSQL

- **No `USER MAPPING`.** Credentials live in the server's `OPTIONS` and one shared connection serves every authorized role, the way a ClickHouse connection is normally shared. `CREATE USER MAPPING` and `DROP USER MAPPING` are syntax errors; per-role remote identities are not available.
- **No `CREATE FOREIGN TABLE`.** Remote tables are reached through the server's catalog (`server.schema.table`), discovered live, rather than declared one by one.
- **No `ALTER SERVER`, no `CREATE OR REPLACE SERVER`.** Drop and recreate to change options.
- **No `CREATE FOREIGN DATA WRAPPER`.** The wrappers are built in.
- **`pg_foreign_server` is superuser-only**, and its option values are unredacted (see above).
- **`CREATE SERVER` connects eagerly**, so it fails on an unreachable remote instead of deferring the error to first use.

## See also

- [`ATTACH` and `DETACH`](../attach/index.md) — the session-level, non-persistent alternative
- [`GRANT`](../grant/index.md) · [`REVOKE`](../revoke/index.md) · [Privileges](../../../security/privileges.md)
- [PostgreSQL integration](../../../cookbook/database_integration/postgres.md)
- [Indexing External Data](../../indexes/inverted/external-data.md) — building a search index over a foreign server's tables
