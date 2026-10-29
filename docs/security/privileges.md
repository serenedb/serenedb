---
title: Privileges
sidebar_position: 4
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Every database object has an owner and an access-control list. The owner — normally the role that created the object — holds every privilege on it implicitly; everyone else has only what has been granted to them (directly, through [role membership](role_membership.md), or through `PUBLIC`).

The owner holds all privileges from the start:

<SqlLogicTest id="security/privileges/example_001" />

## Granting privileges

[`GRANT`](../sql/statements/grant/index.md) gives a privilege on an object to a role, and the `has_*_privilege` functions inspect the result:

<SqlLogicTest id="security/privileges/example_002" />

Privileges are enforced on every query. Acting as the role, reading works and writing is refused:

<SqlLogicTest id="security/privileges/example_003" />

## Available privileges

| Object | Privileges |
|---|---|
| `TABLE` (default) | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER`, `MAINTAIN` |
| `SEQUENCE` | `USAGE`, `SELECT`, `UPDATE` |
| `FUNCTION` | `EXECUTE` |
| `DATABASE` | `CREATE`, `CONNECT`, `TEMPORARY` |
| `SCHEMA` | `CREATE`, `USAGE` |
| `TYPE` | `USAGE` |
| `FOREIGN SERVER` | `USAGE` |

`ALL PRIVILEGES` grants everything applicable to the object type.

`USAGE` on a schema is needed to use any object in it: a role with `SELECT` on `s.t` but no `USAGE` on `s` gets `permission denied for schema s`. As in PostgreSQL, `public` grants `USAGE` to everyone, and a view reads its sources with its owner's privileges, so reading through a view needs `USAGE` only on the view's own schema.

`USAGE` on a [foreign server](../sql/statements/create_server/index.md#privileges) is checked whenever a query reads through it, in any database. Creating a server is gated on `CREATE` on the **database**, not on a schema; a server of the `duckdb_fdw` or `serenedb_fdw` wrapper names a server file and can be created only by a superuser.

`CONNECT` on a database is checked when a session connects to it, when `USE` switches to it, and again whenever a statement reads or writes an object of that database by a qualified name, so revoking `CONNECT` also stops access from sessions connected to other databases. `pg_database_size`, `VACUUM` and `REINDEX` need `CONNECT` on the database they work on.

## Column privileges

`SELECT`, `INSERT` and `UPDATE` can be restricted to specific columns. A column grant does not confer the table-level privilege:

<SqlLogicTest id="security/privileges/example_004" />

## Grant options

With `WITH GRANT OPTION`, the grantee may pass the privilege on to others:

<SqlLogicTest id="security/privileges/example_005" />

### Reading the ACL

An object's access-control list is stored in its catalog `*acl` column (`relacl` for tables) as an array of `aclitem`s in PostgreSQL's format — `grantee=privileges/grantor`, where an empty grantee means `PUBLIC` and a trailing `*` marks a privilege held **with grant option**:

<SqlLogicTest id="security/privileges_advanced/example_001" />

The grantee can then pass the privilege on:

<SqlLogicTest id="security/privileges_advanced/example_002" />

## PUBLIC

`PUBLIC` is a pseudo-role meaning every role, present and future:

<SqlLogicTest id="security/privileges/example_006" />

## Revoking

[`REVOKE`](../sql/statements/revoke/index.md) removes grants — but only the grants the revoker (or a role it can act for) made. Revoking a direct grant does not remove access that still flows from `PUBLIC`, and a grant made by another grantor survives until that grantor revokes it:

<SqlLogicTest id="security/privileges/example_007" />

<DocCallout type="tip">

If a role still has access after a `REVOKE`, check where it flows from: a `PUBLIC` grant, a group membership, or a grant made by a different grantor. `has_table_privilege` reflects the combined result of all of them.

</DocCallout>

### Dependent grants: RESTRICT and CASCADE

When a privilege was passed on via `WITH GRANT OPTION`, those onward grants depend on it. By default (`RESTRICT`) `REVOKE` refuses while dependents exist:

<SqlLogicTest id="security/privileges_advanced/example_003" />

`CASCADE` revokes the privilege and every grant that depended on it, in one step:

<SqlLogicTest id="security/privileges_advanced/example_004" />

## Ownership

Ownership can be transferred with `ALTER ... OWNER TO`. The new owner immediately holds every privilege on the object, including the right to drop it:

<SqlLogicTest id="security/privileges/example_008" />

## What other roles can see

Listings and error messages show a role only the objects it may use:

- `duckdb_tables()`, `duckdb_columns()`, `duckdb_schemas()`, `duckdb_views()` and the other catalog listings, their `sdb_*` names, `sdb_jobs()`, `sdb_job_runs()` and `es_cat_indices()` list a table or view only to a role that owns it or holds a privilege on it or on one of its columns, a schema only to a role with `USAGE` or `CREATE` on it, a sequence only to a role with a privilege on it, a function or type only to a role that may use it, and a job and its runs only to its owner.
- Objects of a database the role has no `CONNECT` on, and of a foreign server it has no `USAGE` on, are left out.
- A `Did you mean` suggestion after a mistyped name names only objects the role could see in those listings.
- An error about a column of a table or view the role cannot see reports `permission denied` for that table or view instead, so errors never list its columns.
- A database-wide `VACUUM` warns only about the tables the role can see when it skips them.

## Superuser-only operations

These require a superuser, whatever privileges a role holds:

- Reading or writing server files and URLs: `COPY ... FROM` or `TO` a file, and every function that reads files or URLs, such as `read_csv`, `read_parquet`, `read_json`, `read_text`, `read_blob` and `glob`. `COPY ... FROM STDIN` and `COPY ... TO STDOUT` work for everyone. The same rule applies to the `stopwords_path` and `model_location` options of a [text search dictionary](../sql/statements/create_text_search_dictionary/index.md), to the `passfile`, `service`, `sslcert`, `sslkey`, `sslrootcert`, `sslcrl` and `sslcrldir` options of a subscription's connection string, and to the `save_location` of `enable_profiling`.
- `ATTACH` (except the form `CREATE DATABASE` runs), `DETACH`, `CONNECT`, `COPY FROM DATABASE`, `CHECKPOINT`, `FORCE CHECKPOINT`, `DROP SECRET`, the external resource statements, and the pragmas `force_checkpoint`, `enable_checkpoint_on_shutdown`, `disable_checkpoint_on_shutdown` and `add_parquet_key`.
- The functions `checkpoint`, `force_checkpoint`, `enable_logging`, `disable_logging`, `truncate_duckdb_logs`, `duckdb_logs`, `duckdb_log_contexts`, `duckdb_secrets`, `which_secret`, `duckdb_databases`, `duckdb_temporary_files`, `duckdb_external_file_cache`, `duckdb_table_sample`, `pragma_storage_info`, `pragma_metadata_info`, `pragma_database_size`, `json_serialize_plan`, `dbgen`, `dsdgen`, `iceberg_to_ducklake`, the external resource functions, and the database connector functions that take a connection or run SQL on another server (`postgres_query`, `postgres_execute`, `postgres_scan`, `postgres_attach`, `clickhouse_query`, `clickhouse_execute`, `clickhouse_scan` and their relatives), together with the `sdb_*` names of these functions. The check is made against the role running the query, also when the function is called inside a view.
- Using an AI secret other than the server-wide default named by `sdb_ai_text_default_secret`, `sdb_ai_embedding_default_secret` or `sdb_ai_system_one_default_secret`.

A view reads files and URLs with its owner's authority, as it reads tables: a role with `SELECT` on a view owned by a superuser reads the files the view reads, a view owned by any other role reads no files, and a view created `WITH (security_invoker = true)` reads them with the authority of the role running the query. Reading the tables of a [foreign server](../sql/statements/create_server/index.md#privileges) needs only `USAGE` on the server.

A table that has no owner, such as one created by `dbgen`, can be used only by a superuser.

## Notes

- Granting a privilege requires owning the object or holding it `WITH GRANT OPTION`.
- Superusers bypass all privilege checks.
- `ALTER DEFAULT PRIVILEGES` is accepted and stored for PostgreSQL compatibility but is not yet applied to newly created objects.

## See also

- [GRANT](../sql/statements/grant/index.md) — full syntax, including the membership form
- [REVOKE](../sql/statements/revoke/index.md) — removing privileges
- [Database roles](roles.md) — the identities privileges are granted to
