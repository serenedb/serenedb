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
| `FOREIGN DATA WRAPPER` | `USAGE` |
| `LANGUAGE` | `USAGE` |
| `PARAMETER` | `SET`, `ALTER SYSTEM` |

`ALL PRIVILEGES` grants everything applicable to the object type.

A statement's table and column privileges are checked once it is fully analyzed, so a statement that also names a column or table that does not exist reports that error rather than the missing privilege.

`USAGE` on a schema is needed to use any object in it: a role with `SELECT` on `s.t` but no `USAGE` on `s` gets `permission denied for schema s`. As in PostgreSQL, `public` grants `USAGE` to everyone, and a view reads its sources with its owner's privileges, so reading through a view needs `USAGE` only on the view's own schema. The expressions a table stores, its column defaults, `CHECK` constraints and generated columns, need no `USAGE` on the schemas they name from a role that inserts or updates rows; that role still needs `USAGE` or `UPDATE` on a sequence a default advances and `EXECUTE` on the functions they call.

`USAGE` on a [foreign server](../sql/statements/create_server/index.md#privileges) is checked whenever a query reads through it, in any database. Creating a server needs `USAGE` on its foreign-data wrapper in the database the session is connected to, which only superusers hold until one grants it with `GRANT USAGE ON FOREIGN DATA WRAPPER`. Such a grant lets the role create `postgres_fdw` and `clickhouse_fdw` servers that connect wherever their options point, with a password of their own and no secrets or server files; the [rules for servers owned by roles without `SUPERUSER`](../sql/statements/create_server/index.md#servers-owned-by-roles-without-superuser) list what such a server may set.

`USAGE` on a language lets a role create functions, procedures and macros in it; a function or macro written without a `LANGUAGE` clause is in `sql`. Each database keeps its own grants on its languages: `GRANT ... ON LANGUAGE` changes those of the current database, and creating a function checks those of the database the function goes into. `PUBLIC` holds `USAGE` on `sql` until it is revoked, for example with `REVOKE USAGE ON LANGUAGE sql FROM PUBLIC`; from then on only roles granted it, such as with `GRANT USAGE ON LANGUAGE sql TO dev`, may create functions, and anyone else gets `permission denied for language sql`. `internal` is untrusted: only superusers create functions in it, and `GRANT` and `REVOKE` refuse it with `language "internal" is not trusted`. `pg_language.lanacl` shows a language's grants and `has_language_privilege` checks them. `DROP OWNED` revokes them, and a role that holds one cannot be dropped.

`SET` on a parameter lets a role change a parameter that only superusers may change, for its session, and `ALTER SYSTEM` lets it change the parameter's server-wide value with `SET GLOBAL`. A grant names a parameter that exists, such as `GRANT SET ON PARAMETER session_replication_role TO etl`; `pg_parameter_acl` lists the parameters that have grants and `has_parameter_privilege` checks them. `DROP OWNED` revokes them, and a role that holds one cannot be dropped.

`CONNECT` on a database is checked when a session connects to it, when `USE`, `SET schema` or `SET search_path` names it, and again whenever a statement reads, creates, changes or drops anything in it, however the statement names the object: by a qualified name, through the search path, or through a function such as `seq_scan`. Revoking `CONNECT` therefore also stops access from sessions connected to other databases, including sessions whose search path already names it, and a name in a database without `CONNECT` reports `permission denied for database` whether or not the object exists. `VACUUM` and `REINDEX` need `CONNECT` on the database they work on, and `pg_database_size` needs it unless the role has the privileges of `pg_read_all_stats`, which also lets it call `pg_tablespace_size` on `pg_global`.

`TEMPORARY` on the database the session is connected to lets a role create temporary tables, views, sequences and types, including the temporary type a `PIVOT` without an `IN` list creates. Every role has it unless it is revoked from `PUBLIC`; without it, creating one reports `permission denied to create temporary tables in database "name"`. A temporary object belongs to the role that created it.

## Column privileges

`SELECT`, `INSERT` and `UPDATE` can be restricted to specific columns. A column grant does not confer the table-level privilege:

<SqlLogicTest id="security/privileges/example_004" />

A view's columns can be granted `SELECT` the same way: the grantee may read those columns of the view, and a query that names no column, such as `SELECT count(*)`, needs `SELECT` on at least one of them. `CREATE OR REPLACE VIEW` keeps the column grants of the columns it keeps.

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

`OWNER TO` works on tables, views, sequences, schemas, types, functions and procedures (`ALTER FUNCTION`, `ALTER PROCEDURE` or `ALTER ROUTINE`), databases, text search dictionaries, subscriptions, jobs and foreign servers. A role that is not a superuser must own the object and be able to `SET ROLE` to the new owner. The new owner of an object in a schema needs `CREATE` on that schema, and changing the owner of a schema or a subscription needs `CREATE` on the database. Changing the owner of a database needs the `CREATEDB` attribute. [`REASSIGN OWNED BY`](roles.md#dropping-roles) transfers every object a role owns at once.

## What other roles can see

Listings and error messages show a role only the objects it may use:

- `duckdb_tables()`, `duckdb_columns()`, `duckdb_schemas()`, `duckdb_views()` and the other catalog listings, their `sdb_*` names, `sdb_jobs()`, `sdb_job_runs()` and `es_cat_indices()` list a table or view only to a role that owns it or holds a privilege on it or on one of its columns, a schema only to a role with `USAGE` or `CREATE` on it, a sequence only to a role with a privilege on it, a function or type only to a role that may use it, and a job and its runs only to its owner.
- Objects of a database the role has no `CONNECT` on, of a foreign server it has no `USAGE` on, and of a database attached without a foreign server are left out.
- A foreign server's options, which can hold its password, are shown only to superusers: `pg_foreign_server` is superuser-only, and `information_schema.foreign_server_options` shows other roles the option names without their values.
- A `Did you mean` suggestion after a mistyped name names only objects the role could see in those listings, and after a mistyped parameter only parameters `duckdb_settings()` lists to the role.
- An error about a column of a table or view the role cannot see reports `permission denied` for that table or view instead, so errors never list its columns.
- `PRAGMA table_info` and `pragma_table_info()` describe a table or view only to a role that could see it in those listings, and report `permission denied` for it to any other role.
- `DESCRIBE` needs the privileges its query needs to run: `DESCRIBE t` needs `SELECT` on every column of `t`, as `SELECT * FROM t` does.
- A primary key, unique or foreign key violation names the offending key only to a role that may read every indexed column of the table the statement writes; any other role gets the same error without the key, for example `PRIMARY KEY or UNIQUE constraint violation: duplicate key`.
- A database-wide `VACUUM` warns only about the tables the role can see when it skips them.

## Predefined roles

Every cluster has the predefined roles of PostgreSQL. A superuser, or a role with the `ADMIN` option on one of them, grants them like any other role, `GRANT pg_read_all_data TO analyst`, and a member that inherits from them gains:

| Role | Grants |
|---|---|
| `pg_read_all_data` | `SELECT` on every table, view and sequence and `USAGE` on every schema, as if granted on each. |
| `pg_write_all_data` | `INSERT`, `UPDATE` and `DELETE` on every table, view and sequence and `USAGE` on every schema. |
| `pg_maintain` | `MAINTAIN` on every table, so `VACUUM` and `ANALYZE` process it. |
| `pg_read_all_settings` | Reading every parameter with `SHOW` and `current_setting`, also those only superusers may read. |
| `pg_read_all_stats` | The queries and details of every session in `pg_stat_activity`, `pg_stat_get_activity()` and `sdb_progress`, and `pg_database_size` and `pg_tablespace_size` on every database and tablespace. |
| `pg_monitor` | Membership in `pg_read_all_settings`, `pg_read_all_stats` and `pg_stat_scan_tables`. |
| `pg_signal_backend` | `pg_cancel_backend` and `pg_terminate_backend` on the sessions of roles that are not superusers. |
| `pg_checkpoint` | `CHECKPOINT`, `FORCE CHECKPOINT` and the functions `checkpoint` and `force_checkpoint`. |
| `pg_create_subscription` | `CREATE SUBSCRIPTION`, together with `CREATE` on the database. |
| `pg_read_server_files` | Reading [server files and URLs](#server-files-and-urls). |
| `pg_write_server_files` | Writing [server files and URLs](#server-files-and-urls). |
| `pg_database_owner` | Its only member is the owner of the database the session is connected to: privileges granted to it apply to that owner, and no role can be granted it or be granted to it. It owns the `public` schema a database is created with, so the database's owner may create objects in it; other roles get `USAGE` on it only. |

`pg_execute_server_program`, `pg_stat_scan_tables`, `pg_use_reserved_connections` and `pg_signal_autovacuum_worker` exist for compatibility and grant nothing. A predefined role cannot be dropped or altered, and no other role name may start with `pg_`.

## Server files and URLs

Reading server files and URLs requires a superuser or a member of `pg_read_server_files`, and writing them a superuser or a member of `pg_write_server_files`:

- `COPY ... FROM` a file reads it and `COPY ... TO` a file writes it; `COPY ... FROM STDIN` and `COPY ... TO STDOUT` work for everyone.
- Every function that reads files or URLs reads them, such as `read_csv`, `read_parquet`, `read_json`, `read_text`, `read_blob` and `glob`.
- The `stopwords_path` and `model_location` options of a [text search dictionary](../sql/statements/create_text_search_dictionary/index.md) and the `passfile`, `service`, `sslcert`, `sslkey`, `sslrootcert`, `sslcrl` and `sslcrldir` options of a subscription's connection string read server files.

The `save_location` of `enable_profiling` sets the `profiling_output` parameter, which only superusers may change.

A function reads with the [secrets](../configuration/secrets_manager.md#who-uses-secrets) of the Secrets Manager only for a superuser.

## Superuser-only operations

These require a superuser, whatever privileges a role holds:

- `ATTACH` (except the form `CREATE DATABASE` runs), `DETACH`, `CONNECT`, `COPY FROM DATABASE`, `DROP SECRET`, the external resource statements, and the pragmas `enable_checkpoint_on_shutdown`, `disable_checkpoint_on_shutdown` and `add_parquet_key`. `CHECKPOINT`, `FORCE CHECKPOINT` and the pragma `force_checkpoint` also run for members of `pg_checkpoint`.
- The functions `enable_logging`, `disable_logging`, `truncate_duckdb_logs`, `duckdb_logs`, `duckdb_log_contexts`, `duckdb_secrets`, `which_secret`, `duckdb_databases`, `duckdb_extensions`, `duckdb_temporary_files`, `duckdb_external_file_cache`, `duckdb_table_sample`, `pragma_storage_info`, `pragma_metadata_info`, `pragma_database_size`, `json_serialize_plan`, `dbgen`, `dsdgen`, `iceberg_to_ducklake`, `iceberg_load_table_response`, the external resource functions, and the database connector functions that take a connection or run SQL on another server (`postgres_query`, `postgres_execute`, `postgres_scan`, `postgres_attach`, `clickhouse_query`, `clickhouse_execute`, `clickhouse_scan` and their relatives), together with the `sdb_*` names of these functions. The check is made against the role running the query, also when the function is called inside a view.
- Using an AI secret other than the server-wide default named by `sdb_ai_text_default_secret`, `sdb_ai_embedding_default_secret` or `sdb_ai_system_one_default_secret`.

A view reads files and URLs with its owner's authority, as it reads tables: a role with `SELECT` on a view reads the files the view reads when the view's owner may read them, and a view created `WITH (security_invoker = true)` reads them with the authority of the role running the query. Reading the tables of a [foreign server](../sql/statements/create_server/index.md#privileges) needs only `USAGE` on the server.

The functions a view calls are checked and run for the role running the query, not for the view's owner: that role needs `EXECUTE` on them, and an SQL function reads tables with its privileges unless the function is [`SECURITY DEFINER`](../sql/statements/create_macro/index.md#sql-functions).

A table that has no owner, such as one created by `dbgen`, can be used only by a superuser, and so can a database a superuser attaches without a foreign server, such as a DuckDB file.

## Notes

- Granting a privilege requires owning the object or holding it `WITH GRANT OPTION`.
- Superusers bypass all privilege checks.
- `ALTER DEFAULT PRIVILEGES` sets the privileges that tables, sequences, functions and types created afterwards receive, in every schema or only `IN SCHEMA` one, for the objects of the current role or `FOR ROLE` another. The database-wide setting replaces the built-in defaults, so `ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC` creates functions and procedures that only their owner may run; a schema's setting only adds privileges to the database-wide ones and cannot take any away. An object created in another database by a qualified name, such as `CREATE SCHEMA other_db.s`, gets the default privileges of the database it is created in.

## See also

- [GRANT](../sql/statements/grant/index.md) — full syntax, including the membership form
- [REVOKE](../sql/statements/revoke/index.md) — removing privileges
- [Database roles](roles.md) — the identities privileges are granted to
