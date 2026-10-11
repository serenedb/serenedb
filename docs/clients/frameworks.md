---
title: ORMs & Tools
sidebar_position: 15
split: headings
---

# ORMs, Migration Tools and Database Tools

The frameworks and tools on this page have test suites in the SereneDB repository that run in CI. Every suite runs the same checks against SereneDB and against PostgreSQL 18. A check that fails on SereneDB is kept as an expected failure linked to its issue, so the tables below list what the tests show.

- **Partial**: connecting, reading the schema, queries, inserts, updates and deletes work. The features under *Not yet* fail.
- **Not yet**: the tool's main workflow fails.

Most failures come from a few PostgreSQL features that SereneDB doesn't support yet. [Common gaps](#common-gaps) lists them with workarounds.

## Python

| Tool | Version | Support | Not yet |
|---|---|---|---|
| Django | 6.1 | Partial | `migrate` of the built-in apps, nested `atomic()` blocks and saving in the admin (savepoints), `select_for_update()`, `QuerySet.iterator()` and `dumpdata` (server-side cursors), `loaddata`, `annotate()` grouped by a primary key, deleting a parent row after its children in one transaction, `JSONField` |
| SQLAlchemy | 2.1 | Partial | `begin_nested()`, `Identity()`, domains, materialized views, partitioned tables, foreign key actions, foreign keys to another schema, index options such as `postgresql_using`, temporary tables in the inspector |
| Alembic | 1.20 | Partial | `create_foreign_key`, batch mode, changing the type of a column a constraint uses, DDL on tables that foreign keys reference. Autogenerate reports `String(n)` columns as changed, and `Float` columns are created as `real` |
| dbt (dbt-postgres) | 1.11 | Partial | Snapshots don't detect changed rows: dbt reads no columns of its temporary tables |
| pgcli | 4.7 | Partial | Completion of tables, views and columns, `\sv` and `\sf`, `EXPLAIN` mode |
| pgAdmin 4 | 9.18 | Partial | Properties pages of tables, views, functions and sequences, editing query results. Reading the role's privileges on connect fails |
| psqlODBC | 18.00 | Partial | Reusing a statement handle with server-side prepare (set `UseServerSidePrepare=0`), `SQLPrimaryKeys`, `SQLSpecialColumns` |
| pg_dump, pg_restore | 18 | Not yet | Dumping a SereneDB database. See [Restoring a PostgreSQL dump](#restoring-a-postgresql-dump) |

Django's `migrate` fails on the built-in apps because they create identity columns ([#1426](https://github.com/serenedb/serenedb/issues/1426)), add foreign keys with `ALTER TABLE` ([#1427](https://github.com/serenedb/serenedb/issues/1427)) and declare constraints `DEFERRABLE` ([#1381](https://github.com/serenedb/serenedb/issues/1381)). Once the tables exist, the ORM, the admin's pages and `inspectdb` work. Forms with model choice fields, such as the admin's change forms, read their choices with `QuerySet.iterator()`, which opens a server-side cursor, so turn server-side cursors off:

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": "localhost",
        "PORT": 7890,
        "USER": "postgres",
        "NAME": "postgres",
        "DISABLE_SERVER_SIDE_CURSORS": True,
    }
}
```

## Java

| Tool | Version | Support | Not yet |
|---|---|---|---|
| pgjdbc `DatabaseMetaData` | 42.7 | Partial | Column origin of result sets (`getTableName`, `getColumnName` on `ResultSetMetaData`), index sort order, `LISTEN`/`NOTIFY`, large objects |
| Hibernate ORM | 7.4 | Partial | `GenerationType.IDENTITY`, schema export of foreign keys, `time(p)` columns, `boolean` parameters and dates with an offset ([#1449](https://github.com/serenedb/serenedb/issues/1449)), pessimistic locks, bulk updates over joins (`ctid`), updating a unique column of a row that a foreign key references |
| Flyway | 13.10 | Not yet | `migrate` needs advisory locks ([#1442](https://github.com/serenedb/serenedb/issues/1442)); `clean` can't drop tables that foreign keys reference |
| Liquibase | 5.0 | Partial | `autoIncrement` columns, `addForeignKeyConstraint`, `dropAll`, renaming columns of tables that foreign keys reference |

With Hibernate, use `GenerationType.SEQUENCE` instead of `GenerationType.IDENTITY`.

## JavaScript

| Tool | Version | Support | Not yet |
|---|---|---|---|
| Prisma | 7.10 | Partial | Prisma Migrate, `db pull` and `db push` (the schema engine binds a list to `= ANY($1)`, [#1448](https://github.com/serenedb/serenedb/issues/1448), and locks with advisory locks), the default relation mode (foreign keys), `Json` fields, `Time`, `Timetz` and `Timestamptz` precision, enum fields in the records `create`, `update` and `upsert` return, atomic number operations, scalar list `push`, `Serializable` transactions |
| TypeORM | 1.1 | Partial | Foreign keys in migrations, `uuid` keys generated with `uuid-ossp`, `jsonb` columns, nested transactions, pessimistic locks |
| Sequelize | 6.37 | Partial | Connecting with the default `timezone: "+00:00"` (set a named zone such as `"Etc/UTC"`), `sync({ alter: true })` and `ENUM` columns (they use `DO` blocks), `findOrCreate`, `onDelete`/`onUpdate` actions, timestamps of paranoid models, `Serializable` transactions, nested transactions, row locks |
| Knex | 3.3 | Partial | Foreign keys added to existing tables, nested transactions, `forUpdate()`/`forShare()` |
| Drizzle ORM, drizzle-kit | 0.45, 0.31 | Partial | `drizzle-kit push` and `pull`, relational queries (they aggregate with `json_agg`), nested transactions, row locks |
| Kysely | 0.29 | Partial | The migrator (advisory locks), `jsonArrayFrom`, updates that add a parameter to a column and return rows ([#1448](https://github.com/serenedb/serenedb/issues/1448)), savepoints, row locks |
| postgres-meta | 0.100 | Not yet | Listing tables, types, functions, indexes, relationships and privileges, TypeScript type generation |

## C\#

| Tool | Version | Support | Not yet |
|---|---|---|---|
| EF Core (Npgsql) | 10.0 | Partial | `Migrate()` (needs `LOCK TABLE`, [#724](https://github.com/serenedb/serenedb/issues/724)), idempotent migration scripts and `EnsureSchema` (`DO` blocks), `EnsureDeleted()`, cascade deletes, JSON columns, `xmin` row versions, the translations of `Regex.IsMatch`, `string.Trim()` and `DateTime.Date`, `Average` of `decimal` ([#1435](https://github.com/serenedb/serenedb/issues/1435)), LINQ over an array parameter such as `ids.Where(...).Contains(...)` (it becomes `unnest`), savepoints, `Serializable` transactions, row locks |
| Npgsql | 10.0 | Partial | Binary `COPY` export ([#1452](https://github.com/serenedb/serenedb/issues/1452)), enum parameters outside a transaction, `LISTEN`/`NOTIFY` |

Npgsql creates keys as identity columns by default. Call `UseSerialColumns()` in `OnModelCreating` to create `serial` keys instead:

```csharp
protected override void OnModelCreating(ModelBuilder model) => model.UseSerialColumns();
```

## Go

| Tool | Version | Support | Not yet |
|---|---|---|---|
| GORM | 1.31 | Partial | Re-running `AutoMigrate` (sized `varchar` columns read back as `text`), nested creates with `ON CONFLICT` (children attach to the wrong parents, [#1451](https://github.com/serenedb/serenedb/issues/1451)), soft-deleting a referenced row, `constraint:OnDelete:CASCADE`, nested transactions, row locks |
| golang-migrate | 4.20 | Not yet | Migrations lock the database with `pg_advisory_lock` |
| goose | 3.28 | Not yet | The version table uses an identity column |
| Atlas | 1.3 | Not yet | Schema inspection aggregates with `jsonb_object_agg` |

## Rust

| Tool | Version | Support | Not yet |
|---|---|---|---|
| SQLx | 0.9 | Partial | The migrator's default locking (advisory locks), nullability inference of `query!` macros (`EXPLAIN (VERBOSE, FORMAT JSON)`), nested transactions, `LISTEN`/`NOTIFY` |
| sea-schema | 0.18 | Partial | Discovery reads `varchar(n)` columns as `text` ([#1434](https://github.com/serenedb/serenedb/issues/1434)) |

SQLx's migrator runs when locking is turned off with `Migrator::set_locking(false)`.

## Ruby

| Tool | Version | Support | Not yet |
|---|---|---|---|
| Active Record | 8.1 | Partial | Reading indexes and foreign keys ([#1447](https://github.com/serenedb/serenedb/issues/1447)), so the schema dump, `index_exists?`, `foreign_key_exists?`, `remove_index`, `rename_column`, `insert_all` and `upsert_all` fail. Also: migrations with advisory locks (set `advisory_locks: false`), counter caches, case-insensitive uniqueness validation, `create_enum`, `jsonb`, `change_column` with `default:` and `null:`, `rename_table`, DDL on tables that foreign keys reference, `requires_new` transactions, `lock`, loading fixtures, ISO 8601 intervals |
| Sequel | 5.109 | Partial | `Dataset#insert` returns `nil`: Sequel reads the primary key from `pg_index.indkey[0]` ([#1459](https://github.com/serenedb/serenedb/issues/1459)). Also: `primary_key` without `serial: true` (identity columns), `alter_table` with several operations, foreign key actions, `DEFERRABLE`, `use_cursor` (cursors), savepoints, row locks, `Serializable` transactions, `LISTEN`/`NOTIFY` |

## PHP

| Tool | Version | Support | Not yet |
|---|---|---|---|
| Doctrine DBAL | 4.5 | Partial | `autoincrement` columns (identity), `lastInsertId()` without a sequence name (`lastval()`), `time` and `datetimetz` precision, savepoints, `Serializable` transactions, row locks |
| Laravel (illuminate/database) | 13.35 | Partial | The `date` cast ([#1449](https://github.com/serenedb/serenedb/issues/1449)), `foreignId()->constrained()`, `->change()`, `time` and `timestampTz` columns, `jsonb()` columns, `fullText()` indexes, savepoints, row locks, renaming a table that foreign keys reference, deleting a parent row after its children in one transaction |

## Database GUIs and API servers

CI replays the catalog queries these tools send while browsing or syncing a database. The queries come from each tool's source code.

| Tool | Version | Not yet |
|---|---|---|
| DBeaver | 26.2 | Reading a sequence's state with `SELECT ... FROM <sequence>` |
| pgweb | 0.17 | — |
| Metabase | 0.64 | — |
| Directus | 12.5 | — |
| Grafana | 13.0 | The query editor's table and column lists |
| Beekeeper Studio | 6.1 | Foreign key lists ([#1447](https://github.com/serenedb/serenedb/issues/1447)) |
| Adminer | 6.1 | Index details ([#1447](https://github.com/serenedb/serenedb/issues/1447)), routine and trigger lists (the regex form of `substring`, [#1454](https://github.com/serenedb/serenedb/issues/1454)), editing rows (`ctid`) |
| Hasura | 2.50 | Table and function metadata (column definition lists, [#1447](https://github.com/serenedb/serenedb/issues/1447)) |
| PostgREST | 16.4 | Schema cache (`json_agg`, `pg_basetype`), `SET search_path = ''`, per-request settings such as `request.jwt.claims` ([#1456](https://github.com/serenedb/serenedb/issues/1456)) |

## Common gaps

| PostgreSQL feature | Affects | Workaround | Issue |
|---|---|---|---|
| Identity columns (`GENERATED ... AS IDENTITY`) | Django, SQLAlchemy `Identity()`, Hibernate `IDENTITY`, EF Core, Liquibase, Doctrine DBAL, Sequel, goose | Use `serial` or `bigserial` keys | [#1426](https://github.com/serenedb/serenedb/issues/1426) |
| `ALTER TABLE ... ADD FOREIGN KEY` | Migrations of nearly every ORM | Declare foreign keys in `CREATE TABLE` | [#1427](https://github.com/serenedb/serenedb/issues/1427) |
| Savepoints | Nested transactions of every ORM | Avoid nested transactions | [#722](https://github.com/serenedb/serenedb/issues/722) |
| `SELECT ... FOR UPDATE` / `FOR SHARE` | Row-locking APIs of every ORM | — | [#1431](https://github.com/serenedb/serenedb/issues/1431) |
| Advisory locks | Flyway, Prisma Migrate, golang-migrate, goose, Active Record, Kysely, SQLx | — | [#1442](https://github.com/serenedb/serenedb/issues/1442) |
| Foreign key actions (`ON DELETE CASCADE`, `SET NULL`, `SET DEFAULT`) | EF Core, Sequelize, GORM, Active Record, Sequel, SQLAlchemy | Delete dependent rows in the application | [#1439](https://github.com/serenedb/serenedb/issues/1439) |
| DDL on a table that a foreign key references | Flyway `clean`, Liquibase, Alembic, Active Record, Sequel, Laravel | — | [#1440](https://github.com/serenedb/serenedb/issues/1440) |
| Deleting a parent row in the transaction that deleted its children | Django, Hibernate, EF Core, GORM, SQLAlchemy, Active Record, Sequel, Laravel | Commit the deletes of the children first | [#1433](https://github.com/serenedb/serenedb/issues/1433) |
| Updating an indexed column of a row that a foreign key references | Hibernate, GORM soft deletes | — | [#1441](https://github.com/serenedb/serenedb/issues/1441) |
| `DEFERRABLE` constraints, `SET CONSTRAINTS` | Django, Active Record, Sequel | — | [#1381](https://github.com/serenedb/serenedb/issues/1381) |
| `DECLARE ... CURSOR` | Django, Sequel | Django: `DISABLE_SERVER_SIDE_CURSORS` | [#720](https://github.com/serenedb/serenedb/issues/720) |
| `jsonb` | Django, EF Core, TypeORM, Prisma, Active Record, Sequel, Laravel, Atlas | `json` columns | [#613](https://github.com/serenedb/serenedb/issues/613) |
| Type modifiers (`time(p)`, `timestamptz(p)`; `varchar(n)` reads back as `text`) | Hibernate, Prisma, Doctrine DBAL, Laravel, Sequelize, GORM, Alembic | — | [#1434](https://github.com/serenedb/serenedb/issues/1434), [#1437](https://github.com/serenedb/serenedb/issues/1437) |
| `float` creates `real` | Alembic, Active Record | Declare `double precision` | [#1438](https://github.com/serenedb/serenedb/issues/1438) |
| `SERIALIZABLE` isolation | EF Core, Prisma, Sequelize, Sequel, Doctrine DBAL | — | [#1444](https://github.com/serenedb/serenedb/issues/1444) |
| `DO` blocks, PL/pgSQL | EF Core, Sequelize, Active Record | — | [#1443](https://github.com/serenedb/serenedb/issues/1443) |
| `CREATE INDEX ... USING btree` and index options | Drizzle, SQLAlchemy, PostgreSQL dumps | Leave out `USING btree` | [#1445](https://github.com/serenedb/serenedb/issues/1445) |
| Several subcommands in one `ALTER TABLE` | Django, Active Record, Sequel, Laravel | One subcommand per statement | [#1430](https://github.com/serenedb/serenedb/issues/1430) |
| Dropping a column when an index covers it or a later column | Django, EF Core, Active Record, Sequel | — | [#1428](https://github.com/serenedb/serenedb/issues/1428) |
| Changing the type of a `PRIMARY KEY` or `UNIQUE` column | Django, EF Core, Active Record, Sequel, Alembic | — | [#1429](https://github.com/serenedb/serenedb/issues/1429) |
| `GROUP BY` a primary key without the other selected columns | Django, Active Record, Sequel | List every selected column in `GROUP BY` | [#1432](https://github.com/serenedb/serenedb/issues/1432) |
| Types of untyped parameters | pgcli, Prisma, GORM, Active Record, EF Core, pgx | Cast parameters (`$1::int`) | [#1448](https://github.com/serenedb/serenedb/issues/1448) |
| `DEALLOCATE` of protocol-prepared statements | psqlODBC, PDO, Laravel | psqlODBC: `UseServerSidePrepare=0` | [#1436](https://github.com/serenedb/serenedb/issues/1436) |
| Missing functions (`json_agg`, `parse_ident`, `pg_basetype`, `btrim`, `lastval`, ...) | Drizzle, Kysely, postgres-meta, PostgREST, Grafana, EF Core, Doctrine DBAL | — | [#1454](https://github.com/serenedb/serenedb/issues/1454) |
| `LISTEN` / `NOTIFY` | pgjdbc, Npgsql, pgx, SQLx, Sequel | — | [#723](https://github.com/serenedb/serenedb/issues/723) |
| `NULL` values first in `DESC` order | Active Record, Sequel | `SET default_null_order = 'NULLS_LAST_ON_ASC_FIRST_ON_DESC'` | [ORDER BY](../sql/query_syntax/orderby/index.md) |

## Restoring a PostgreSQL dump

`pg_dump` can't dump a SereneDB database, and a PostgreSQL dump doesn't restore as written: its first statements set `search_path` to `''`, and SereneDB can't resolve schema-qualified names after that ([#1446](https://github.com/serenedb/serenedb/issues/1446)). Replace that line before restoring a plain-format dump:

```sh
sed "s/^SELECT pg_catalog.set_config('search_path', '', false);$/SET search_path = public, pg_catalog;/" dump.sql \
  | psql -h localhost -p 7890 -U postgres -d mydb
```

The remaining errors come from `CREATE INDEX ... USING btree` ([#1445](https://github.com/serenedb/serenedb/issues/1445)), `ALTER TABLE ... ADD FOREIGN KEY` ([#1427](https://github.com/serenedb/serenedb/issues/1427)) and `ALTER FUNCTION ... OWNER TO`.
