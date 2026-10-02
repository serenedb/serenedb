---
title: Row-Level Security
sidebar_position: 6
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

[Privileges](privileges.md) decide *whether* a role may read or change a table. Row-level security (RLS) decides *which rows*: once it is enabled on a table, every query sees only the rows a **policy** lets through, and every write must produce rows a policy accepts. The rules follow PostgreSQL.

## Enabling row-level security

`ALTER TABLE ... ENABLE ROW LEVEL SECURITY` switches a table to default-deny. A role that is neither the owner nor a superuser sees no rows at all until a policy grants them:

<SqlLogicTest id="security/row_level_security/example_001" />

`DISABLE ROW LEVEL SECURITY` turns the policies off again (they are kept, just not applied).

## Policies

`CREATE POLICY` adds a rule to a table. The `USING` expression decides which existing rows are visible; it is evaluated per row and may reference the table's columns, functions such as `current_user`, and subqueries:

<SqlLogicTest id="security/row_level_security/example_002" />

```sql
CREATE POLICY name ON table
    [ AS { PERMISSIVE | RESTRICTIVE } ]
    [ FOR { ALL | SELECT | INSERT | UPDATE | DELETE } ]
    [ TO { role | PUBLIC | CURRENT_USER | SESSION_USER } [, ...] ]
    [ USING ( expression ) ]
    [ WITH CHECK ( expression ) ]
```

- `FOR` picks the commands the policy applies to (default `ALL`). `INSERT` policies take only `WITH CHECK`; `SELECT` and `DELETE` policies take only `USING`.
- `TO` picks the roles (default `PUBLIC`). Membership counts: a policy for a group role applies to its members.
- Permissive policies (the default) are combined with `OR`; restrictive policies are combined with `AND` on top of them. With no permissive policy for a command, nothing is allowed.
- Column privileges are checked on the columns the query itself uses, not on the table's columns a policy compares: a role that may select only `id` still sees its rows of a table whose policy tests `owner`. Tables a policy's subqueries read still need the role's `SELECT`.

<SqlLogicTest id="security/row_level_security/example_003" />

`ALTER POLICY` renames a policy or replaces its roles, `USING` or `WITH CHECK`; `DROP POLICY [IF EXISTS]` removes it. Policies are transactional (a rolled-back `CREATE POLICY` leaves nothing behind), survive restarts, follow `RENAME TABLE` and `RENAME COLUMN`, and are dropped with their table. Dropping or retyping a column a policy uses is refused unless `CASCADE` is given, which drops the policy too. Only the table owner (or a superuser) may create, alter or drop policies and enable or disable RLS.

## Writes

`WITH CHECK` is verified on every row an `INSERT` or `UPDATE` produces; when a policy has no `WITH CHECK`, its `USING` expression is used. A row that fails is rejected with an error, it is never silently dropped. `UPDATE` and `DELETE` only touch rows their `USING` policies make visible; when the statement also reads the rows (a `WHERE`, `RETURNING` or a `SET` that references columns), the `SELECT` policies apply too, and the rows an `UPDATE` writes must pass them as well:

<SqlLogicTest id="security/row_level_security/example_004" />

`MERGE` sees only the target rows its `SELECT` policies show; the others count as not matched. A matched row that an `UPDATE` or `DELETE` action takes must pass that command's `USING` policies, otherwise the statement fails with `target row violates row-level security policy`. The rows the actions write are checked like those of `INSERT` and `UPDATE`.

`INSERT ... ON CONFLICT` checks every proposed row against the `INSERT` policies before looking for conflicts, so a row that conflicts is checked too; with a conflict target or `DO UPDATE`, it must also pass the `SELECT` policies. Conflicts are found among all rows, visible or not: `DO NOTHING` skips them, and `DO UPDATE` requires the existing row to pass the `UPDATE` and `SELECT` policies:

<SqlLogicTest id="security/row_level_security/example_010" />

## Who bypasses the policies

- Superusers and roles with the `BYPASSRLS` attribute are never subject to policies.
- The table owner is exempt too, unless the table has `FORCE ROW LEVEL SECURITY` (undo with `NO FORCE ROW LEVEL SECURITY`).

<SqlLogicTest id="security/row_level_security/example_005" />

Views run with their owner's rights, so the policies of the tables inside a view are those that apply to the view owner. A view created `WITH (security_invoker = true)` applies the querying role's policies instead. Prepared statements re-check the policies on every execution, so `SET ROLE` between `PREPARE` and `EXECUTE` is always honoured.

## Policies on views

A view can carry policies of its own. They filter the rows the view returns and apply to the role that queries the view, while the tables inside the view keep following the rules above. `ALTER VIEW ... ENABLE ROW LEVEL SECURITY` (and `DISABLE`, `FORCE`, `NO FORCE`) works as for tables, and the same roles bypass the policies: superusers, `BYPASSRLS` roles and the view owner unless the view has `FORCE ROW LEVEL SECURITY`. Views are read-only, so their policies are `FOR SELECT` or `FOR ALL` and take only a `USING` expression:

<SqlLogicTest id="security/row_level_security/example_009" />

An [inverted index over a view](../sql/indexes/inverted/views.md) that is queried directly applies the view's policies too.

## Performance: policies are pushed into the scan

A policy is an ordinary filter for the optimizer. Comparisons against constants — including `current_user`, which is resolved when the statement is planned — land in the table scan next to your own filters, where they prune data just like a `WHERE` clause would. On an [inverted index](../sql/statements/create_index/inverted.md) the policy becomes a column filter of the index scan, so full-text search on a table with RLS stays an index search:

<SqlLogicTest id="security/row_level_security/example_006" />

## Leak protection

A filter written by a user must not be able to observe rows a policy hides — for example through a division by zero or a failing cast that only hidden rows would trigger. SereneDB keeps the policy and the table scan behind a barrier that the optimizer cannot move user expressions into. Only predicates that can neither raise an error nor return different results for the same input cross it: comparisons, `IS [NOT] NULL`, `IN` lists, `BETWEEN`, boolean logic, casts that cannot fail, and the full-text match operator `@@`. Everything else (casts that can fail, functions that can raise an error, volatile functions such as `random()`) runs on rows the policy already admitted:

<SqlLogicTest id="security/row_level_security/example_007" />

Statistics are hidden the same way: `stats()` above the barrier reports nothing about the hidden rows, and `pg_stats` hides tables whose policies apply to the current role.

Some channels remain, as in PostgreSQL: unique and foreign key violations reveal that a conflicting row exists, `EXPLAIN` estimates and plan shape depend on table-wide statistics, and full-text relevance scores (BM25) are computed over all rows of the index. Qualify cross-schema references inside policies (`schema.table`, `schema.function(...)`), because unqualified names are resolved through the table's schema first and then the session's `search_path`. A policy never uses a temporary table or macro: a policy expression that would resolve to one is refused.

## Catalogs

`pg_policy` holds one row per policy and the `pg_policies` view shows them with role names and the expressions; `pg_class.relrowsecurity` and `relforcerowsecurity` show the table and view flags; `row_security_active(relation)` tells whether policies apply to the current role; `duckdb_policies()` lists the policies of tables and views with their expressions:

<SqlLogicTest id="security/row_level_security/example_008" />

## Limitations

- `WITH CHECK` expressions (and `USING` expressions of `ALL`/`UPDATE` policies used as checks) cannot contain subqueries. Subqueries in `USING` work everywhere else, including the target row checks of `MERGE` and `ON CONFLICT DO UPDATE`.
- A new row that violates both a policy and a table constraint (a unique key, `NOT NULL` or `CHECK`) is reported as a constraint violation; PostgreSQL reports the policy.
- `COPY ... FROM` is refused when policies apply, as in PostgreSQL; use `INSERT`. `COPY ... TO` returns only visible rows.
- Row-level security applies to regular tables and views; it cannot be enabled on search tables, temporary tables and views, Iceberg tables or relations of attached databases.
- There is no `row_security` setting.
