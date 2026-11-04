---
title: Database Roles
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Roles are the identities of the access-control system. A role can own database objects, hold privileges on other roles' objects, and — if it has the `LOGIN` attribute — start client connections. Roles are global across the server: the same role exists in every database.

The concept of a role subsumes the classic notions of "user" and "group": a role with `LOGIN` behaves like a user, a role that is granted to other roles behaves like a group, and nothing stops one role from being both.

Every server starts with one predefined role: the superuser `postgres`. It bypasses all permission checks and is always able to connect locally, so it cannot be locked out — use it to create the rest.

## Viewing roles

The existing roles are listed in the `pg_roles` catalog (and, for password state, the superuser-only `pg_authid`):

<SqlLogicTest id="security/managing_roles/example_001" />

## Creating roles

Create a role that can connect, with a password to authenticate:

<SqlLogicTest id="security/roles/example_001" />

`CREATE ROLE` and `CREATE USER` differ only in their default: `CREATE ROLE` creates a role **without** `LOGIN` (a group, or a pure owner of objects), while `CREATE USER` includes it:

<SqlLogicTest id="security/roles/example_002" />

## Role attributes

Attributes control a role's server-level abilities. They are set at creation or changed later with [`ALTER ROLE`](../sql/statements/alter_role/index.md):

<SqlLogicTest id="security/roles/example_003" />

| Attribute | Meaning |
|---|---|
| `LOGIN` / `NOLOGIN` | May the role start client connections? Roles without it can still be granted to others or own objects. |
| `SUPERUSER` / `NOSUPERUSER` | Bypasses every permission check. Only another superuser can create one. |
| `CREATEDB` / `NOCREATEDB` | May the role create databases? |
| `CREATEROLE` / `NOCREATEROLE` | May the role create, alter and drop other roles? It cannot hand out attributes it does not hold itself. |
| `INHERIT` / `NOINHERIT` | Does the role automatically use privileges of roles it is a member of? Default `INHERIT`. |
| `PASSWORD '...'` | Stores a SCRAM-SHA-256 verifier for password authentication. `PASSWORD NULL` clears it. |
| `VALID UNTIL '<timestamp>'` | The password stops working after this time — enforced at login. |
| `CONNECTION LIMIT <n>` | How many sessions the role may have open at once, over the PostgreSQL protocol and the HTTP APIs together; `-1` (the default) means no limit. A login beyond it fails with `too many connections for role`. Superusers are not limited. |

A role that grants management powers with `CREATEROLE` gets them, but bounded like PostgreSQL: a `CREATEROLE` role cannot hand out an attribute it does not itself hold (so it cannot create a `SUPERUSER`), and it can only alter or drop roles it administers — i.e. roles it created, on which it automatically receives `ADMIN OPTION`:

<SqlLogicTest id="security/managing_roles/example_002" />

Change attributes, rotate a password, or set an expiry at any time:

<SqlLogicTest id="security/roles/example_004" />

A role can also be renamed:

<SqlLogicTest id="security/roles/example_005" />

Creating a role whose name is already taken is an error:

<SqlLogicTest id="security/managing_roles/example_003" />

## Dropping roles

A role cannot be dropped while it still owns objects, or while a grant on them names it, as grantee or as grantor — the dependency is reported and the drop is refused. Reassign or drop the objects and revoke the grants first:

<SqlLogicTest id="security/roles/example_006" />

<SqlLogicTest id="security/roles/example_009" />

`DROP ROLE` accepts a comma-separated list, and `IF EXISTS` makes a missing role a no-op instead of an error:

<SqlLogicTest id="security/roles/example_007" />

Default privileges that name the role block the drop too. Two statements clear everything that holds a role back, in the database the session is connected to:

- `REASSIGN OWNED BY role [, ...] TO new_role` makes `new_role` the owner of every object the roles own in the current database, and of every database and subscription they own. Grants they hold and their default privileges stay.
- `DROP OWNED BY role [, ...] [CASCADE | RESTRICT]` drops every object the roles own in the current database and revokes their privileges there and on databases, together with the grants they passed on, their default privileges and their foreign-data wrapper grants. Privileges are revoked as a `REVOKE` run by the current role would: a role dropping its own objects keeps the grants others made to it, and a superuser removes those the owners made. Databases and subscriptions they own are kept. Without `CASCADE`, it fails when objects of other roles depend on the objects it would drop.

Both need the privileges of every role they name, and `REASSIGN OWNED` also those of the new owner. Neither accepts the `postgres` role. Run them in each database where the role owns objects or holds privileges, then drop it:

<SqlLogicTest id="security/roles/example_010" />

## See also

- [CREATE ROLE](../sql/statements/create_role/index.md) — full syntax and options
- [ALTER ROLE](../sql/statements/alter_role/index.md) — change attributes and passwords
- [DROP ROLE](../sql/statements/drop_role/index.md) — remove roles
- [Role membership](role_membership.md) — use roles as groups
