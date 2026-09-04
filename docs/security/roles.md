---
title: Database Roles
sidebar_position: 2
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
| `CONNECTION LIMIT <n>` | Accepted and stored for PostgreSQL compatibility; not currently enforced. |

A role that grants management powers with `CREATEROLE` gets them, but bounded like PostgreSQL: a `CREATEROLE` role cannot hand out an attribute it does not itself hold (so it cannot create a `SUPERUSER`), and it can only alter or drop roles it administers — i.e. roles it created, on which it automatically receives `ADMIN OPTION`:

<SqlLogicTest id="security/managing_roles/example_002" />

Change attributes, rotate a password, or set an expiry at any time:

<SqlLogicTest id="security/roles/example_004" />

A role can also be renamed:

<SqlLogicTest id="security/roles/example_005" />

Creating a role whose name is already taken is an error:

<SqlLogicTest id="security/managing_roles/example_003" />

## Dropping roles

A role cannot be dropped while it still owns objects — the dependency is reported and the drop is refused. Reassign or drop the objects first:

<SqlLogicTest id="security/roles/example_006" />

`DROP ROLE` accepts a comma-separated list, and `IF EXISTS` makes a missing role a no-op instead of an error:

<SqlLogicTest id="security/roles/example_007" />

<DocCallout type="attention">

Privileges the role holds on *other* objects are revoked automatically when it is dropped — only ownership blocks the drop. PostgreSQL's bulk helpers `DROP OWNED BY` and `REASSIGN OWNED BY` are not supported yet, so transfer ownership with `ALTER ... OWNER TO` per object.

</DocCallout>

## See also

- [CREATE ROLE](../sql/statements/create_role/index.md) — full syntax and options
- [ALTER ROLE](../sql/statements/alter_role/index.md) — change attributes and passwords
- [DROP ROLE](../sql/statements/drop_role/index.md) — remove roles
- [Role membership](role_membership.md) — use roles as groups
