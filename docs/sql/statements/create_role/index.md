---
title: CREATE ROLE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `CREATE ROLE` statement adds a new role. Roles are cluster-wide: one role exists across every database on the server. A role can own objects, hold privileges granted with [`GRANT`](../grant/index.md), contain other roles as members, and — if it has the `LOGIN` attribute — connect to the server as a database user.

`CREATE USER` is the same statement, except the new role gets `LOGIN` by default; `CREATE ROLE` defaults to `NOLOGIN`.

## Examples

Create a role. By default it cannot log in — a plain role is a bundle of privileges to grant to others:

<SqlLogicTest id="sql/statements/create_role/index/example_001" />

Create a role that can connect, authenticating with a password:

<SqlLogicTest id="sql/statements/create_role/index/example_002" />

`CREATE USER` implies `LOGIN`:

<SqlLogicTest id="sql/statements/create_role/index/example_003" />

Give a role the `CREATEDB` and `CREATEROLE` attributes, allowing it to create databases and to manage other roles (the optional `WITH` keyword is noise, as in PostgreSQL):

<SqlLogicTest id="sql/statements/create_role/index/example_004" />

Set an expiry on the password. After the timestamp passes, password authentication for the role fails:

<SqlLogicTest id="sql/statements/create_role/index/example_005" />

Make the new role an immediate member of an existing role, inheriting its privileges:

<SqlLogicTest id="sql/statements/create_role/index/example_006" />

Contradictory attributes are rejected:

<SqlLogicTest id="sql/statements/create_role/index/example_007" />

Role names are unique across the server:

<SqlLogicTest id="sql/statements/create_role/index/example_008" />

## Notes

- `SUPERUSER` roles bypass all permission checks. Only a superuser can create another superuser.
- `CONNECTION LIMIT` is accepted and stored (visible in `pg_roles.rolconnlimit`) but is not currently enforced at connect time.
- `REPLICATION`, `BYPASSRLS`, and `SYSID` are accepted for PostgreSQL compatibility and have no effect.

## See also

- [ALTER ROLE](../alter_role/index.md) — change a role's attributes or password
- [DROP ROLE](../drop_role/index.md) — remove roles
- [GRANT](../grant/index.md) — grant privileges or role membership
- [SET ROLE](../set_role/index.md) — switch the current role within a session

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
