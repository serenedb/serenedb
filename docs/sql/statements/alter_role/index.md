---
title: ALTER ROLE
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `ALTER ROLE` statement changes an existing role: its attributes (`LOGIN`, `CREATEDB`, …), its password, or its name. `ALTER USER` is an alias.

## Examples

Start with a login role:

<SqlLogicTest id="sql/statements/alter_role/index/example_001" />

Add an attribute:

<SqlLogicTest id="sql/statements/alter_role/index/example_002" />

Several attributes can be changed in one statement; the `NO`-prefixed form removes an attribute:

<SqlLogicTest id="sql/statements/alter_role/index/example_003" />

Rotate the password:

<SqlLogicTest id="sql/statements/alter_role/index/example_004" />

Set a password expiry — after this timestamp, password authentication fails:

<SqlLogicTest id="sql/statements/alter_role/index/example_005" />

Rename the role:

<SqlLogicTest id="sql/statements/alter_role/index/example_006" />

Contradictory attributes are rejected:

<SqlLogicTest id="sql/statements/alter_role/index/example_007" />

Altering a role that does not exist is an error:

<SqlLogicTest id="sql/statements/alter_role/index/example_008" />

## Notes

- Changing role attributes requires the `CREATEROLE` attribute (or superuser); only a superuser can grant or remove `SUPERUSER`.
- Granting or removing `SUPERUSER` reconnects the [foreign servers](../create_server/index.md#servers-owned-by-roles-without-superuser) the role owns under the rules for their new owner, and fails if one of them breaks those rules or cannot connect under them.
- `PASSWORD NULL` removes the stored password, disabling password authentication for the role.
- A new `CONNECTION LIMIT` applies to the logins that follow; sessions already open stay open.
- `SET name = value` stores a parameter default that every new session of the role starts with, before the parameters its client sends; `SET name FROM CURRENT` stores the value the parameter has in the current session; `RESET name`, `SET name TO DEFAULT` and `RESET ALL` remove stored defaults. A role may store defaults for itself and a role with `CREATEROLE` and the `ADMIN` option for the roles it administers, but only a superuser may store or `RESET` a parameter that only a superuser may change, and `RESET ALL` by any other role removes only the defaults that role may change. A session applies each default as if the role ran `SET` itself: a default the role may not set, or whose value is not valid, is skipped with a warning.
- `ALTER ROLE ALL SET name = value` and `ALTER ROLE ALL RESET name` store and remove defaults for every role in every database; only a superuser may change them, and anyone else gets `permission denied to alter setting`. `ALTER ROLE name IN DATABASE db SET ...` and `ALTER ROLE ALL IN DATABASE db SET ...` store defaults for one database. At login the defaults apply from the widest to the narrowest, each overriding the one before: every role, the database, the role, the role in the database. `pg_db_role_setting` lists them, with `0` for every database or every role.

## See also

- [CREATE ROLE](../create_role/index.md) — create a role
- [DROP ROLE](../drop_role/index.md) — remove roles
- [GRANT](../grant/index.md) — grant privileges or role membership

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
