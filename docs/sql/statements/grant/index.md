---
title: GRANT
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `GRANT` statement has two forms. The first grants privileges on an object (a table, sequence, function, database, schema, type, foreign server, or foreign-data wrapper) to a role. The second grants membership in a role to another role, so the member inherits the role's privileges.

## Examples

Given a table and some roles:

<SqlLogicTest id="sql/statements/grant/index/example_001" />

Allow a role to read the table:

<SqlLogicTest id="sql/statements/grant/index/example_002" />

Grant several privileges at once:

<SqlLogicTest id="sql/statements/grant/index/example_003" />

`ALL PRIVILEGES` grants every privilege applicable to the object:

<SqlLogicTest id="sql/statements/grant/index/example_004" />

A privilege can be restricted to specific columns:

<SqlLogicTest id="sql/statements/grant/index/example_005" />

With `WITH GRANT OPTION`, the grantee may pass the privilege on to others:

<SqlLogicTest id="sql/statements/grant/index/example_006" />

Inspect the result with the `has_*_privilege` functions:

<SqlLogicTest id="sql/statements/grant/index/example_007" />

<SqlLogicTest id="sql/statements/grant/index/example_008" />

The second form grants role membership — `doc_staff` now inherits everything granted to `doc_reader`:

<SqlLogicTest id="sql/statements/grant/index/example_009" />

<SqlLogicTest id="sql/statements/grant/index/example_010" />

`PUBLIC` is a pseudo-role meaning every role:

<SqlLogicTest id="sql/statements/grant/index/example_011" />

Only the unquoted keyword means every role: a quoted `"PUBLIC"` names a role called `PUBLIC`.

`WITH GRANT OPTION` can only be granted to roles: granting it to `PUBLIC`, with `GRANT` or `ALTER DEFAULT PRIVILEGES`, reports `grant options can only be granted to roles`.

In `GRANT`, `REVOKE` and `ALTER DEFAULT PRIVILEGES`, a grantee may be written `GROUP name`, which means the same as `name`.

Wherever a role is named, as a grantee, an owner or a member, after `GRANTED BY`, in the `IN ROLE`, `ROLE` and `ADMIN` lists of `CREATE ROLE`, and in `ALTER ROLE`, `ALTER GROUP`, `REASSIGN OWNED`, `DROP OWNED` and `ALTER DEFAULT PRIVILEGES FOR ROLE`, `CURRENT_USER` and `CURRENT_ROLE` stand for the role the session acts as and `SESSION_USER` for the role it logged in as (or set with `SET SESSION AUTHORIZATION`). Quoted, `"CURRENT_USER"` names a role called `CURRENT_USER`. None of the three keywords can name a new role or a role to drop: `CREATE ROLE current_user` reports `CURRENT_USER cannot be used as a role name here`, and `DROP ROLE current_user` reports `cannot use special role specifier in DROP ROLE`.

Granting to a role that does not exist is an error:

<SqlLogicTest id="sql/statements/grant/index/example_012" />

## Privileges by object type

| Object | Privileges |
|---|---|
| `TABLE` (default) | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER`, `MAINTAIN` |
| `SEQUENCE` | `USAGE`, `SELECT`, `UPDATE` |
| `FUNCTION` | `EXECUTE` |
| `DATABASE` | `CREATE`, `CONNECT`, `TEMPORARY` |
| `SCHEMA` | `CREATE`, `USAGE` |
| `TABLESPACE` | `CREATE` |
| `TYPE` | `USAGE` |
| `FOREIGN SERVER` | `USAGE` |
| `FOREIGN DATA WRAPPER` | `USAGE` |
| `PARAMETER` | `SET`, `ALTER SYSTEM` |

## Notes

- Granting a privilege requires owning the object or holding the privilege `WITH GRANT OPTION`; granting membership requires `ADMIN OPTION` on the role, or superuser. A role that holds some privilege on the object but not the grant options for all it grants gets the warning `no privileges were granted for "t"` or `not all privileges were granted for "t"` and grants only what it can; a role that holds nothing on it is refused. A table-level grant counts only the table's privileges; a column grant counts the table's and that column's.
- A role that is not the owner grants as the role holding the grant options: itself, or a role whose privileges it inherits, taking the first that holds every option the grant needs, or else the one holding the most. That role is recorded as the grantor, and each column of a column grant picks its own.
- The object's owner holds all privileges on it unless it revokes some from itself, and may always grant them; a granted role's privileges flow to its members automatically.
- A database, tablespace, foreign server, foreign-data wrapper or language is named without qualification: `GRANT CONNECT ON DATABASE other.db TO r` is a syntax error. A schema can be qualified with its database, and a nested schema is named by its path, as in `GRANT USAGE ON SCHEMA outer.inner TO r`.
- Several objects, grantees, granted roles and members can be listed, separated by commas: `GRANT SELECT ON t1, t2 TO r1, r2` grants on every pair, and if any part fails, such as a missing table, nothing is granted. `ALTER DEFAULT PRIVILEGES FOR ROLE` and `IN SCHEMA` take lists the same way.
- Unlike PostgreSQL, an unknown privilege keyword (e.g. `GRANT FLY ON t TO r`) is a syntax error rather than a semantic one.

## See also

- [REVOKE](../revoke/index.md) — take privileges or membership away
- [CREATE ROLE](../create_role/index.md) — create roles to grant to
- [SET ROLE](../set_role/index.md) — act as a role you are a member of

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
