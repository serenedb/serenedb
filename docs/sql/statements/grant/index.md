---
title: GRANT
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `GRANT` statement has two forms. The first grants privileges on an object (a table, sequence, function, database, schema, or type) to a role. The second grants membership in a role to another role, so the member inherits the role's privileges.

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
| `TYPE` | `USAGE` |

## Notes

- Granting a privilege requires owning the object or holding the privilege `WITH GRANT OPTION`; granting membership requires `ADMIN OPTION` on the role, the `CREATEROLE` attribute, or superuser.
- The object's owner always holds all privileges on it, and the owner of a granted role's privileges flow to members automatically.
- Unlike PostgreSQL, an unknown privilege keyword (e.g. `GRANT FLY ON t TO r`) is a syntax error rather than a semantic one.

## See also

- [REVOKE](../revoke/index.md) — take privileges or membership away
- [CREATE ROLE](../create_role/index.md) — create roles to grant to
- [SET ROLE](../set_role/index.md) — act as a role you are a member of

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
