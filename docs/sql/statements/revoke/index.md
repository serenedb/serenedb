---
title: REVOKE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `REVOKE` statement is the inverse of [`GRANT`](../grant/index.md): it removes privileges on an object from a role, or removes a role's membership in another role. The `GRANT OPTION FOR` / `ADMIN OPTION FOR` forms remove only the right to re-grant, keeping the underlying privilege or membership.

## Examples

Given a table with privileges granted and a role membership:

<SqlLogicTest id="sql/statements/revoke/index/example_001" />

Take one privilege away:

<SqlLogicTest id="sql/statements/revoke/index/example_002" />

<SqlLogicTest id="sql/statements/revoke/index/example_003" />

Remove only the grant option — the role keeps `SELECT` but can no longer pass it on:

<SqlLogicTest id="sql/statements/revoke/index/example_004" />

<SqlLogicTest id="sql/statements/revoke/index/example_005" />

Revoke role membership:

<SqlLogicTest id="sql/statements/revoke/index/example_006" />

<SqlLogicTest id="sql/statements/revoke/index/example_007" />

`ALL PRIVILEGES` clears everything the role holds on the object:

<SqlLogicTest id="sql/statements/revoke/index/example_008" />

## Notes

- Revoking requires the same authority as granting: object ownership (or `GRANT OPTION`) for privileges, `ADMIN OPTION` / `CREATEROLE` / superuser for membership.
- Revoking a privilege the role does not hold is not an error — the statement simply has no effect.

## See also

- [GRANT](../grant/index.md) — grant privileges or role membership
- [DROP ROLE](../drop_role/index.md) — remove roles entirely

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
