---
title: DROP ROLE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `DROP ROLE` statement removes one or more roles. `DROP USER` is an alias. A role that still owns objects or holds privileges on them cannot be dropped — transfer ownership (`ALTER TABLE ... OWNER TO ...`) or revoke the grants first.

## Examples

Given a few roles:

<SqlLogicTest id="sql/statements/drop_role/index/example_001" />

Drop one:

<SqlLogicTest id="sql/statements/drop_role/index/example_002" />

Drop several at once:

<SqlLogicTest id="sql/statements/drop_role/index/example_003" />

`DROP USER` works on any role:

<SqlLogicTest id="sql/statements/drop_role/index/example_004" />

With `IF EXISTS`, dropping a missing role is not an error:

<SqlLogicTest id="sql/statements/drop_role/index/example_005" />

Without it, it is:

<SqlLogicTest id="sql/statements/drop_role/index/example_006" />

## Notes

- Dropping roles requires the `CREATEROLE` attribute (or superuser); dropping a superuser role requires superuser.
- If the role owns objects or is referenced by grants, `DROP ROLE` fails with `role "..." cannot be dropped because some objects depend on it`.

## See also

- [CREATE ROLE](../create_role/index.md) — create a role
- [ALTER ROLE](../alter_role/index.md) — change a role's attributes
- [REVOKE](../revoke/index.md) — remove privileges before dropping a role

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
