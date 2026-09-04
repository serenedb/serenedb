---
title: SET ROLE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SET ROLE` statement switches the current role of the session. Privilege checks then apply to that role instead of the role you authenticated as. A superuser can set any role; other users can only set roles they are a member of. `RESET ROLE` (or `SET ROLE DEFAULT`) switches back.

## Examples

Given a role with access to a table:

<SqlLogicTest id="sql/statements/set_role/index/example_001" />

Switch to it:

<SqlLogicTest id="sql/statements/set_role/index/example_002" />

`current_user` and `current_role` now report the set role, while `session_user` remains the role that authenticated:

<SqlLogicTest id="sql/statements/set_role/index/example_003" />

Access checks are made against the current role:

<SqlLogicTest id="sql/statements/set_role/index/example_004" />

Switch back:

<SqlLogicTest id="sql/statements/set_role/index/example_005" />

<SqlLogicTest id="sql/statements/set_role/index/example_006" />

Setting a role that does not exist is an error:

<SqlLogicTest id="sql/statements/set_role/index/example_007" />

## Notes

- Setting a role you are not a member of fails with `permission denied to set role "..."` (superusers are exempt).
- The switch lasts for the session (until `RESET ROLE`, another `SET ROLE`, or disconnect).

## See also

- [GRANT](../grant/index.md) — grant role membership
- [CREATE ROLE](../create_role/index.md) — create roles

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
