---
title: Privileges
sidebar_position: 4
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Every database object has an owner and an access-control list. The owner — normally the role that created the object — holds every privilege on it implicitly; everyone else has only what has been granted to them (directly, through [role membership](role_membership.md), or through `PUBLIC`).

The owner holds all privileges from the start:

<SqlLogicTest id="security/privileges/example_001" />

## Granting privileges

[`GRANT`](../sql/statements/grant/index.md) gives a privilege on an object to a role, and the `has_*_privilege` functions inspect the result:

<SqlLogicTest id="security/privileges/example_002" />

Privileges are enforced on every query. Acting as the role, reading works and writing is refused:

<SqlLogicTest id="security/privileges/example_003" />

## Available privileges

| Object | Privileges |
|---|---|
| `TABLE` (default) | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`, `TRIGGER`, `MAINTAIN` |
| `SEQUENCE` | `USAGE`, `SELECT`, `UPDATE` |
| `FUNCTION` | `EXECUTE` |
| `DATABASE` | `CREATE`, `CONNECT`, `TEMPORARY` |
| `SCHEMA` | `CREATE`, `USAGE` |
| `TYPE` | `USAGE` |
| `FOREIGN SERVER` | `USAGE` |

`ALL PRIVILEGES` grants everything applicable to the object type.

`USAGE` on a [foreign server](../sql/statements/create_server/index.md#privileges) is checked whenever a query reads through it, in any database. Creating a server is gated on `CREATE` on the **database**, not on a schema.

## Column privileges

`SELECT`, `INSERT` and `UPDATE` can be restricted to specific columns. A column grant does not confer the table-level privilege:

<SqlLogicTest id="security/privileges/example_004" />

## Grant options

With `WITH GRANT OPTION`, the grantee may pass the privilege on to others:

<SqlLogicTest id="security/privileges/example_005" />

### Reading the ACL

An object's access-control list is stored in its catalog `*acl` column (`relacl` for tables) as an array of `aclitem`s in PostgreSQL's format — `grantee=privileges/grantor`, where an empty grantee means `PUBLIC` and a trailing `*` marks a privilege held **with grant option**:

<SqlLogicTest id="security/privileges_advanced/example_001" />

The grantee can then pass the privilege on:

<SqlLogicTest id="security/privileges_advanced/example_002" />

## PUBLIC

`PUBLIC` is a pseudo-role meaning every role, present and future:

<SqlLogicTest id="security/privileges/example_006" />

## Revoking

[`REVOKE`](../sql/statements/revoke/index.md) removes grants — but only the grants the revoker (or a role it can act for) made. Revoking a direct grant does not remove access that still flows from `PUBLIC`, and a grant made by another grantor survives until that grantor revokes it:

<SqlLogicTest id="security/privileges/example_007" />

<DocCallout type="tip">

If a role still has access after a `REVOKE`, check where it flows from: a `PUBLIC` grant, a group membership, or a grant made by a different grantor. `has_table_privilege` reflects the combined result of all of them.

</DocCallout>

### Dependent grants: RESTRICT and CASCADE

When a privilege was passed on via `WITH GRANT OPTION`, those onward grants depend on it. By default (`RESTRICT`) `REVOKE` refuses while dependents exist:

<SqlLogicTest id="security/privileges_advanced/example_003" />

`CASCADE` revokes the privilege and every grant that depended on it, in one step:

<SqlLogicTest id="security/privileges_advanced/example_004" />

## Ownership

Ownership can be transferred with `ALTER ... OWNER TO`. The new owner immediately holds every privilege on the object, including the right to drop it:

<SqlLogicTest id="security/privileges/example_008" />

## Notes

- Granting a privilege requires owning the object or holding it `WITH GRANT OPTION`.
- Superusers bypass all privilege checks.
- `ALTER DEFAULT PRIVILEGES` is accepted and stored for PostgreSQL compatibility but is not yet applied to newly created objects.

## See also

- [GRANT](../sql/statements/grant/index.md) — full syntax, including the membership form
- [REVOKE](../sql/statements/revoke/index.md) — removing privileges
- [Database roles](roles.md) — the identities privileges are granted to
