---
title: Role Membership
sidebar_position: 3
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

It is convenient to group roles together to manage privileges in one place: grant a privilege to the group once, and every member has it. In SereneDB, as in PostgreSQL, a group is just a role — membership is granted and revoked with the same [`GRANT`](../sql/statements/grant/index.md) and [`REVOKE`](../sql/statements/revoke/index.md) statements used for privileges.

Set up a group role, give it a privilege, and add a member:

<SqlLogicTest id="security/role_membership/example_001" />

`pg_has_role` reports membership:

<SqlLogicTest id="security/role_membership/example_002" />

## Inheritance

Members with the default `INHERIT` attribute exercise the group's privileges automatically — `doc_sec_bob` can read the table because `doc_sec_analysts` can:

<SqlLogicTest id="security/role_membership/example_003" />

## SET ROLE

A session can also switch to a role explicitly with [`SET ROLE`](../sql/statements/set_role/index.md). Privilege checks then apply to that role instead of the one that authenticated; `session_user` keeps reporting the login identity while `current_user` reports the active one. `RESET ROLE` switches back:

<SqlLogicTest id="security/role_membership/example_004" />

A superuser can set any role; other roles can only set roles they are a member of.

## Grant options and delegation

Membership can be granted `WITH ADMIN OPTION`, which lets the member grant the same membership on to others — the mechanism for delegating group management without superuser:

<SqlLogicTest id="security/membership_options/example_001" />

The deputy, holding `ADMIN OPTION`, can now add members itself:

<SqlLogicTest id="security/membership_options/example_002" />

A member created `NOINHERIT` is still a member, but does not use the group's privileges automatically — it must `SET ROLE` to the group to exercise them:

<SqlLogicTest id="security/membership_options/example_003" />

## Transitive membership

Membership chains: a member of a member is a member. Adding `doc_mo_staff` to `doc_mo_deputy` (which is a member of `doc_mo_lead`) reaches `doc_mo_lead` through the chain:

<SqlLogicTest id="security/membership_options/example_004" />

Cycles are refused — a role cannot become a member of a role that is already (transitively) a member of it:

<SqlLogicTest id="security/membership_options/example_005" />

## Revoking membership

Revoking the membership takes the inherited privileges with it:

<SqlLogicTest id="security/role_membership/example_005" />

## Grantors

Each grant of a membership records the role that made it, shown in `pg_auth_members.grantor`. A superuser's grant is recorded as made by `postgres`, the bootstrap superuser; anyone else's as made by the role through which they hold `ADMIN OPTION` on the granted role. `GRANTED BY role` names the grantor instead: a role whose privileges the current role has and that holds `ADMIN OPTION` itself. Two grantors granting the same membership make two grants, and the member keeps the membership while either one remains.

`REVOKE` removes only the grant made by the revoking role, or by the role `GRANTED BY` names, and warns `role "..." has not been granted membership in role "..." by role "..."` when there is none. Revoking a grant that carries `ADMIN OPTION`, or `REVOKE ADMIN OPTION FOR` it, fails with `dependent privileges exist` while the member has granted the membership on and holds `ADMIN OPTION` through no other grant; with `CASCADE` those grants are revoked too. A role that made grants cannot be dropped while they remain; `DROP OWNED` revokes them.

A role with `CREATEROLE` that creates a role is granted the new role `WITH ADMIN OPTION`, recorded as granted by `postgres`. The [`createrole_self_grant`](../configuration/overview.md#security) setting, `set`, `inherit` or both, also grants it the new role with those options, recorded as granted by itself, so that it can `SET ROLE` to the role or use its privileges.

## Notes

- Granting membership requires `ADMIN OPTION` on the role, or superuser.
- Membership is not cyclic: a role cannot be granted to itself, directly or through a chain.
- Groups usually have no `LOGIN` — they exist to be granted, not to connect.
- A `NOINHERIT` member does not use the group's privileges implicitly; it must [`SET ROLE`](../sql/statements/set_role/index.md) to the group first, as in PostgreSQL.

## See also

- [GRANT](../sql/statements/grant/index.md) — the membership form of GRANT
- [SET ROLE](../sql/statements/set_role/index.md) — switch the active role
- [Privileges](privileges.md) — what the group grants actually confer
