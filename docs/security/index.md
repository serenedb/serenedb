---
title: Security
sidebar_position: 1
---

SereneDB manages access with the same model as PostgreSQL: **roles** own database objects and hold **privileges**, and every client connection is authenticated against a **host-based authentication** ruleset before it may act as a role.

There is no separate concept of a "user" or a "group" — a role can log in like a user, be granted to other roles like a group, or both. Roles are global to the server, not per-database.

The model in one picture:

1. [Client authentication](client_authentication.md) decides whether a connection may act as the role it claims to be — by password, or by an explicit trust rule.
2. [Roles](roles.md) are the identities: their attributes control what a role may do at the server level (log in, create databases, create other roles).
3. [Privileges](privileges.md) control what a role may do to individual objects (read a table, use a sequence, execute a function). The owner of an object holds every privilege on it and hands out the rest with [`GRANT`](../sql/statements/grant/index.md).
4. [Role membership](role_membership.md) groups roles: members automatically inherit what the group has been granted.

## The first connection

A freshly initialized server has exactly one role: the superuser `postgres`, with no password. It is always trusted on local connections, so on the machine that runs `serened` this works immediately:

```sh
psql -h 127.0.0.1 -U postgres
```

Remote connections are different: a role without a password can never log in over the network. To reach the server remotely, give the superuser a password first — either interactively over a local connection:

```sql
ALTER ROLE postgres PASSWORD 'a-strong-password';
```

or at first boot with the `POSTGRES_PASSWORD` environment variable, which seeds the password when the data directory is created — handy for containers:

```sh
POSTGRES_PASSWORD=a-strong-password serened /data --listen postgres://0.0.0.0:7890
```

From there, create the roles your application needs and grant them exactly the privileges they require — the following pages walk through each layer.

## See also

- [CREATE ROLE](../sql/statements/create_role/index.md) — create a role
- [GRANT](../sql/statements/grant/index.md) — grant privileges or membership
- [SET ROLE](../sql/statements/set_role/index.md) — act as another role
