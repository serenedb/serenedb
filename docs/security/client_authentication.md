---
title: Client Authentication
sidebar_position: 5
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Before a connection may act as a role, it must prove it is entitled to. SereneDB uses PostgreSQL's model: a **host-based authentication (HBA) ruleset** decides, per connection, *how* the client must authenticate — by password, by an explicit trust rule, or not at all (rejected). The role's stored password is the material for that check, not the check itself.

## The default posture

Out of the box, without any configuration:

- The bootstrap superuser `postgres` is **trusted on local connections** (unix socket and loopback TCP) — no password asked. This is the anti-lockout guarantee: whoever administers the machine can always reach the server and can never be locked out by a bad configuration.
- **Everyone else authenticates with a password** (SCRAM-SHA-256), locally and remotely alike.
- A role with **no password cannot log in at all** — a password rule never silently degrades to trust. This also means an unknown role and a wrong password produce the same generic error, so role names cannot be probed from the network.

The default is equivalent to this ruleset:

```text
local all postgres           trust
host  all postgres 127.0.0.1/32 trust
host  all postgres ::1/128      trust
local all all                scram-sha-256
host  all all 0.0.0.0/0      scram-sha-256
host  all all ::0/0          scram-sha-256
```

Note the last lines: unlike PostgreSQL's packaged default, password authentication is already enabled for every address. Exposing the server (`--listen postgres://0.0.0.0:7890`) is therefore the only step between you and working remote connections — no ruleset editing required, but every remote client must present a valid password.

## Passwords

Passwords are stored as SCRAM-SHA-256 verifiers, never in cleartext. Set one at creation or rotate it later:

<SqlLogicTest id="security/client_authentication/example_001" />

`PASSWORD NULL` clears the password — after which the role cannot log in again until it gets a new one (or a trust rule covers it):

<SqlLogicTest id="security/client_authentication/example_002" />

`VALID UNTIL` puts an expiry on the password; an expired password is refused at login:

<SqlLogicTest id="security/client_authentication/example_003" />

For containers and provisioning, the `POSTGRES_PASSWORD` environment variable seeds the superuser's password when the data directory is first created:

```sh
POSTGRES_PASSWORD=a-strong-password serened /data --listen postgres://0.0.0.0:7890
```

<DocCallout type="tip" title="Forgot the superuser password?">

Connect over loopback — `psql -h 127.0.0.1 -U postgres` is always trusted — and run `ALTER ROLE postgres PASSWORD '...'`. No file editing or restart needed.

</DocCallout>

## The HBA ruleset

The ruleset is a server setting in PostgreSQL's `pg_hba.conf` grammar, managed with SQL. Setting it requires superuser; it is persisted to `<datadir>/pg_hba.conf` and takes effect immediately:

```sql
SET hba = '
host all all 10.0.0.0/8   trust
host all all 0.0.0.0/0    scram-sha-256';
```

Rules are matched first-to-last against the connection's type (`local` or `host`), database, role, and client address; the first match decides the method. For example, the ruleset above trusts every client from the private `10.0.0.0/8` network and requires a password from everywhere else.

| Method | Effect |
|---|---|
| `trust` | Connection is accepted with no credential check. |
| `reject` | Connection is refused outright. |
| `scram-sha-256` | Password authentication via SCRAM (the default and recommended method). |
| `md5` | Legacy MD5 password exchange, for old clients. |
| `password` | Cleartext password exchange (use only over TLS). |

The remaining PostgreSQL methods — `peer`, `ident`, `cert`, `ldap`, `gss` — are accepted by the parser for compatibility but refuse the connection if matched.

<DocCallout type="attention" title="The superuser cannot be locked out">

Whatever ruleset you install, the bootstrap superuser keeps its trusted local access — an un-droppable safety rule is checked first. A configuration mistake can never cut off local administrative access; to restrict the superuser, restrict where the server listens instead.

</DocCallout>

## Notes

- A misconfigured ruleset is rejected as a whole (`SET hba` fails and the previous ruleset stays active) — the server never ends up between two configurations.
- IPv4-mapped IPv6 rules (`::ffff:a.b.c.d/128`) match the corresponding IPv4 clients, as in PostgreSQL.
- A connection that matches no rule is refused with the PostgreSQL-compatible `no pg_hba.conf entry` error.

## See also

- [Database roles](roles.md) — the identities being authenticated
- [ALTER ROLE](../sql/statements/alter_role/index.md) — passwords and expiry
- [Security overview](index.md) — the model end to end
