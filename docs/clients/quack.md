---
title: Quack API
sidebar_position: 15
split: headings
---

# Quack API

A SereneDB server can serve Quack, DuckDB's client/server protocol, over HTTP. Another SereneDB server or [`serened shell`](./serened-shell.md) then attaches it with `ATTACH 'quack:host:port'` and queries its tables like a local catalog, as the [Quack ATTACH page](../sql/statements/attach/quack.md) shows. Results travel in DuckDB's columnar format, and a query that reads only the attached server runs there as a whole.

## Enabling the API

Add an HTTP listener with `?api=quack`:

```bash
serened ./data --listen 'postgres://127.0.0.1:7890,http://127.0.0.1:9494?api=quack'
```

The protocol endpoint is `POST /quack`. `GET /quack` answers with a line of text that names the endpoint, so a quick check is:

```bash
curl -s http://127.0.0.1:9494/quack
```

```text
This is a SereneDB Quack endpoint. Use ATTACH 'quack:...' to connect here.
```

Port 9494 is the port a `quack:` address uses when it names none. Sessions work in the listener's database, which is `postgres` unless `db=` names another one, as in `?api=quack&db=shop`.

A client attaches over HTTPS unless the server address is `localhost`, `127.0.0.1` or `::1`. Serve the endpoint on an `https://` listener with a certificate the clients trust, or have them attach with `DISABLE_SSL true`.

## Authentication

A client presents one credential when it connects: the `TOKEN` of its `ATTACH`, or the token of a `quack` secret. The `Authorization` header of the HTTP requests is not used.

| Credential | Example | Statements run as |
|---|---|---|
| `<role>:<password>` | `alice:secret` | the role, after its [password](../security/client_authentication.md#passwords) is checked |
| `Basic <base64 of role:password>` | `Basic YWxpY2U6c2VjcmV0` | the role |
| a static token, with or without `Bearer ` | `s3cret`, `Bearer s3cret` | `postgres`, when the server runs with `--auth_bearer_token=s3cret` |
| `ApiKey <base64 of id:key>` | `ApiKey aWQ6a2V5` | `postgres`, when the server runs with `--auth_api_key=id:key` |

A role without a password, such as `postgres` on a fresh server, is accepted only from a loopback address, with an empty password: `postgres:`. Any other credential that does not check out fails the `ATTACH` with `authentication failed`.

The role's privileges apply to everything the client runs, and so do its settings, such as `statement_timeout`. A client without `SELECT` on a table gets `permission denied for table ...` like a `psql` session of the same role.

## Sessions

- Each client session that uses an attached database has a session of its own on the server, with its own transaction state, so concurrent client sessions do not interfere. Every `quack_query` call opens one more. A session ends with the client session, and `DETACH` ends all of them.
- A session runs one statement at a time. A new statement from the client cancels the one the session still runs.
- Cancelling a query on the client side stops it on the server. A SereneDB client also stops it when its `statement_timeout` runs out, and fails with `canceling statement due to statement timeout`.
- A session whose client stops sending requests and heartbeats is closed once the client's `HEARTBEAT_TIMEOUT` passes, 60 seconds by default.
- A server shutdown closes every session, and a statement still running fails on the client.

`quack_active_connections()` lists the server's sessions: the listener (`server_id`), the session (`connection_id`), its last statement (`query`), that statement's `state` (`idle`, `active`, `finished` or `cancelled`), the hash of the client's `CLIENT_ID` (`client_id_hash`), the rows its result cache holds (`cached_rows`) and when the statement started (`query_started_at`):

```sql
SELECT connection_id, state, query
FROM quack_active_connections()
ORDER BY query_started_at;
```

## Limits

- One request body holds at most 64 MiB.
- Only the tables and views users create are visible to a client: the server's `pg_catalog` and `information_schema` tables are not.
