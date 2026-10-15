---
title: Quack
split: headings
---

SereneDB attaches another SereneDB server over the Quack protocol, or any other server that speaks it, such as DuckDB. The server needs an HTTP listener with `?api=quack`, as the [Quack API page](../../../clients/quack.md) describes. The same `ATTACH` works in [`serened shell`](../../../clients/serened-shell.md). For the general `ATTACH` and `DETACH` syntax, see [ATTACH AND DETACH](./index.md).

```sql
ATTACH 'quack:10.0.0.5:9494' AS remote (TOKEN 'alice:secret');

SELECT o.id, o.total
FROM remote.public.orders AS o
WHERE o.total > 100
ORDER BY o.id;
```

The attached database lists the server's schemas and the tables and views in them, so a table `orders` in the server's `public` schema is `remote.public.orders`. A name without a schema, such as `remote.orders`, means the server's default schema: `public` on a SereneDB server, `main` on DuckDB.

## Address

The path is `quack:host` or `quack:host:port`; an IPv6 host goes in brackets, as in `quack:[::1]:9494`. Without a port the address means port 9494.

The connection uses HTTPS unless the host is `localhost`, `127.0.0.1` or `::1`. `DISABLE_SSL true` connects over plain HTTP to any host, and `DISABLE_SSL false` uses HTTPS for a local one too.

## Options

| Option | Description | Default |
|---|---|---|
| `TOKEN` | The credential the server checks, as the [Quack API page](../../../clients/quack.md#authentication) lists them | the token of a matching `quack` secret |
| `SECRET` | The name of the `quack` secret that holds the token. Cannot be combined with `TOKEN`. | |
| `DISABLE_SSL` | Connect over plain HTTP | `true` for a local host, `false` otherwise |
| `CLIENT_ID` | An identifier of the client, empty or at least 4 characters | the `quack_default_client_id` setting |
| `HEARTBEAT_TIMEOUT` | Seconds the server keeps the session without a request from the client | the `quack_default_heartbeat_timeout` setting, 60 |

An `ATTACH` without a credential fails with `Could not find a Quack authentication token`.

## Secrets

A `quack` secret keeps the token out of the `ATTACH` statement. Its `SCOPE` is the address it applies to:

```sql
CREATE SECRET orders_server (
    TYPE quack,
    TOKEN 'alice:secret',
    SCOPE 'quack:10.0.0.5:9494'
);

ATTACH 'quack:10.0.0.5:9494' AS remote;
```

`EXTRA_HTTP_HEADERS` adds headers to every request the client sends, for example for a proxy in front of the server: `EXTRA_HTTP_HEADERS MAP {'X-Proxy-Token': 'abc'}`.

## Queries

- A query, or a subquery of a `UNION`, that reads only the attached database runs on the server as a whole, joins and aggregates included.
- A query that joins the attached tables with local ones reads the columns it needs from the server and does the rest locally, so it transfers every row of the remote tables it reads.
- `INSERT INTO remote.public.t SELECT ...` from local data sends the rows to the server.
- `CREATE TABLE`, `DROP` and `ALTER` on the attached database run on the server.
- `UPDATE` and `DELETE` run on the server when the whole statement reads only the attached database.
- `statement_timeout` covers the time a query waits for the server. A query that fails, is cancelled or runs out of time stops on the server too.

The attached database reads the server's table list at `ATTACH`. A table that another client creates on the server afterwards is visible once `quack_clear_cache()` reloads the lists of every attached quack database:

```sql
SELECT * FROM quack_clear_cache();
```

`quack_query` runs a SQL string on a server without attaching it:

```sql
SELECT *
FROM quack_query('quack:10.0.0.5:9494', 'SELECT count(*) FROM orders', token := 'alice:secret');
```

It takes the `token`, `disable_ssl`, `client_id` and `heartbeat_timeout` options of `ATTACH`.

## Limitations

- `UPDATE` and `DELETE` that read local tables fail with `PlanUpdate not implemented yet` and `PlanDelete not implemented yet`.
- On a server other than SereneDB, a statement that runs on the server as a whole, and `quack_query`, run there already when they are prepared, explained or described, to learn their result columns, and the first `EXECUTE` returns the rows of that run. So `PREPARE p AS DELETE FROM remote.main.t WHERE ...` deletes the rows at `PREPARE`. On a SereneDB server they run once per execution, as `quack_query` with several statements does on any server.
