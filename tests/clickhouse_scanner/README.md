# clickhouse_scanner extension tests

Driver for the vendored duckdb_clickhouse extension's own test suite at
[third_party/duckdb_clickhouse/test/](../../third_party/duckdb_clickhouse/test/).
Runs the `.test` files via DuckDB's `unittest` binary against the same serenedb
build that ships the statically-linked extension.

This is the DuckDB-level layer of the clickhouse_scanner test story. The
serened-level layer (pg-wire end-to-end coverage) is the `*_chscan.test_slow`
files under [tests/sqllogic/sdb/clickhouse/](../sqllogic/sdb/clickhouse/),
driven by the regular sqllogic runner (which has its own `launch_clickhouse()`).

## Prereqs

- A configured build dir (`build/`). The `unittest` binary is built by
  default; the driver will `ninja unittest` for you if needed.
- `docker` reachable from your user (`docker ps` works without sudo). The
  driver brings up a `clickhouse/clickhouse-server:24.8` container (matching
  the native-protocol revision the vendored clickhouse-cpp negotiates) on a
  free port via `docker-compose.yml`.

## Running

```bash
tests/clickhouse_scanner/run.sh                # docker clickhouse on a free port
tests/clickhouse_scanner/run.sh --use-existing # clickhouse already at $CHHOST:$CHPORT (CI)
```

## How this differs from the postgres_scanner driver

The structure mirrors [tests/postgres_scanner/](../postgres_scanner/) (same
`run.sh` + `docker-compose.yml` + `test-config.json`), with two ClickHouse-specific
points:

- **Env-var auth, not a mounted config.** The 24.8 image locks the `default`
  user to localhost, so serened's host-side connection (arriving from the
  docker gateway) would be rejected. `CLICKHOUSE_SKIP_USER_SETUP=1` re-opens it
  to all networks with no password -- the direct analogue of postgres's
  `POSTGRES_HOST_AUTH_METHOD=trust`. (Earlier this used a mounted
  `config/open-default-user.xml`; the env var removes that file.)

- **Pre-seeded read fixtures ([`fixtures/01_init.sql`](fixtures/01_init.sql)).**
  Unlike the postgres tests, which create their data in-test, the clickhouse
  read tests cover complex types (`Array`/`Map`/`UUID`/`Decimal`/`Nullable` in
  `chtest.types_all`) that the connector cannot yet INSERT, so that data is
  pre-seeded by the container init script. Now that the connector is read+write,
  simple-type data is created in-test (see the `write_*_chscan` files); only the
  complex-type read coverage still depends on the fixture.
