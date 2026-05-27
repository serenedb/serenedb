# postgres_scanner extension tests

Driver for the DuckDB postgres_scanner extension's upstream test suite,
vendored at [third_party/duckdb_postgres/test/](../../third_party/duckdb_postgres/test/).
Runs ~126 `.test` files via DuckDB's `unittest` binary against the same
serenedb build that ships the statically-linked extension.

This is the DuckDB-level layer of the postgres_scanner test story. The
serened-level layer (pg-wire end-to-end coverage) is the `*_pgscan.test`
files under [tests/sqllogic/sdb/pg/duckdb_postgres/](../sqllogic/sdb/pg/duckdb_postgres/),
driven by the regular sqllogic runner.

## Prereqs

- A configured build dir (`build/`). The `unittest` binary is built by
  default; the driver will `ninja unittest` for you if needed. Configure
  with `-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` to skip it (the driver then
  refuses to run).
- `docker` reachable from your user (`docker ps` works without sudo).
  The driver brings up a `postgres:18.3` container (matching the REL_18
  sources we statically link) on a free port via `docker-compose.yml`.

## Running

```bash
tests/postgres_scanner/run.sh                # docker postgres on a free port
tests/postgres_scanner/run.sh --use-existing # postgres already at $PGHOST:$PGPORT (CI)
```

The driver brings postgres up, provisions the `postgresscanner` database,
runs the suite, and writes JUnit XML to `tests/sqllogic/postgres_scanner.xml`.
In CI it's invoked by the `test-functional` job via
`048-ci-in-docker-run-extension-tests.bash` ->
[docker-compose.extensions.yml](../sqllogic/docker-compose.extensions.yml)
(`run.sh --use-existing`).

## Why this isn't just `unittest --test-dir`

Three things differ from a naive run, each non-obvious:

- **Own [`test-config.json`](test-config.json), not upstream's
  `attach_postgres.json`.** That config is for the inverse scenario
  (running DuckDB *core's* suite against postgres-as-storage); its
  `on_new_connection: USE pgdb;` breaks duckdb_postgres' own tests.
  Ours just sets `search_path = main` and skips two tests:
  - `postgres_binary` -- server-side `COPY FROM <host-path>` needs the
    postgres backend uid to read a runner-owned file; fails across the
    docker bind-mount.
  - `postgres_execute_transaction` -- asserts an exact log-row count
    that's only deterministic with upstream's per-test `pgdb` isolation.

- **Locale pinned to `C.UTF-8`** (in both compose files). DuckDB rewrites
  `col LIKE 'foo9%'` -> `col >= 'foo9' AND col < 'foo:'`; under `en_US.utf8`
  (the postgres image default) `:` sorts before `9`, so the range excludes
  matching rows and `attach_like.test` fails. Upstream's CI runs a
  C-locale postgres and never hits this.

- **Synthetic `tpch.lineitem` (10k rows).** Upstream seeds tpch via
  DuckDB's dbgen; we don't ship the CLI, so `run.sh` fakes the one table
  `attach_timeout_error.test` needs to trip its 1s statement_timeout.
