# postgres_scanner extension tests

Driver for the DuckDB postgres_scanner extension's upstream test suite,
vendored at [third_party/duckdb_postgres/test/](../../third_party/duckdb_postgres/test/).
Runs ~108 `.test` files via DuckDB's `unittest` binary against the same
serenedb build that ships the statically-linked extension.

This is the DuckDB-level layer of the postgres_scanner test story.
The serened-level layer (pg-wire end-to-end coverage) is the
`*_pgscan.test` files under
[tests/sqllogic/sdb/pg/duckdb_postgres/](../sqllogic/sdb/pg/duckdb_postgres/),
driven by the regular sqllogic runner.

## Prereqs

- A configured build dir (`build/`). The DuckDB `unittest` binary is
  built by default; the driver will `ninja unittest` for you if needed.
  Pass `-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` at configure time to skip
  it (the driver will then refuse to run).
- `docker` reachable from your user (i.e. `docker ps` works without
  sudo). The driver brings up a postgres:18.3 container (matching the
  REL_18_3 sources we statically link) on a free port via
  `docker-compose.yml`.

## Running

```bash
# default: postgres comes up in docker
tests/postgres_scanner/run.sh

# CI / advanced: postgres is already up at $PGHOST:$PGPORT
tests/postgres_scanner/run.sh --use-existing
```

The driver:

1. Brings up postgres on a free port (docker compose or in-process).
2. Exports `POSTGRES_TEST_DATABASE_AVAILABLE=1` plus `PGHOST/PGPORT/...`
   so upstream tests' `require-env POSTGRES_TEST_DATABASE_AVAILABLE`
   gate flips and their per-test connections find the fixture.
3. Invokes `unittest --test-config third_party/duckdb_postgres/test/configs/attach_postgres.json`,
   which runs the upstream JSON's `on_init` / `on_cleanup` hooks to
   create and drop a fresh postgres database per test.
4. Writes JUnit XML to `tests/sqllogic/postgres_scanner.xml` so it
   joins the standard test-artifact bundle.

## Skips

The upstream config ships its own `skip_tests` list (60+ entries,
each with a reason) -- we don't maintain our own. If we hit a
serenedb-specific failure that upstream doesn't already skip, we'll
layer a serenedb-side `test-config.json` then.
