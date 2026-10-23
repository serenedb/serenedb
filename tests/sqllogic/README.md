# sqllogic tests

The files run through our fork of [sqllogictest-rs](../../third_party/sqllogictest-rs) against serened, over both PostgreSQL wire protocols.

## Running

`scripts/ci/test.py` runs them the way CI does: every file once per wire protocol, on a pool of serened servers it starts from the build, with the services a file needs (MinIO, the Iceberg REST fixture, PostgreSQL, ...) in Docker:

```bash
scripts/ci/test.py --build-dir build_clangd --suite sqllogic --test 'sqllogic:*:sdb/pg/simple/*'
scripts/ci/test.py --build-dir build_clangd --suite sqllogic --list
```

The summary lands in `out/test/summary.txt`, every file's result in `out/test/results.jsonl`, and a failed file's runner log under `out/test/units/`.

`run.sh` runs files against a server you started, for an edit-run loop and for `--override`:

```bash
./tests/sqllogic/run.sh --single-port 7777 --test 'sdb/pg/simple_any/setop.test' --debug true
```

Common options:
- `--single-port 7777`: Test against SereneDB on this port
- `--test 'path/to/*.test'`: Run specific test files
- `--debug`: Enable debug mode (disables timeout)
- `--jobs N`: Set parallelism level (default: CPU cores)
- `--override`: Rewrite test file (Note: doesn't create empty database)
- `--runner <dir>`: the runner's source (default `third_party/sqllogictest-rs`), built on first use

## The sqlite corpus

`any/pg/sqlite` is a submodule of [serenedb/sqllogictest](https://github.com/serenedb/sqllogictest): DuckDB's copy of SQLite's sqllogictest corpus as upstream ships it, with our changes on top as commits (the tests SereneDB doesn't run removed, the skips for PostgreSQL and SereneDB, PostgreSQL's results where they differ from SQLite's). Only CI's test jobs check it out:

```bash
git submodule update --init --checkout tests/sqllogic/any/pg/sqlite
```

Its files are in sqllogictest's original format (no column names, one value per line, results over 8 values hashed), which `test.py` and `run.sh` tell the runner. A big file runs as several units of the runner's `--part`, sized from the timing store; `--fast` skips the corpus.

## Generated Tests

A test of thousands of similar cases isn't committed: `run.sh` writes it before
every run. Each `generators/<path>.py` prints the test that `run.sh` saves as
`<path>.gen.test`, so `generators/sdb/pg/index/phrase_deferred_matrix.py`
produces `sdb/pg/index/phrase_deferred_matrix.gen.test`. `.gitignore` ignores
every `*.gen.test`; edit the generator, not its output.

A generator builds its test with `sqllogic_gen.Test`, which writes the header,
the setup statements and the checks:

```python
from sqllogic_gen import Test, lit

t = Test("Deferred wildcard checks agree with the table itself.", seed=11)
t.dictionary("wdm_kw3", "generate_wildcard_ngrams(keyword(), 3)")
t.table("wdm", "id INTEGER PRIMARY KEY, msg VARCHAR")
t.insert("wdm", (f"({i}, '{lit(word)}')" for i, word in enumerate(words)))
t.index("wdm_kw3_idx", "wdm", "id, msg wdm_kw3")
t.refresh("wdm")
t.section("Whole values.")
for pred, truth in predicates:
    t.same_rows(f"SELECT id FROM wdm_kw3_idx WHERE {pred}",
                f"SELECT id FROM wdm WHERE {truth}")
t.write()
```

`same_rows(actual, expected)` checks that two queries return the same rows and
records how many they return; `same_scores(actual, expected)` checks that two
queries give each `id` the same `s`. `t.rng` is seeded with `seed`, so a
generator writes the same cases on every run.

A generator whose test would run longer than about 20 s splits it with
`t.part()`: everything before the first `t.part()` is shared, and each part
becomes `<path>.<k>.gen.test` with that setup and what follows its
`t.part()`, so a part builds only the tables and indexes it checks.

Where only the server knows the answer, such as a row count, a generator prints
`<recorded>`. `run.sh` fills those cells from `generators/<path>.recorded`, and
`run.sh --override` records them there, as values or, for a query that fails,
as its exact error, which the test then expects in the block form. A value is
keyed by a hash of its query's SQL, with `.2`, `.3` for repeats of the same SQL,
and shows the start of that SQL, so a review sees which query changed. Adding,
removing or reordering queries adds or removes only their lines; a query with no
recorded value keeps `<recorded>` and fails until `run.sh --override` records
it. A query that fails where the `.recorded` file has values or another error is
recorded only with `SDB_RECORD_NEW_ERRORS=1` set; without it `--override` lists
those queries and leaves the file unchanged. Query plans go into a committed
test next to the generated one, like `sdb/pg/index/phrase_deferred_plans.test`.

## Recovery Tests

Recovery tests intentionally crash serened (via `SET sdb_faults = '...'`) and
verify the server recovers correctly on restart. **Each test runs against its
own fresh serened + fresh datadir**, restarted after every crash, so a broken
test cannot cascade-fail the rest. A planned crash exits with 0 or 75; any other
exit fails the test.

```bash
scripts/ci/test.py --build-dir build_clangd --suite recovery
scripts/ci/test.py --build-dir build_clangd --suite recovery --test 'recovery:recovery/alter_rename.test'
```

A failed test keeps its serened logs, one per restart, under `out/test/units/<test>/server/`.

### Run a single recovery test with serened's output live

Start serened in one terminal:

```bash
PORT=7777 ./tests/sqllogic/run_serened_loop.sh /tmp/recdata
```

Run one test in another:

```bash
cd tests/sqllogic
./run.sh --host localhost --single-port 7777 \
         --test recovery/alter_rename.test \
         --engines pg-wire-simple \
         --runner ../../third_party/sqllogictest-rs
```

Terminal 1 shows serened's output live -- easier than tailing a log file.

## Tests that need a server of their own

A test that leaves server-wide state other tests can observe, or that breaks on other tests' server-wide activity, goes under `recovery/`, where every file gets a server of its own. `recovery/detach_held_drop_database_pgscan.test_slow` is one: it drops a remote PostgreSQL database that only its own session may hold, while a listing in any other test that is not scoped to one database opens a session on every attached PostgreSQL catalog.

## Iceberg tests: fixture or Google BigLake

`*_iceberg.test_slow` files do not spell out the catalog. They begin with

```
statement ok retry 20 backoff 250ms
${ICEBERG_BOOTSTRAP}

statement ok
CREATE SERVER <name> FOREIGN DATA WRAPPER iceberg_fdw OPTIONS (${ICEBERG_SERVER_OPTIONS});
```

and `run.sh` fills both variables from `ICEBERG_BACKEND`:

| `ICEBERG_BACKEND` | bootstrap | server options |
|---|---|---|
| `local` (default) | S3 secret `iceberg_ci_storage` for the MinIO warehouse | the iceberg-rest fixture, `authorization_type 'none'` |
| `biglake` | ICEBERG secret `iceberg_ci_catalog`: `PROVIDER google` from `BIGLAKE_CLIENT_EMAIL` / `BIGLAKE_PRIVATE_KEY` / `BIGLAKE_PRIVATE_KEY_ID`, or your gcloud application-default credentials when those are unset | `bl://projects/$BIGLAKE_PROJECT/catalogs/$BIGLAKE_CATALOG` on `biglake.googleapis.com` |

The bootstrap is shared by every test on one server, so it retries the catalog conflict two
parallel files can hit creating it. Schemas are named after `${__DATABASE__}`, which carries a
per-run suffix, so concurrent runs share one BigLake catalog without colliding. Tests that only
make sense on the fixture (explicit `sigv4`, literal `iceberg_scan('s3://...')` paths) are named
`*_fixture_iceberg.test_slow` and are skipped in `biglake` mode. BigLake rate-limits
`LoadTable`, so keep `--jobs` at 2 there:

```bash
ICEBERG_BACKEND=biglake BIGLAKE_PROJECT=<project> BIGLAKE_CATALOG=<catalog> \
  ./run.sh --single-port 7777 --jobs 2 --test 'sdb/**/*_iceberg.test_slow'
BIGLAKE_PROJECT=<project> BIGLAKE_CATALOG=<catalog> scripts/ci/test.py --build-dir build_clangd \
  --biglake --suite recovery --jobs 2 --test 'recovery:recovery/view_index_*_iceberg.test_slow'
```

CI runs the fixture pass always and adds the BigLake pass (`test.py --biglake` over sqllogic,
recovery and the stress profile) only when the workflow is dispatched with the `BIGLAKE`
checkbox on and the `BIGLAKE_*` repository secrets are present.

## Directory organization
* `any/any/` - tests that **every SQL database** must pass (absent for now)
* `any/pg/` - tests that **every PostgreSQL-compatible backend** must pass, it should have symlink to `any/any/` when the last one appears.
* `sdb/pg/` - tests that **only SereneDB PostgreSQL-compatible backend** must pass. It has symlink to `any/pg/` suites with suffix `_any`.
* `pg/` - tests that only reference PostgreSQL backend must pass, but **SereneDB PostgreSQL-compatible backend strives to pass**. It has symlinks to `any/pg/` suites with suffix `_any`.

This structure is applied for each protocol.
```
any
  any/ (1)
  protocolX/ (2) --> (1)
  protocolY/ (3) --> (2)
  ...
sdb
  protocolX/ --> (2)
  protocolY/ --> (3)
  ...
protocolX/ --> (2)
protocolY/ --> (3)
...
```
where `protocolX` is PostgreSQL and `protocolY`, for example, **might be** MySql, DuckDB etc.

Expected output for `any/pg` should be overriden by reference PostgreSQL implementation (as for any other future protocol).

Let's see typical usages.

1) Run tests with only SereneDB PostgreSQL-compatible backend: just use `tests/sqllogic/sdb/pg/*/*.test` path pattern.
2) Run tests with both SereneDB PostgreSQL-compatible and reference PostreSQL: just use `tests/sqllogic/any/pg/*/*.test` path pattern.
3) You're adding new features and want to run SereneDB PostgreSQL-compatible on PostgreSQL tests that prevoiusly weren't passed: just use `tests/sqllogic/any/pg/new_feature/*.test` path pattern.

## Reference PostgreSQL image

Tests under `any/pg/` also run against real PostgreSQL in the `validate-pg` job,
so everything they use has to exist there too. `any/pg/geometry/` needs PostGIS,
which the stock `postgres` images do not ship, so that job runs
`serenedb/serenedb-test-postgres`, built from `fixtures/postgres/Dockerfile` by the
`build-images` workflow: `postgres:18.6` plus the PostGIS package from the PGDG apt repo
the base image already configures. `fixtures/image_tag.sh` gives its tag.

Installing the package does not enable it. A test that needs an extension asks
for it itself, guarded so SereneDB -- where the same functionality is built in
-- never sees the statement:

```
onlyif postgres
statement ok
CREATE EXTENSION IF NOT EXISTS postgis;
```

Do **not** install extensions into `template1` instead. The runner gives every
test file its own database copied from `template1`, so an extension there lands
in all of them: PostGIS alone adds `spatial_ref_sys` to `public` and grantable
privileges on ~1000 functions, which breaks unrelated tests that enumerate
tables or drop roles.

To make another extension available, append its package to the `apt-get install`
line. PGDG carries ~200 packages for PostgreSQL 18 (`pgvector`, `h3`, `cron`,
`hll`, `hypopg`, ...); `apt-cache search postgresql-18-` inside the image lists
them.

One more trap in that setup: the healthcheck has to probe over TCP
(`pg_isready -h 127.0.0.1`). While `docker-entrypoint.sh` runs the init scripts
the server listens on the unix socket only, so a socket probe reports ready
before the database is reachable from the `tests` container.
