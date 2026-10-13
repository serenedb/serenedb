---
paths:
  - "tests/**"
  - "resources/otel/**"
---

# Tests

- `tests/sqllogic/any/...` -- sqllogic against any engine (PG and SereneDB); use for behaviour we expect from both. Validate new SQL behaviour on real PostgreSQL before pinning it (`PG_CHANGED_TESTS=any/pg/<file>.test ./tests/sqllogic/run_pg_tests.sh --host <host> --single-port <port>` for one file, the whole `pg/**` suite without the variable; CI's `validate pg` runs PostgreSQL 18 with PostGIS from the fixture image `tests/sqllogic/fixtures/postgres`; locally, run that image on a free port): outcomes must match, error text may differ.
- `tests/sqllogic/sdb/...` -- sqllogic against SereneDB only (SereneDB-specific syntax / extensions).
- `tests/sqllogic/pg/...` -- sqllogic against Postgres only (used to validate the spec).
- `tests/sqllogic/recovery/...` -- sqllogic with crash injection (`SET sdb_faults = '...'`) plus a restart; each test runs against a fresh serened + datadir. Fault points are armed only here (see "Fault points" in [sqllogic.md](sqllogic.md)).
- `tests/server/<area>/...`, `tests/iresearch/...` -- gtest unit tests; use for isolated C++ logic where a sqllogic test would be awkward (library classes / pure functions).
- `tests/bench/micro/...` -- microbenchmarks for performance claims.
- `tests/duckdb/` -- driver for the **DuckDB-level** suites: DuckDB core's own test tree and each vendored extension's.

When a change needs a test:

- Bug fix: always. Write and run the test before the fix: it fails for the reason the fix claims. A bug a plain test can't reach (a race, an error or a crash at one spot) gets its test through a fault point (see "Fault points" in [sqllogic.md](sqllogic.md)). Crash / recovery bugs go under `tests/sqllogic/recovery/`.
- Never edit a test or an expectation just to make a failure go away.
- A failing test your change doesn't touch: report it to the user with its failure, and leave the test alone.
- Anything checked by hand (a crash/restart loop, a driver check) ends up as a committed recovery or driver test.
- After an upstream update, a changed error text for the same failure is fine (update the test); an internal error or an assertion in its place, or ok -> error, is a regression to fix; for error -> ok, find out why it now succeeds (upstream supports it, or a check got lost) before the expectation changes.
- New feature / behaviour change: sqllogic test in the right subtree above. Add a unit test too if there's isolated C++ logic worth pinning.
- CMake-only changes: configure every preset the change affects, and build in the one you work with.
- Doc-only changes: their SQL examples are sqllogic tests (see [Documenting with runnable examples](docs.md#documenting-with-runnable-examples)).
- Code that goes away takes its gtests with it (port them when there is a replacement). A sqllogic test changes only when the behaviour it pins changes, such as an error that no longer happens or a feature that is now implemented: rewrite it to the new behaviour. Don't add tests that only assert that something no longer exists.

```bash
# run.sh's default: sdb/**, the SQLite subtree included
./tests/sqllogic/run.sh --single-port <port> --debug true

# What CI's `ours` scope runs: the same without the SQLite subtree
SDB_SQLLOGIC_SCOPE=ours ./tests/sqllogic/run_sdb_tests.sh --single-port <port>

# Specific tests
./tests/sqllogic/run.sh --single-port <port> --test 'tests/sqllogic/any/pg/simple/*.test' --debug true

# Recovery tests (needs build/bin/serened)
./tests/sqllogic/run_recovery_tests.sh

# One recovery test against another build
BUILD_DIR=build_clangd ./tests/sqllogic/run_recovery_tests.sh recovery/<file>.test
```

- `run.sh` connects to a serened that is already running. Scope it with repeatable `--test '<glob>'`; it takes no positional arguments. A boolean flag (`--fast`, `--override`, `--debug`) takes the next argument as its value unless that starts with `--`, so give it its value (`--fast=true`) when anything but another option follows. `--fast` strips the trailing `*` from each `--test` glob, so `*.test*` stops matching `.test_slow` files. Both wire engines (`pg-wire-simple,pg-wire-extended`) run by default, one after the other.
- The exit code says nothing when no file matched: count one `[OK]`/`[FAILED]` line per file you asked for.
- `run_recovery_tests.sh` starts an auto-restarting serened per file and runs `pg-wire-simple` only. Its test paths are relative to `tests/sqllogic`; a path that matches nothing still prints PASS. It has no `--help`: arguments it doesn't know go to `run.sh`. Server logs stay in `/tmp/serened-logs-XXXXXX/` (`worker-N-test-M.log` per test, `failures-wN.txt` for the failures).
- One recovery run per account at a time: at start and at exit the script `kill -9`s every `recovery-worker-*` and `run_serened_loop.sh` process the account owns, and at exit it deletes `${TMPDIR:-/tmp}/recovery-worker-*`. Check `pgrep -u $USER -f run_serened_loop.sh` first; if another run is going, ask the user.
- `run.sh` and the driver and DuckDB runners default to `nproc` jobs, the recovery runner to one worker per file up to `nproc`. Pick the parallelism before a run (`nproc / 2` when other work shares the machine), never after a failure: a test that fails under load is a bug, in the code or in the test.

C++ unit tests:

```bash
./build/bin/iresearch-tests "--gtest_filter=*PhraseFilterTestCase*"
./build/bin/serenedb-tests "--gtest_filter=*StorageVersionTest*"

# All of them, as CI runs them
./scripts/gtest-parallel/gtest-parallel ./build/bin/serenedb-tests
(cd build/bin && python3 ../../scripts/gtest-parallel/gtest_parallel.py ./iresearch-tests)
```

`ninja serened` doesn't relink the test binaries: build the test target you run.

## Other suites

- **Drivers:** `tests/drivers/run.sh --port <port> --lang python` against a running serened (default port 5432). Languages: python, java, js, go, rust, php, csharp, c, ruby, r and psql; `--lang sqlsmith` is the fuzzer.
- **Network:** `tests/drivers/network/run.sh` starts its own serened and checks pg_hba CIDR and mask matching.
- **Stress:** `tests/drivers/stress/run.sh --profile smoke` (`smoke`, `soak`, `soak-tsan`, `wedge-probe`, `biglake-reindex-smoke`; `--seconds` and `--workers` override the profile). It saturates the machine.
- **iresearch load test:** `CORPUS_PATH=$(scripts/ci/steps/iresearch-load-fetch-corpus.bash) build/bin/iresearch-load-tests --gtest_filter='LoadTest*'`; fetching the corpus needs `SEARCHBENCH_S3_KEY_ID` and `SEARCHBENCH_S3_SECRET`.
- **Packages (RTA):** `scripts/ci/steps/08-ci-{deb,docker,tarball}-rta.bash` install the `.deb`, the Docker image or the tarball built by `05-ci-in-docker-package.bash`, start it, and run sqllogic and the network tests against it (the drivers too with `RTA_DRIVERS`).
- The recovery, network and stress runners take `BUILD_DIR=<dir>` (default `build`).

## Running DuckDB's own test suites

```bash
./tests/duckdb/run.sh                    # every suite
./tests/duckdb/run.sh --suite core       # just duckdb core
./tests/duckdb/run.sh --suite interop    # DuckDB files against the official duckdb/duckdb image
./tests/duckdb/run.sh --list             # suite names
```

`run.sh` builds DuckDB's `unittest` binary only when it is missing: after a change, run `ninja unittest` first.

A suite can carry a checked-in skip list (`tests/duckdb/config/<suite>.json`)
naming the SereneDB divergences that can't pass, with a reason per entry;
everything else is a regression gate on the fork. See
[tests/duckdb/README.md](../../tests/duckdb/README.md) before adding to it.

## When you change ...

Each of these has a follow-up step that nothing runs for you.

- **A python driver test file:** add it to the list in `tests/drivers/python/run.sh`, or no suite runs it.
- **The OpenTelemetry schema** (`resources/otel/otel_schema.sql`): `scripts/otel/schema.py generate` rewrites the sqllogic include generated from it. Its conformance fixtures (`resources/otel/conformance/*/*.json`): `scripts/otel/fixtures.py generate` writes the `.pb` beside each.
