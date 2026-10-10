---
paths:
  - "tests/**"
---

# Tests

The test tree is split by what runs the test and what it covers:

- `tests/sqllogic/any/...` -- sqllogic against any engine (PG and SereneDB); use for behaviour we expect from both. `any/pg` runs through the symlinks `sdb/pg/any` and `pg/any`, never directly. Validate new SQL behaviour on real PostgreSQL before pinning it (`./tests/sqllogic/run_pg_tests.sh --host <host> --single-port <port>`; CI's `validate pg` uses the `serenedb-test-postgres:18-3.6` image from `tests/sqllogic/docker-compose.validate-pg-tests.yml`): outcomes must match, error text may differ.
- `tests/sqllogic/sdb/...` -- sqllogic against SereneDB only (SereneDB-specific syntax / extensions).
- `tests/sqllogic/pg/...` -- sqllogic against Postgres only (used to validate the spec).
- `tests/sqllogic/recovery/...` -- sqllogic with crash injection (`SET sdb_faults = '...'`) plus a restart; each test runs against a fresh serened + datadir. `SET sdb_faults` is allowed only here (pre-commit `check-fault-points`).
- `tests/server/<area>/...`, `tests/iresearch/...` -- gtest unit tests; use for isolated C++ logic where a sqllogic test would be awkward (library classes / pure functions / hard-to-reproduce bugs).
- `tests/bench/micro/...` -- microbenchmarks for performance claims.
- `tests/duckdb/` -- driver for the **DuckDB-level** suites: DuckDB core's own test tree and each vendored extension's, via DuckDB's `unittest` binary. Built by default; configure with `-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` to skip it.

When a change needs a test:

- Bug fix: always, unless you can argue the bug is uncoverable. The test fails before the fix, for the reason the fix claims. Crash / recovery bugs go under `tests/sqllogic/recovery/`.
- Never edit a test or an expectation just to make a failure go away.
- Anything checked by hand (a crash/restart loop, a driver check) ends up as a committed recovery or driver test.
- After an upstream update, a changed error text is fine (update the test); ok -> error is a regression to fix; error -> ok needs understanding before the expectation changes.
- New feature / behaviour change: sqllogic test in the right subtree above. Add a unit test too if there's isolated C++ logic worth pinning.
- CMake-only changes: rely on CI.
- Doc-only changes: their SQL examples are sqllogic tests (see [Documenting with runnable examples](docs.md#documenting-with-runnable-examples)).
- Code that goes away takes its gtests with it (port them when there is a replacement). A sqllogic test changes only when the behaviour it pins changes, such as an error that no longer happens or a feature that is now implemented: rewrite it to the new behaviour. Don't add tests that only assert that something no longer exists.

```bash
# All sqllogic tests
./tests/sqllogic/run.sh --single-port 7890 --debug true

# Specific tests
./tests/sqllogic/run.sh --single-port 7890 --test 'tests/sqllogic/any/pg/simple/*.test' --debug true

# Recovery tests (auto-restarts serened on injected crashes; needs build/bin/serened)
./tests/sqllogic/run_recovery_tests.sh --runner ../../third_party/sqllogictest-rs

# One recovery test against another build
BUILD_DIR=build_clangd ./tests/sqllogic/run_recovery_tests.sh recovery/<file>.test
```

- `run.sh` connects to a serened that is already running (see [Launch](../../CONTRIBUTING.md#launch)). Scope it with repeatable `--test '<glob>'`; it takes no positional arguments. `--fast` strips the trailing `*` from each `--test` glob, so `*.test*` stops matching `.test_slow` files. Both wire engines (`pg-wire-simple,pg-wire-extended`) run by default.
- The exit code says nothing when no file matched: count one `[OK]`/`[FAILED]` line per file you asked for.
- `run_recovery_tests.sh` starts an auto-restarting serened per file. Its test paths are relative to `tests/sqllogic`; a path that matches nothing still prints PASS. It has no `--help`: arguments it doesn't know go to `run.sh`. Server logs stay in `/tmp/serened-logs-XXXXXX/` (`worker-N-test-M.log` per test, `failures-wN.txt` for the failures).
- One recovery run per account at a time: at start and at exit the script `kill -9`s every `recovery-worker-*` and `run_serened_loop.sh` process the account owns and deletes `/tmp/recovery-worker-*`.
- `run.sh` and the driver and DuckDB runners default to `nproc` jobs, the recovery runner to one worker per file up to `nproc`; on a busy machine use `nproc / 2`. Never lower parallelism to make a flaky test pass: a test that fails under load is a bug.

C++ unit tests:

```bash
./build/bin/iresearch-tests "--gtest_filter=*PhraseFilterTestCase*"
./build/bin/serenedb-tests "--gtest_filter=*VPackLoadInspectorTest*"
./build/bin/serenedb-tests "--gtest_filter=*DataSourceWithSearchTest*"

# All of them, as CI runs them
./scripts/gtest-parallel/gtest-parallel ./build/bin/serenedb-tests
(cd build/bin && python3 ../../scripts/gtest-parallel/gtest_parallel.py ./iresearch-tests)
```

`ninja serened` doesn't relink the test binaries: build the test target you run.

## Other suites

- **Drivers:** `tests/drivers/run.sh --port <port> --lang python` against a running serened (default port 5432). Languages: python, java, js, go, rust, php, csharp, c, ruby, r and psql; `--lang sqlsmith` is the fuzzer.
- **Network:** `tests/drivers/network/run.sh` starts its own serened and checks pg_hba CIDR and mask matching from different `127.x` source addresses.
- **Stress:** `tests/drivers/stress/run.sh --profile smoke` (`smoke`, `soak`, `soak-tsan`, `wedge-probe`, `biglake-reindex-smoke`; `--seconds` and `--workers` override the profile). Parallel DDL, DML and RBAC churn with a consistency oracle and a hang detector, against a serened it starts, kills and crashes itself. It saturates the machine.
- **iresearch load test:** `CORPUS_PATH=$(scripts/ci/steps/iresearch-load-fetch-corpus.bash) build/bin/iresearch-load-tests --gtest_filter='LoadTest*'`; fetching the corpus needs `SEARCHBENCH_S3_KEY_ID` and `SEARCHBENCH_S3_SECRET`.
- **Packages (RTA):** `scripts/ci/steps/08-ci-{deb,docker,tarball}-rta.bash` install the `.deb`, the Docker image or the tarball built by `05-ci-in-docker-package.bash`, start it, and run sqllogic and the network tests against it (the drivers too with `RTA_DRIVERS`).
- The recovery, network and stress runners take `BUILD_DIR=<dir>` (default `build`).

## Running DuckDB's own test suites

DuckDB core and each vendored extension ship sqllogic-style test suites under
`third_party/duckdb/test/` and `third_party/duckdb_<name>/test/`. They run
through DuckDB's `unittest` binary, which is built by default (opt out with
`-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` if you want to skip its ~1.3GB output).

```bash
./tests/duckdb/run.sh                    # every suite
./tests/duckdb/run.sh --suite core       # just duckdb core
./tests/duckdb/run.sh --suite interop    # DuckDB files against the official duckdb/duckdb image
./tests/duckdb/run.sh --list             # suite names
```

Each suite carries a checked-in skip list (`tests/duckdb/config/<suite>.json`)
naming the SereneDB divergences that can't pass, with a reason per entry;
everything else is a regression gate on the fork. See
[tests/duckdb/README.md](../../tests/duckdb/README.md) before adding to it.

The serened-level postgres_scanner tests
(`tests/sqllogic/sdb/pg/duckdb_postgres/*_pgscan.test_slow`) ride the regular
sqllogic runner -- the `_pgscan.` filename suffix triggers
`launch_postgres()` in `tests/sqllogic/run.sh` automatically.

## What CI runs

- Every push to a PR runs `auto | pre-commit`, the pre-commit hooks.
- Ticking **Trigger Jobs** in the PR's "CI Triggers" comment runs `serenedb | build & test` (`build-manual.yml`) with the configs `dev`, `asan`, `tsan` and `perf`, plus `validate pg` when `any/pg` tests changed. `scripts/ci/classify-changes.sh origin/main` prints the gates a diff opens (iresearch, DuckDB suites, changed pg tests); a DuckDB `core` hit also opens the sqlite subtree and sqlsmith.
- `scripts/ci/steps/run-suites.sh` picks the suites per config:
  - `dev`: gtest, network, sqllogic, drivers and recovery, plus the gated iresearch tests and load test, DuckDB suites, sqlite subtree and sqlsmith.
  - `asan`, `tsan`: sqllogic and drivers; the "Extra ASAN" / "Extra TSAN" checkboxes add the rest except recovery.
  - `perf` (optimized, no asserts): sqllogic and drivers, plus the gated sqlite subtree and sqlsmith.
- At night (`jobs-schedule.yml`) everything runs on `dev`, plus the stress soak, a macOS build and the package tests; Monday to Thursday nights also run msan, asan, ubsan and tsan with the full set and stress. Stress runs only at night.
- The CI steps run the suites in Docker with the service containers they need (PostgreSQL, MinIO, iceberg-rest, ClickHouse). The same wrappers run locally:

  ```bash
  SDB_SQLLOGIC_SCOPE=ours BUILD_DIR=build_clangd tests/sqllogic/run_in_docker.sh
  TEST_KIND=recovery BUILD_DIR=build_clangd tests/sqllogic/run_in_docker.sh
  TEST_KIND=duckdb DUCKDB_SUITES=core BUILD_DIR=build_clangd tests/sqllogic/run_in_docker.sh
  BUILD_DIR=build_clangd tests/drivers/run_in_docker.sh
  ```

  The sqllogic scopes: `ours` is `sdb/**` without the sqlite subtree, `all` adds `sdb/pg/any/sqlite/**`, and `biglake` runs `*_iceberg.test_slow` against Google BigLake, which needs its credentials.

A red run:

- `gh pr checks <N> --repo serenedb/serenedb` finds a PR's run. Fork PRs dispatch on the base branch, so `gh run list --branch <branch>` misses them.
- `gh run view <run-id> --repo serenedb/serenedb --log-failed` prints the failed jobs' logs. Every step ends with a verdict line (`UNIT_TESTS=`, `IRESEARCH_TESTS=`, `IRESEARCH_LOAD_TESTS=`, `SQLLOGIC_TESTS=`, `RECOVERY_TESTS=`, `DRIVER_TESTS=`, `DUCKDB_TESTS=`, `NETWORK_TESTS=`, `STRESS_TESTS=` with `PASSED` or `FAILED`); a sqllogic failure prints `failed to run` followed by the record, the expected and the actual output.
- `gh run view <run-id>` lists the artifacts (service logs, sanitizer reports, junit); `gh run download <run-id> -n <name> -D <dir>` fetches one.
- Main's baseline is the nightly: `gh run list --repo serenedb/serenedb --workflow jobs-schedule.yml --limit 10`.
