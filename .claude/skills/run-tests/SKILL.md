---
name: run-tests
description: Run SereneDB's test suites locally the way CI runs them - gtest, sqllogic, recovery, DuckDB suites, drivers and sqlsmith, network, stress, the iresearch load test and the package (RTA) tests - pick the tests that cover a change, and read a red CI run. Use when running tests, choosing which tests to run, or looking into a CI failure.
---

# Running SereneDB tests

Run the tests that cover the change unless the user asks for a suite; CI runs the rest. CONTRIBUTING.md "Build", "Launch" and "Test" have the base commands; this maps every suite CI runs to its local command.

## Before running

- Build the targets you run (`ninja -C <build dir> serened serenedb-tests`); `ninja serened` doesn't relink the test binaries. Never pipe `ninja` into `tail`/`grep`: the pipe hides its exit code.
- `<build dir>/bin/serened` must be newer than your edits; a no-op `ninja` keeps a stale binary.
- Build flavour: CI's `dev` config has asserts, fault injection and the gtest binaries, like the `clangd` (RelWithDebInfo) and `lldb` (Debug) presets. `bench` and `perf` have neither fault injection nor gtests: recovery tests and anything that sets `sdb_faults` fail there.
- Parallelism: the runners default to `nproc`; on a box that is already busy use `nproc / 2`. Never lower it to make a flaky test pass: a test that fails under load is a bug.
- A script that needs a `cd` runs in a subshell, `( cd tests/sqllogic && ... )`, so later commands keep the repo root.

## What CI runs

- Every push to a PR: `auto | pre-commit`; `pre-commit run --files <files>` runs the same hooks.
- Ticking **Trigger Jobs** in the PR's "CI Triggers" comment: `serenedb | build & test` (`build-manual.yml`) with the configs `dev`, `asan`, `tsan` and `perf`, plus `validate pg` when `any/pg` tests changed. `scripts/ci/classify-changes.sh origin/main` prints the gates a diff opens (iresearch, DuckDB suites, changed pg tests); a DuckDB `core` hit also opens the sqlite subtree and sqlsmith.
- `scripts/ci/steps/run-suites.sh` picks the suites per config:
  - `dev`: gtest, network, sqllogic, drivers and recovery, plus the gated iresearch tests and load test, DuckDB suites, sqlite subtree and sqlsmith.
  - `asan`, `tsan`: sqllogic and drivers; the "Extra ASAN" / "Extra TSAN" checkboxes add the rest except recovery.
  - `perf` (optimized, no asserts): sqllogic and drivers, plus the gated sqlite subtree and sqlsmith.
- At night (`jobs-schedule.yml`): everything on `dev` plus the stress soak, a macOS build and the package tests; from Monday to Thursday also msan, asan, ubsan and tsan with the full set and stress. Stress runs only at night.

## Suites

### gtest (CI steps 042, 043, 046)

- `./build_clangd/bin/serenedb-tests --gtest_filter='Suite.Name'`; all of them: `./scripts/gtest-parallel/gtest-parallel ./build_clangd/bin/serenedb-tests`.
- iresearch: `( cd build_clangd/bin && python3 ../../scripts/gtest-parallel/gtest_parallel.py ./iresearch-tests -- --gtest_filter='Suite.*' )`.
- iresearch load test: `CORPUS_PATH=$(scripts/ci/steps/iresearch-load-fetch-corpus.bash) build_clangd/bin/iresearch-load-tests --gtest_filter='LoadTest*'`; the corpus fetch needs `SEARCHBENCH_S3_KEY_ID` and `SEARCHBENCH_S3_SECRET`.

### sqllogic (044)

`tests/sqllogic/run.sh` connects to a serened that is already running; start one per CLAUDE.md "Local smoke server".

```bash
./tests/sqllogic/run.sh --single-port <port> --fast \
  --test 'tests/sqllogic/sdb/pg/index/*.test' --test 'tests/sqllogic/sdb/pg/ddl/alter_table.test'
```

- Scope with repeatable `--test '<glob>'`; the runner takes no positional arguments.
- `--fast` drops `.test_slow` files. Both wire engines run by default (`pg-wire-simple,pg-wire-extended`).
- The exit code says nothing when nothing ran: count one `[OK]`/`[FAILED]` line per file you asked for.
- CI's scopes (`tests/sqllogic/run_sdb_tests.sh`): `ours` is `sdb/**` without the sqlite subtree, `all` adds `sdb/pg/any/sqlite/**`, `biglake` runs `*_iceberg.test_slow` against Google BigLake and needs its credentials.
- Against real PostgreSQL, as `validate pg` does: `./tests/sqllogic/run_pg_tests.sh --host <host> --single-port <port>`.

### recovery (045)

```bash
( cd tests/sqllogic && BUILD_DIR=build_clangd ./run_recovery_tests.sh recovery/<file>.test )
```

- Starts an auto-restarting serened per file; `--jobs` defaults to one worker per file, at most `nproc`.
- One recovery run per account at a time: at start and at exit the script `kill -9`s every `recovery-worker-*` and `run_serened_loop.sh` process the account owns and deletes `/tmp/recovery-worker-*`.
- Test paths are relative to `tests/sqllogic`; a repo-relative path matches nothing and still prints PASS. Arguments other than test paths, `--jobs`, `--fast` and `--runner` go to run.sh, so `--help` starts the full suite.
- Logs stay in `/tmp/serened-logs-XXXXXX/`: `worker-N-test-M.log` is that test's serened log, `failures-wN.txt` lists the failures.

### DuckDB suites (048)

`./tests/duckdb/run.sh --suite <suite>`, with `core`, `avro`, `azure`, `httpfs`, `iceberg`, `inet`, `markdown`, `postgres_scanner`, `spatial` or `interop`; skip lists in `tests/duckdb/config/<suite>.json`. `postgres_scanner` needs a PostgreSQL server.

### drivers and sqlsmith (047)

`tests/drivers/run.sh --port <port> --lang python` against a running serened. The default port is 5432; the languages are python, java, js, go, rust, php, csharp, c, ruby, r and psql, and `--lang sqlsmith` is the fuzzer. The python suite also checks the CLI-help and OpenTelemetry fixtures (CLAUDE.md "When you change ...").

### network (049)

`BUILD_DIR=build_clangd tests/drivers/network/run.sh` starts its own serened and checks pg_hba CIDR and mask matching from different `127.x` source addresses, so it runs on the same host as the server.

### stress (051)

`BUILD_DIR=build_clangd tests/drivers/stress/run.sh --profile smoke`; profiles `smoke`, `soak`, `soak-tsan`, `wedge-probe` and `biglake-reindex-smoke`, with `--seconds` and `--workers` to override them. Parallel DDL, DML and RBAC churn with a consistency oracle and a hang detector, against a serened it starts, kills and crashes itself. It saturates the box.

### package tests, RTA (08)

`scripts/ci/steps/08-ci-{deb,docker,tarball}-rta.bash` install the `.deb`, the Docker image or the tarball built by `05-ci-in-docker-package.bash`, start it, and run sqllogic and the network tests against it (the drivers too with `RTA_DRIVERS`). They run in `make-package.yml` (manual, with `RTA=test` or `required`) and in the nightly release check; locally they need Docker, `BUILD_IMAGE` and the packages.

## Run a suite the way CI does

The CI steps run the suites in Docker with the service containers they need (PostgreSQL, MinIO, iceberg-rest, ClickHouse) on the image `serenedb/serenedb-build-ubuntu:latest`:

```bash
SDB_SQLLOGIC_SCOPE=ours BUILD_DIR=build_clangd tests/sqllogic/run_in_docker.sh
TEST_KIND=recovery BUILD_DIR=build_clangd tests/sqllogic/run_in_docker.sh
TEST_KIND=duckdb DUCKDB_SUITES=core BUILD_DIR=build_clangd tests/sqllogic/run_in_docker.sh
BUILD_DIR=build_clangd tests/drivers/run_in_docker.sh
```

## A red CI run

- Find the run with `gh pr checks <N> --repo serenedb/serenedb`. Fork PRs dispatch on the base branch, so `gh run list --branch <branch>` misses them.
- Failed logs: `gh run view <run-id> --repo serenedb/serenedb --log-failed`. Every step ends with a verdict line (`UNIT_TESTS=`, `IRESEARCH_TESTS=`, `IRESEARCH_LOAD_TESTS=`, `SQLLOGIC_TESTS=`, `RECOVERY_TESTS=`, `DRIVER_TESTS=`, `DUCKDB_TESTS=`, `NETWORK_TESTS=`, `STRESS_TESTS=` with `PASSED` or `FAILED`); a sqllogic failure prints `failed to run` followed by the record, the expected and the actual output.
- Artifacts (service logs, sanitizer reports, junit): `gh run view <run-id> --repo serenedb/serenedb` lists them, `gh run download <run-id> --repo serenedb/serenedb -n <name> -D <dir>` fetches one.
- Main's baseline is the nightly: `gh run list --repo serenedb/serenedb --workflow jobs-schedule.yml --limit 10`.
