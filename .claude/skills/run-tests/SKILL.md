---
name: run-tests
description: Run SereneDB tests locally - sqllogic (run.sh), recovery, gtest, python/psql driver and DuckDB suites - and reproduce a CI failure. Use when running or re-running tests, when a run "passes" suspiciously fast, or when a test fails with Connection refused.
---

# Running SereneDB tests

Run only what covers the change unless the user asked for a suite. CONTRIBUTING.md "Build", "Launch" and "Test" give the base commands (presets, `serened` launch, `run.sh`, gtest filters); this skill adds what they leave out. On a shared machine use a free port instead of its 7890.

## Pick the binary

| Build | Asserts | `sdb_faults` | Use for |
|---|---|---|---|
| `build/` (preset `lldb`, Debug) | yes | yes | debugging; slow suites |
| `build_clangd/` (preset `clangd`, RelWithDebInfo) | yes | yes | correctness runs, recovery, gtests |
| `build_bench/`, `build_perf/` | no | no | timing only; recovery tests fail with `unrecognized configuration parameter "sdb_faults"` |

Check what you actually test: `ls -la <dir>/bin/serened` must be newer than your edits, and the build log must show the relink. A no-op `ninja` keeps a stale binary.

## sqllogic: `tests/sqllogic/run.sh`

It connects to a serened that is already running; it never starts one. Start one per CLAUDE.md "Local smoke server".

```bash
./tests/sqllogic/run.sh --single-port <port> --fast \
  --test 'tests/sqllogic/sdb/pg/index/*.test' --test 'tests/sqllogic/sdb/pg/ddl/alter_table.test'
```

- No positional arguments: `--fast <path>` makes `<path>` the value of `--fast` and runs the whole default tree. Scope only with repeatable `--test '<repo-relative glob>'`; a directory is not expanded.
- `--fast` drops `.test_slow` files. `--jobs` defaults to nproc; lower it when the box is busy.
- Both wire engines run by default (`pg-wire-simple,pg-wire-extended`).
- The exit code lies when nothing ran: count one `[OK]`/`[FAILED]` line per file you asked for. "Finished in 0 ms" means the glob matched nothing.
- `--debug true` builds and uses the debug Rust runner. Without cargo on your account, use `SDB_SKIP_SQLLOGIC_BUILD=1` and the prebuilt `.cache/cargo-target/release/sqllogictest` (then don't pass `--debug true`).
- `--database` doubles as the sqllogictest label (default `serenedb`); changing it flips `onlyif`/`skipif`. Keep the default.
- Against real PostgreSQL: `./tests/sqllogic/run_pg_tests.sh --host <host> --single-port <port>`.

## Recovery: `tests/sqllogic/run_recovery_tests.sh`

Starts its own auto-restarting serened per file, so it needs no server.

```bash
cd tests/sqllogic && BUILD_DIR=build_clangd ./run_recovery_tests.sh --jobs 4 recovery/<file>.test
```

- Test paths are relative to `tests/sqllogic` (or absolute). A repo-relative path matches nothing and still prints PASS.
- Any argument that is not a test path, `--jobs`, `--fast` or `--runner` is forwarded to run.sh. `--help` therefore starts the full suite.
- Logs stay in `/tmp/serened-logs-XXXXXX/`: `worker-N-test-M.log` is that test's serened log, `failures-wN.txt` lists failures.
- A boot crash loop looks like "Connection refused" at the first `connection after_crash` record; read the worker log, then see the `debug-serened` skill.

## gtest

- One test: `./build_clangd/bin/serenedb-tests --gtest_filter='Suite.Name'`.
- Several: `python3 scripts/gtest-parallel/gtest_parallel.py ./build_clangd/bin/iresearch-tests -- --gtest_filter='Suite.*'` (iresearch tests share state and need the process isolation).
- `ninja serened` doesn't relink the test binaries: build the test target you run.

## Other suites

- Python drivers: `tests/drivers/python/run.sh` runs a hardcoded file list (`for extra in ...`); a new test file must be added there or CI never runs it.
- Changing a serened flag breaks the CLI-help fixture: `python3 tests/drivers/python/cli_help.py override --bin build/bin/serened` (binary built from the tree you ship).
- DuckDB suites: `./tests/duckdb/run.sh --suite core` (skip lists in `tests/duckdb/config/<suite>.json`).

## Reproduce CI

Run the CI step the way CI does before narrowing down: `scripts/ci/steps/run-suites.sh` drives `scripts/ci/steps/04*-ci-in-docker-run-*.bash` (sqllogic: `SDB_SQLLOGIC_SCOPE=ours BUILD_DIR=<dir> tests/sqllogic/run_in_docker.sh`). Job logs: `gh api repos/serenedb/serenedb/actions/jobs/<job-id>/logs | grep -E 'FAILED|Error'`. Flaky under ASAN/TSAN only: compare with the `dev` job and with other branches' runs before blaming the change.
