---
paths:
  - "tests/**"
  - "resources/otel/**"
---

# Tests

## Where a test goes

- `tests/sqllogic/any/`: behaviour SereneDB shares with PostgreSQL. PostgreSQL is the ground truth: validate on real PostgreSQL before pinning (`PG_CHANGED_TESTS=any/pg/<file>.test ./tests/sqllogic/run_pg_tests.sh --host <host> --single-port <port>`, against the image `tests/sqllogic/fixtures/postgres`); outcomes must match, error text may differ.
- `tests/sqllogic/sdb/`: SereneDB only. `tests/sqllogic/pg/`: PostgreSQL only, validating the spec.
- `tests/sqllogic/recovery/`: a fresh serened per file, for crashes, restarts and anything server-wide ([sqllogic.md](sqllogic.md)).
- `tests/server/<area>/`, `tests/iresearch/`: gtests, for isolated C++ logic a sqllogic test can't reach well.
- `tests/drivers/<lang>/`: client drivers over the wire, on the shared spec in `tests/drivers/spec/`; `tests/drivers/psql/` pins psql's output; `tests/drivers/network/`: pg_hba matching.
- `tests/drivers/stress/`: concurrency and soak profiles; `tests/drivers/sqlsmith/` and `tests/drivers/fuzz/`: fuzzers.
- `tests/bench/micro/`: microbenchmarks; `scripts/perf/`: end-to-end performance scripts. `tests/duckdb/`: DuckDB's and the extensions' own suites.

## When a change needs one

- A bug fix, always: the test is written and run first, and fails for the reason the fix claims. A bug a plain test can't reach (a race, an error or a crash at one spot) gets a fault point.
- A new feature or behaviour change: a sqllogic test in the right subtree, plus a gtest for isolated logic worth pinning.
- A change to plan shape or to which execution path runs: tests pin both `EXPLAIN` and the results, across the shapes it affects.
- Shared low-level code: run its whole sibling test file, and add adversarial inputs (a NULL first row, an all-NULL chunk, boundaries).
- Anything checked by hand (a crash loop, a driver check) becomes a committed recovery or driver test.
- Never edit a test or an expectation to make a failure go away. A failing test you didn't touch is still a bug to fix, in the code or in the test.
- After a DuckDB update, a changed error text is fine (update the test); an internal error, an assertion or ok -> error is a regression; for error -> ok, find out why before changing the expectation.
- Removed code takes its gtests with it; a sqllogic test changes only when the behaviour it pins changes. No tests that only assert something is gone.

## Running

- `run.sh` connects to a running serened (CLAUDE.md, "Local smoke server"); scope it with repeated `--test '<glob>'`, no positional arguments. Both wire engines run by default. Boolean flags take the next argument as their value: write `--fast=true`. `--fast` skips `.test_slow` files.
- The exit code says nothing when no file matched: count one `[OK]`/`[FAILED]` line per file.
- `run_recovery_tests.sh` starts its own servers; its paths are relative to `tests/sqllogic`, and a path that matches nothing still prints PASS. One recovery run per account at a time: it kills the account's other recovery servers. Logs stay in `/tmp/serened-logs-*/`.
- Choose the parallelism before a run (`nproc / 2` when the machine is shared), never after a failure: a test that fails under load is a bug.
- gtests: one test runs directly (`--gtest_filter=...`); more go through `scripts/gtest-parallel/gtest-parallel`, as CI runs them. `iresearch-load-tests` run as one process. `ninja serened` doesn't relink the test binaries.
- Drivers: `tests/drivers/run.sh --port <port> --lang <lang>`, only for languages whose toolchain is installed; a skipped language is not a pass.
- DuckDB suites: `./tests/duckdb/run.sh --suite <name>` (`--list` names them); run `ninja unittest` first after a change. A suite's skip list holds only the SereneDB divergences that can't pass ([tests/duckdb/README.md](../../tests/duckdb/README.md)).

```bash
./tests/sqllogic/run.sh --single-port <port> --test 'tests/sqllogic/sdb/pg/simple/*.test'
BUILD_DIR=build_clangd ./tests/sqllogic/run_recovery_tests.sh recovery/<file>.test
./scripts/gtest-parallel/gtest-parallel --gtest_filter='*StorageVersion*' ./build/bin/serenedb-tests
```

## When you change ...

- **A python driver test file:** add it to the list in `tests/drivers/python/run.sh`, or nothing runs it.
- **A driver's dependency manifest:** the CI image installs from it ([ci.md](ci.md), "CI images").
- **The OpenTelemetry schema** (`resources/otel/otel_schema.sql`): `scripts/otel/schema.py generate`; its conformance fixtures: `scripts/otel/fixtures.py generate`.
