---
paths:
  - "tests/**"
  - "resources/otel/**"
---

# Tests

## Where a test goes

- `tests/sqllogic/any/`: behaviour SereneDB shares with PostgreSQL. PostgreSQL is the ground truth: validate on real PostgreSQL before pinning (`scripts/ci/test.py --suite postgres --test 'postgres:*:pg/any/<file>.test'`, which runs the file against the image `tests/sqllogic/fixtures/postgres`); outcomes must match, error text may differ.
- `tests/sqllogic/sdb/`: SereneDB only. `tests/sqllogic/pg/`: PostgreSQL only, validating the spec.
- `tests/sqllogic/recovery/`: a fresh serened per file, for crashes, restarts and anything server-wide ([sqllogic.md](sqllogic.md)).
- `tests/server/<area>/`, `tests/iresearch/`: gtests, for isolated C++ logic a sqllogic test can't reach well.
- `tests/drivers/<lang>/`: client drivers over the wire, on the shared spec in `tests/drivers/spec/`; `tests/drivers/psql/` pins psql's output; `tests/drivers/network/`: pg_hba matching.
- `tests/drivers/stress/`: concurrency and soak profiles, and the harness's own pytest tests (`test_*.py`, the `stress:harness` unit); `tests/drivers/sqlsmith/` and `tests/drivers/fuzz/`: fuzzers.
- `tests/bench/micro/`: microbenchmarks; `scripts/perf/`: end-to-end performance scripts. `tests/duckdb/`: DuckDB's and the extensions' own suites.
- `third_party/re2/re2/testing/`: the re2 fork's own suites, whose exhaustive and random tests check every matching engine against a backtracking reference; CI runs them when `third_party/re2` or what it builds against changes.
- `scripts/ci/tests/`: pytest tests of the CI scripts, which pre-commit runs when those scripts change.

## When a change needs one

- A bug fix, always ([CLAUDE.md](../../CLAUDE.md), "Evidence"); a bug a plain test can't reach (a race, an error or a crash at one spot) gets a fault point.
- A new feature or behaviour change: a sqllogic test in the right subtree, plus a gtest for isolated logic worth pinning.
- A change to plan shape or to which execution path runs: tests pin both `EXPLAIN` and the results, across the shapes it affects.
- Shared low-level code: run its whole sibling test file, and add adversarial inputs (a NULL first row, an all-NULL chunk, boundaries, strings longer than the 12 bytes `string_t` inlines, since inlined ones hide lifetime bugs).
- Anything checked by hand (a crash loop, a driver check) becomes a committed recovery or driver test.
- Never edit a test or an expectation to make a failure go away.
- After a DuckDB update, a changed error text is fine (update the test); an internal error, an assertion or ok -> error is a regression; for error -> ok, find out why before changing the expectation.
- Removed code takes its gtests with it; a sqllogic test changes only when the behaviour it pins changes. No tests that only assert something is gone.

## Running

- `scripts/ci/test.py --build-dir <build>` runs the suites as CI does: every test as a unit in one queue over the machine, the servers and services the units need, a watchdog that saves every thread's stack on a hang, and a summary in `out/test/summary.txt` (every unit in `out/test/results.jsonl`, a failed unit's logs under `out/test/units/`). `--suite` and repeated `--test '<unit glob>'` scope it, `--list` prints the units it would run, `--fast` skips `.test_slow` files, `--corpus` adds the sqlite corpus. It is the only test runner.
- `--server <host>:<port>` runs the sqllogic units against a server you started (CLAUDE.md, "Local smoke server"), for an edit-run loop; `--override` rewrites the files with the results, on one wire engine, and records the generated tests' values.
- `--suite postgres` runs the tests under `pg/` and `any/pg/` against PostgreSQL; `--suite cancellation` cancels queries of random sqllogic files at random moments, on both wire engines, and checks that serened keeps answering; every run picks its files and moments anew, and `SDB_CANCEL_SEED=<seed>` replays the run whose log names that seed.
- The sqlite corpus (`tests/sqllogic/any/pg/sqlite`) is a submodule nothing clones by default: `test.py --corpus` checks it out and runs it, and CI does only when a run asks for it ([ci.md](ci.md)).
- Choose the parallelism before a run (`--jobs`, `nproc / 2` when the machine is shared), never after a failure: a test that fails under load is a bug.
- gtests: one test runs directly (`--gtest_filter=...`); more go through `test.py --suite gtest` or `scripts/gtest-parallel/gtest-parallel`. `iresearch-load-tests` run as one process. `ninja serened` doesn't relink the test binaries.
- Drivers: `tests/drivers/run.sh --port <port> --lang <lang>`, only for languages whose toolchain is installed; a skipped language is not a pass.
- DuckDB suites: `test.py --suite duckdb --duckdb-suite <name>`; run `ninja unittest` first after a change. A suite's skip list holds only the SereneDB divergences that can't pass ([tests/duckdb/README.md](../../tests/duckdb/README.md)).
- re2 fork suites, in a build with `SDB_GTEST`: `ninja re2-tests`, then `test.py --suite re2`.
- Coverage of a change, as CI's `dev` reports it: a build with `-DSDB_COVERAGE=On` (`cmake --preset clangd -B build_cov -DSDB_COVERAGE=On`), the tests through `test.py --build-dir build_cov`, then `scripts/coverage/diff.py --build-dir build_cov --profiles out/test/profiles --out out/coverage`; `--html` also renders the changed files.

```bash
scripts/ci/test.py --build-dir build_clangd --suite sqllogic --test 'sqllogic:*:sdb/pg/simple/*'
scripts/ci/test.py --build-dir build_clangd --suite recovery --test 'recovery:recovery/<file>.test'
scripts/ci/test.py --build-dir build_clangd --suite duckdb --duckdb-suite core --test '*test/sql/aggregate/*'
scripts/ci/test.py --suite sqllogic --server 127.0.0.1:<port> --test 'sqllogic:*:sdb/pg/simple/*'
./scripts/gtest-parallel/gtest-parallel --gtest_filter='*StorageVersion*' ./build/bin/serenedb-tests
```

## When you change ...

- **A python driver test file:** add it to the list in `tests/drivers/python/run.sh`, or nothing runs it.
- **A driver's dependency manifest:** the CI image installs from it ([ci.md](ci.md), "CI images").
- **The OpenTelemetry schema** (`resources/otel/otel_schema.sql`): `scripts/otel/schema.py generate`; its conformance fixtures: `scripts/otel/fixtures.py generate`.
