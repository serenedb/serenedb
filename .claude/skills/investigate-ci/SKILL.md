---
name: investigate-ci
description: Investigate a failed SereneDB CI run (nightly or PR build) or a bug report. Find what failed, whether a PR caused it, the root cause, and a local repro. Used by the pudgeai bot and by developers.
---

# Investigating SereneDB CI failures

## Where CI runs

| Workflow | When | What |
|---|---|---|
| `.github/workflows/jobs-schedule.yml` ("auto \| nightly") | Daily at 05:00 UTC, plus 01:00 UTC Mon–Thu for sanitizers | Daily: `everything / dev` (iresearch, DuckDB suites, SQLite, SQLsmith, stress soak), a macOS build, and `release-check` (Deb, Docker and tarball install checks). Sanitizer runs: msan Mon, asan Tue, ubsan Wed, tsan Thu, each with 10,000 SQLsmith queries and a stress soak. Coverage only runs by hand. Results go to Telegram |
| `.github/workflows/build-manual.yml` | PR builds, started from the "Trigger Jobs" checkbox comment | linux-amd64 by default, with ASAN+TSAN functional runs. Checkboxes add arm64, "run everything", extra ASAN/TSAN, BigLake |
| `.github/workflows/build-reusable.yml` | Called by both | Build in Docker, then `scripts/ci/steps/run-suites.sh`, then upload `out/` as `test-artifacts-<config>-<location>-<run_number>` (kept 30 days) |

## Reading a failed job log

`run-suites.sh` wraps each suite in a log group named after its step script and prints a marker when a suite fails:

- `FAILED (<rc>): bash …/scripts/ci/steps/<NNN>-….bash` means the suite failed and the job is red.
- `SOFT-FAILED (<rc>): …` means a soft-fail suite (currently the stress tests) failed. The job stays green on that account, but the finding can still be a real bug.

| Step script | Suite | Failure looks like |
|---|---|---|
| `042-…-iresearch-tests` | iresearch gtests | `[  FAILED  ] Suite.Test` |
| `043-…-serenedb-tests` | server gtests | `[  FAILED  ] Suite.Test` |
| `044-…-sqllogic-tests` | sqllogictest-rs over `tests/sqllogic` | `<path>.test .. [FAILED]` (passing lines end in `[OK]`) |
| `045-…-recovery-tests` | crash injection via `SET sdb_faults` | failing `recovery/*.test` |
| `046-…-iresearch-load-test` | load test, needs the S3 corpus | a corpus fetch failure skips it but still fails the job |
| `047-…-driver-tests` | client drivers in many languages, plus SQLsmith when `SDB_DRV_LANG=sqlsmith` | `===== [<lang>] END (FAIL …)` and the summary table under `[drivers] SUMMARY` |
| `048-…-duckdb-tests` | DuckDB's own suites through the embedded fork | `===== [duckdb] END (rc=…)` |
| `049-…-network-tests` | network tests | marker only |
| `051-…-stress-tests` | stress soak (soft-fail) | `[stress] FAIL verdict=… findings=N`, a `findings (N):` list and a `reproduce:` command with the seed |

Lines from the test container are prefixed with `tests-1  |`. Server logs come from `serenedb-single-1  |`.

## Infrastructure or product?

Treat these as infrastructure first:

- Maven or pip failing to download, or `Network is unreachable` inside the drivers container.
- A missing S3 corpus.
- Runner shutdowns.
- `No space left on device`.

Say so plainly and don't look for a code cause. Sanitizer reports, assertion failures, wrong results in sqllogic, and stress findings like `restart_after_injected_crash_failed` are product bugs until shown otherwise.

## Did this PR cause it?

1. Check whether the same failure appears in recent nightlies on `main`. The nightly run history and the issues labelled `pudgeai` and `ci-failure` both help.
2. Check whether the PR diff touches the failing code path. Use `git log -p` and `git blame` on the files in the stack trace or test.
3. If it fails on main without the PR, it's pre-existing. If the PR touches the area and main is green, it's likely the PR's.

## Reproducing locally

Build (see CONTRIBUTING.md):

```bash
cmake --preset lldb -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21
ninja
```

| What | Command |
|---|---|
| One sqllogic test | `./tests/sqllogic/run.sh --single-port 7890 --test 'tests/sqllogic/<subtree>/<file>.test' --debug true` |
| Recovery tests | `./tests/sqllogic/run_recovery_tests.sh --jobs 1 --runner ../../third_party/sqllogictest-rs` (needs `build/bin/serened`) |
| gtests | `./build/bin/serenedb-tests "--gtest_filter=*Name*"`, `./build/bin/iresearch-tests "--gtest_filter=*Name*"` |
| DuckDB suites | `./tests/duckdb/run.sh --suite core` |
| Stress finding | the `reproduce:` line from the log, e.g. `python3 tests/drivers/stress/main.py --profile soak --scenario ddl_churn --seconds 900 --workers 8 --restarts 3 --seed <seed> --binary build/bin/serened` |
| SQLsmith finding | `sqlsmith --target="host=localhost port=5432 dbname=postgres user=postgres" --max-queries=2000 --seed=<seed> --verbose` |
| Local server | see "Local smoke server" in CLAUDE.md |

## Where a regression test goes

| Subtree | Use for |
|---|---|
| `tests/sqllogic/any/pg/` | Behaviour PostgreSQL and SereneDB share. `validate-pg` also runs these against real PostgreSQL |
| `tests/sqllogic/sdb/pg/` | SereneDB-only behaviour: full-text, vector search, extensions |
| `tests/sqllogic/recovery/` | Crash and recovery bugs |
| `tests/server/<area>/`, `tests/iresearch/` | gtests for isolated C++ logic |

Read a sibling `.test` file first: directives, retries and connection naming are set by example.

## Writing up findings

- Quote the 3–10 log lines that show the failure.
- Give the root cause as `path/to/file.cpp:123` at the commit under investigation.
- Say whether the PR caused it, using the steps above.
- Give the exact repro command.
- State a confidence: possible, likely or confirmed.
