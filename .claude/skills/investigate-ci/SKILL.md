---
name: investigate-ci
description: Investigate a failed SereneDB CI run from a PR number, run URL or job URL - list failed jobs, pull the failing tests and their records from the logs and artifacts, decide flaky vs real by comparing with main and other branches, and name the likely cause. Read-only. Use when CI is red or someone asks why a check failed.
argument-hint: "<PR-number | run-url | job-url>"
---

# Investigate a CI failure

A read-only first pass: turn one PR or run into a per-test verdict (real, flaky or infrastructure) with evidence and a cause hypothesis.

## Hard rules

- Read-only. Never re-run jobs, tick the CI Triggers checkbox, push, comment, or edit the PR. Report; the user decides.
- Read source at the failing commit with `git show <sha>:<path>`; don't check it out.
- Working files go to `.cache/claude/ci/<run-id>/` (gitignored, per checkout), never a shared `/tmp` path.

## How CI is wired

- Every push to a PR runs `auto | pre-commit` (`.github/workflows/simple.yml`): the pre-commit hooks. Reproduce with `pre-commit run --files <files>`.
- Opening a PR posts a "CI Triggers" comment; ticking **Trigger Jobs** starts `serenedb | build & test` (`build-manual.yml` via `build-reusable.yml`). Jobs: `classify changes`, then `dev` (asserts + tests + fault injection), `asan`, `tsan`, `perf` (optimized, no tests), `validate pg`, `Report Status`.
- `scripts/ci/classify-changes.sh` picks the suites from the diff; `scripts/ci/steps/run-suites.sh` runs the steps `scripts/ci/steps/04*-ci-in-docker-run-*.bash`.
- Each step prints a verdict line: `SQLLOGIC_TESTS=`, `RECOVERY_TESTS=`, `DRIVER_TESTS=`, `DUCKDB_TESTS=`, `IRESEARCH_TESTS=` ... `PASSED|FAILED`.

## Steps

1. Find the run and its failed jobs:

   ```bash
   gh pr checks <N> --repo serenedb/serenedb
   gh run list --repo serenedb/serenedb --workflow build-manual.yml --branch <branch> --limit 5
   gh run view <run-id> --repo serenedb/serenedb --json jobs --jq '.jobs[] | "\(.databaseId) \(.conclusion) \(.name)"'
   ```

2. Pull the failed logs once and read everything from the file:

   ```bash
   mkdir -p .cache/claude/ci/<run-id>
   gh run view <run-id> --repo serenedb/serenedb --log-failed > .cache/claude/ci/<run-id>/failed.log
   grep -nE '\[FAILED\]|_TESTS=FAILED|\[  FAILED  \]|ERROR: AddressSanitizer|WARNING: ThreadSanitizer' .cache/claude/ci/<run-id>/failed.log
   ```

   - sqllogic: `<file>.test .. [FAILED] after N ms`, then `failed to run \`<file>\`` followed by the record, expected and actual output: `grep -n -A40 'failed to run' failed.log`. Every file runs on both wire engines; a failure on only one points at the protocol path.
   - gtest: `[  FAILED  ] Suite.Name`.
   - A job that died without a verdict line: look for the build error (`error:`) or a timeout in the tail of that job's log.

3. Artifacts, when the log isn't enough:

   ```bash
   gh api repos/serenedb/serenedb/actions/runs/<run-id>/artifacts --jq '.artifacts[].name'
   gh run download <run-id> --repo serenedb/serenedb -n test-artifacts-<config>-linux-amd64-<run number> -D .cache/claude/ci/<run-id>
   ```

   They hold `logs/` (`sqllogic-tests.log`, `drivers-tests.log`, cmake/make logs, the service containers' logs: postgres, minio, iceberg-rest, ...), `sanitizers/` and `drivers-tests/` (junit per driver).

4. Flaky or real:
   - Same job on main: `gh run list --repo serenedb/serenedb --workflow build-manual.yml --branch main --limit 10 --json databaseId,conclusion,createdAt`.
   - Same test on other branches: take recent failed runs (`gh run list ... --status failure --limit 20`), pull only their failed logs and grep for the test file.
   - Failing only under `asan`/`tsan` while `dev` passes, with a timing-shaped symptom (a periodic refresh count off by one, a retry budget exceeded): suspect sanitizer slowdown, and say so with the evidence; it is still a bug if the test can't tolerate a slow machine.
   - Fails on the PR and never elsewhere: treat as real until shown otherwise.

5. Cause: read the failing record against the PR diff (`gh pr diff <N>`), then the code it exercises. For a reproduction, run the same step locally as CI does (`run-tests` skill, "Reproduce CI"), on a build of the same flavour (`dev` = asserts + fault injection, like the `clangd` preset).

## Report

Per failed test or job: verdict (real / flaky / infrastructure), the evidence (log lines with run and job ids, the other runs checked), the suspected cause with `file:line`, and the next step. Keep it short; no fixes unless the user asks.
