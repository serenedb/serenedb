---
paths:
  - ".github/**"
  - "scripts/ci/**"
  - "tests/sqllogic/fixtures/**"
---

# CI

- Claude touches CI only when the user asks: no changes to workflows, `scripts/ci/`, CI images or fixtures, no runs, no reruns of failed or flaky jobs.
- A requested run ticks only **Trigger Jobs** in the PR's "CI Triggers" comment; the other boxes (all architectures, ignoring the diff, extra sanitizer suites, BigLake) only when asked.
- A red job is reproduced natively. The dev machines are set up like CI, so run the step's own runner (`scripts/ci/steps/` shows it) against a local build of the job's config; never `run_in_docker.sh` or containers to mimic CI.
- `act` checks a workflow change locally: `./scripts/ci/act-local.sh validate <workflow>.yml`.

## What a run covers

- Every push runs `auto | pre-commit` (every hook, every file).
- **Trigger Jobs** runs `serenedb | build & test` with `dev`, `asan`, `tsan` and `perf`, plus `validate pg` when `pg/` or `any/pg/` tests changed; `scripts/ci/classify-changes.sh origin/main` prints which gated suites a diff opens. `scripts/ci/steps/run-suites.sh` picks the suites per config.
- The nightly (`jobs-schedule.yml`) runs everything on `dev`, stress and the package tests, and one sanitizer per weekday; it is main's baseline.

## A red run

- `gh pr checks <N>` finds a PR's run; `gh run view <id> --log-failed` prints the failed jobs' logs, where each step ends with a `*_TESTS=PASSED|FAILED` line and a sqllogic failure shows the record, the expected and the actual output.
- `gh run view <id>` lists the artifacts (service logs, sanitizer reports); `gh run download <id> -n <name> -D <dir>` fetches one.

## CI images

CI installs nothing at build or test time: toolchains, driver packages and fixtures come from images, and a missing dependency fails the runner instead of being installed or skipped.

1. Add the dependency where the image picks it up: a system package in `scripts/ci/build-ubuntu.Dockerfile`, a driver package in that driver's manifest under `tests/drivers/`, a Python test-data package in `scripts/ci/test-data-requirements.txt`, a service with baked-in content as its own Dockerfile under `tests/sqllogic/fixtures/<name>/` (listed in `scripts/ci/build_images.sh`). Pin versions.
2. Build it locally (`docker buildx build --load -t serenedb-build-ubuntu:local --build-context drivers=../../tests/drivers -f build-ubuntu.Dockerfile .` in `scripts/ci`) and run the affected runner with `BUILD_IMAGE=serenedb-build-ubuntu:local`.
3. Publish from the branch with `serenedb | create infra` (`PUSH_IMAGES_2_REGISTRY=true`, `TAG_LATEST=false`, `TAG=<branch>`), test with `gh workflow run build-manual.yml --ref <branch> -f PR_NUMBER=<pr> -f PR_SHA=<sha> -f BUILD_CONFIG='{"BUILD_IMAGE":"serenedb/serenedb-build-ubuntu:<branch, / as ->"}'`, and after the merge run `create infra` on main with `TAG_LATEST=true`.
