---
paths:
  - ".github/workflows/**"
  - "scripts/ci/**"
  - "tests/sqllogic/fixtures/**"
  - "tests/drivers/*/requirements.txt"
  - "tests/drivers/*/package*.json"
  - "tests/drivers/*/composer.*"
  - "tests/drivers/*/go.*"
  - "tests/drivers/*/pom.xml"
  - "tests/drivers/*/*.csproj"
  - "tests/drivers/*/Cargo.*"
---

# CI

Claude leaves CI alone until the user asks: it changes nothing in CI (the workflows, `scripts/ci/`, the CI images, their fixtures or the dependency manifests they install from), starts no runs (Trigger Jobs, `gh workflow run`) and doesn't look at runs or their results. A change first passes locally ([testing.md](testing.md)); CI comes after that.

## What CI runs

- Every push to a PR runs `auto | pre-commit`: every hook on every file. A PR from a fork can't change `.github/workflows/` (`protect-workflows.yml` fails it).
- Ticking **Trigger Jobs** in the PR's "CI Triggers" comment runs `serenedb | build & test` (`build-manual.yml`) with the configs `dev`, `asan`, `tsan` and `perf`, plus `validate pg` when tests under `pg/` or `any/pg/` changed. `scripts/ci/classify-changes.sh origin/main` prints the gates a diff opens (iresearch, DuckDB suites, changed pg tests); a DuckDB `core` hit also opens the sqlite subtree and sqlsmith.
- `scripts/ci/steps/run-suites.sh` picks the suites per config:
  - `dev`: gtest, network, sqllogic, drivers and recovery, plus the gated iresearch tests and load test, DuckDB suites, sqlite subtree and sqlsmith.
  - `asan`, `tsan`: sqllogic and drivers; the "Extra ASAN" / "Extra TSAN" checkboxes add the rest except recovery.
  - `perf` (optimized, no asserts): sqllogic and drivers, plus the gated sqlite subtree and sqlsmith.
- The nightly (`jobs-schedule.yml`) runs everything on `dev` at 05:00 UTC, plus the stress soak, a macOS build and the package tests. Monday to Thursday at 01:00 UTC one sanitizer runs the full set except recovery, plus stress: msan, asan, ubsan, tsan. Stress runs only at night, apart from the `biglake-reindex-smoke` profile that the BigLake checkbox adds to a PR run.
- The CI steps run the suites in Docker with the service containers they need. The same wrappers run locally:

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

## Testing CI workflows locally

Run or dry-run the GitHub Actions workflows locally with
[`act`](https://github.com/nektos/act), via `scripts/ci/act-local.sh`:

```bash
./scripts/ci/act-local.sh list                       # list workflows + jobs
./scripts/ci/act-local.sh validate build-manual.yml  # dry-run: parse + plan, no exec
./scripts/ci/act-local.sh run build-manual.yml -j perf   # actually run a job
./scripts/ci/act-local.sh classify                   # run the PR change-classifier alone
```

Run `validate` whenever you touch `.github/workflows/`. Full `run` needs the build image
and `/mnt/data` caches for heavy jobs; put fake secrets in `.secrets`
(gitignored) for workflows that reference them.

## CI images carry every dependency

CI installs nothing while it builds or tests. Toolchains, driver packages,
language runtimes and test fixtures come from images:

- `scripts/ci/build-ubuntu.Dockerfile` is the build and test image. It installs each driver's
  dependencies from the manifests in `tests/drivers/` (`requirements.txt`, `package-lock.json`,
  `composer.lock`, `go.sum`, `pom.xml`, `*.csproj`, `Cargo.lock`). Regenerate it with the
  `build-images` workflow whenever one of those changes.
- Service fixtures that need content baked in (models, extensions) get their own image, built by
  the same workflow (`tests/sqllogic/fixtures/ollama`).
- Runners never install a missing dependency or skip a missing toolchain; they fail and name what
  is missing. Locally, install it yourself once (e.g. `npm ci` in `tests/drivers/js`).
- The one exception is our own test tooling built from source (`third_party/sqllogictest-rs`): it
  is rebuilt every run so it can change in a PR.

### Adding or changing a CI dependency

1. Put it where the image picks it up:
   - a system package or toolchain: the `apt-get install` list in `scripts/ci/build-ubuntu.Dockerfile`;
   - a driver's package: that driver's manifest in `tests/drivers/`;
   - a Python package for test data or fixtures (Spark, pyiceberg, boto3, ...): `scripts/ci/test-data-requirements.txt`;
   - a service with baked-in content: its own Dockerfile under `tests/sqllogic/fixtures/<name>/` with the service's version in an `ARG <NAME>_VERSION=` line, and `<name>` in the fixture loop of `scripts/ci/build_images.sh`. Runners tag the image with `tests/sqllogic/fixtures/image_tag.sh`, so a changed Dockerfile gets a new image without any edit. The kev image's tag is pinned in `tests/sqllogic/run.sh`.

   Pin versions, and never add a runtime fallback that installs the dependency when it is missing.
2. Build and try it locally: `docker buildx build --load -t serenedb-build-ubuntu:local --build-context drivers=../../tests/drivers -f build-ubuntu.Dockerfile .` in `scripts/ci`, then run the affected runner with `BUILD_IMAGE=serenedb-build-ubuntu:local` (e.g. `tests/sqllogic/run_in_docker.sh`).
3. Publish from your branch: run the `serenedb | create infra` workflow (`build-images.yml`) on it with `PUSH_IMAGES_2_REGISTRY=true`, `TAG_LATEST=false` and `TAG=<your branch>`.
4. Run CI on that image. The PR's "Trigger Jobs" uses `:latest`, so dispatch the build yourself with the image in `BUILD_CONFIG`:
   ```bash
   gh workflow run build-manual.yml --ref <branch> -f PR_NUMBER=<pr> -f PR_SHA=$(git rev-parse HEAD) \
     -f BUILD_CONFIG='{"BUILD_IMAGE":"serenedb/serenedb-build-ubuntu:<branch with - for />"}'
   ```
5. After the PR merges, run `serenedb | create infra` on main with `TAG_LATEST=true`.
