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

# CI workflows and images

## Testing CI workflows locally

Run or dry-run the GitHub Actions workflows locally with
[`act`](https://github.com/nektos/act), via `scripts/ci/act-local.sh`
(self-bootstrapping -- installs `act` on first use, shares the host Docker
socket so the in-container build steps work):

```bash
./scripts/ci/act-local.sh list                       # list workflows + jobs
./scripts/ci/act-local.sh validate build-manual.yml  # dry-run: parse + plan, no exec
./scripts/ci/act-local.sh run build-manual.yml -j perf   # actually run a job
./scripts/ci/act-local.sh classify                   # run the PR change-classifier alone
```

`validate` always works and catches YAML / job-graph errors before you push --
run it whenever you touch `.github/workflows/`. Full `run` needs the build image
and `/mnt/data` caches for heavy jobs; put fake secrets in `.secrets`
(gitignored) for workflows that reference them.

## CI images carry every dependency

CI never downloads or installs anything while it builds or tests. Toolchains, driver
packages, language runtimes and test fixtures come from images:

- `scripts/ci/build-ubuntu.Dockerfile` is the build and test image. It installs each driver's
  dependencies from the manifests in `tests/drivers/` (`requirements.txt`, `package-lock.json`,
  `composer.lock`, `go.sum`, `pom.xml`, `*.csproj`, `Cargo.lock`) and turns the package managers
  offline. Regenerate it with the `build-images` workflow whenever one of those changes.
- Service fixtures that need content baked in (models, extensions) get their own image, built by
  the same workflow (`tests/sqllogic/fixtures/ollama`).
- Runners never install a missing dependency or skip a missing toolchain; they fail and name what
  is missing. Locally, install it yourself once (e.g. `npm ci` in `tests/drivers/js`).
- The one exception is our own test tooling built from source (`third_party/sqllogictest-rs`): it
  is rebuilt every run so it can change in a PR, with its crates cached on the CI machine.

### Adding or changing a CI dependency

1. Put it where the image picks it up:
   - a system package or toolchain: the `apt-get install` list in `scripts/ci/build-ubuntu.Dockerfile`;
   - a driver's package: that driver's manifest in `tests/drivers/`;
   - a Python package for test data or fixtures (Spark, pyiceberg, boto3, ...): `scripts/ci/test-data-requirements.txt`;
   - a service with baked-in content: its own Dockerfile under `tests/sqllogic/fixtures/<name>/`. Its tag is derived from the directory's content (`tests/sqllogic/fixtures/image_tag.sh`), so runners pick up the new image without any edit, and `scripts/ci/build_images.sh` builds it.

   Pin versions, and never add a runtime fallback that installs the dependency when it is missing.
2. Build and try it locally: `docker buildx build --load -t serenedb-build-ubuntu:local --build-context drivers=../../tests/drivers -f build-ubuntu.Dockerfile .` in `scripts/ci`, then run the affected runner with `BUILD_IMAGE=serenedb-build-ubuntu:local` (e.g. `tests/sqllogic/run_in_docker.sh`).
3. Publish from your branch: run the `serenedb | create infra` workflow (`build-images.yml`) on it with `PUSH_IMAGES_2_REGISTRY=true`, `TAG_LATEST=false` and `TAG=<your branch>`. It pushes the build image as `serenedb/serenedb-build-ubuntu:<os>_clang-<version>_commit-<sha>` and as `:<your branch>` with `/` turned into `-`, plus the fixture images. `:latest`, which every other branch's CI uses, does not move.
4. Run CI on that image. The PR's "Trigger Jobs" uses `:latest`, so dispatch the build yourself with the image in `BUILD_CONFIG`:
   ```bash
   gh workflow run build-manual.yml --ref <branch> -f PR_NUMBER=<pr> -f PR_SHA=$(git rev-parse HEAD) \
     -f BUILD_CONFIG='{"BUILD_IMAGE":"serenedb/serenedb-build-ubuntu:<branch with - for />"}'
   ```
5. After the PR merges, run `serenedb | create infra` on main with `TAG_LATEST=true`; that moves `:latest` to the new image for everyone.
