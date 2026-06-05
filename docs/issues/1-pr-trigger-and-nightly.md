# Issue 1 -- PR trigger, nightly, and CI/script refactor (agreed end state)

This is the **world after this PR merges**, not a step list. It's the single
source of truth for the work; the survey/roadmap in `../plan.md` is background.
Companion: [`test-classification.md`](test-classification.md) (validated
dependency graph + change categories).

Guiding principle: **catch the maximum number of issues in the PR itself with
the least effort** -- post-merge catch-up via nightly is the expensive case, so
the PR run does as much cheap, high-signal work as possible.

---

## Trigger

- Still **manually triggered** and still the **required merge gate** (fork-safe,
  no per-commit fleet waste). Unchanged.
- The change set is computed from the diff **at trigger time** (not recomputed
  on every push).
- **No checkboxes -> run the diff-derived set.** This is the normal path.
- **Checkboxes are additive only** -- they add to the diff-derived set, never
  remove. Restructured into clear groups:
  - **Architecture:** arm64 (default amd64)
  - **Scope:** "run everything" (full matrix regardless of diff, incl. sqlite +
    extensions + iresearch suites), "include slow under sanitizer"
  - **Instrumentation (extras):** sanitize unit tests; MSAN; UBSAN -- these last
    two are wired but disabled until fixed (see Sanitizers).
- The old `SearchBench` checkbox is gone.

## Change categories -> what runs

Categories (path-based, coarse -- see classification doc): `core` (default),
`iresearch`, `extension`, `duckdb`, plus "any other `third_party/` submodule".
Superset rules: `duckdb`/other-third_party => everything.

| Change | What the PR runs |
|---|---|
| **core** (server/, libs except iresearch, general) | gtest basics + connector (uninstrumented) · functional under **ASAN** · functional under **TSAN** (our sqllogic **excl. sqlite**, recovery, drivers -- **full, incl. our `test_slow`**) · **perf-build smoke** (drivers + sqllogic smoke on the local optimized binary) |
| **iresearch** (`libs/iresearch`) | core set **+ `iresearch-tests` + iresearch-load** |
| **extension** (a `duckdb_*` extension submodule) | core set **+ that extension's tests** |
| **`third_party/duckdb` or any other `third_party/` submodule** | **everything** (incl. iresearch suites + all extension tests + sqlite) |
| **scripts/ or cmake only** | no special detection -- runs the `core` set; contributor/reviewer ticks "run everything" if a build-system change warrants it |
| **never on a normal PR** | **sqlite sqllogic** (its own class, like extension unittests -> nightly or `duckdb` change only) |

## Parallel jobs per changeset

**Always 4 jobs (= 4 builds); the *contents* vary by changeset, not the count.**
The conditional **uninstrumented** suites -- sqlite, extension tests,
iresearch-tests, iresearch-load -- **ride inside the `gtest` (plain) build**
instead of spawning their own jobs. Rationale: (a) their time **under asan/tsan
isn't validated yet**, so keep them uninstrumented; (b) **same cmake settings**
-> duckdb/extensions are built **once** in the plain build, not rebuilt per extra
job; (c) the job count stays fixed at 4, which fits the 5-machine fleet.

The 4 jobs:

1. **`gtest`** -- plain build (`SDB_DEV=On` **asserts**, `GTEST=On`, `FI=Off`, no
   sanitizer; also builds `serened` / `unittest` / `iresearch-load-tests` when
   those suites are selected -- sqlite/extension just need the asserts). Runs
   basics + connector, plus -- by changeset, all uninstrumented:
   **+ iresearch-tests + iresearch-load** (iresearch/duckdb), **+ extension
   tests** (extension/duckdb), **+ sqlite sqllogic** (duckdb/other-third_party).
2. **`functional-asan`** -- functional build (`FI=On`) + ASAN: our sqllogic
   (no sqlite) + recovery + drivers.
3. **`functional-tsan`** -- same + TSAN.
4. **`perf-smoke`** -- perf build: drivers + sqllogic smoke on the local binary.

| Changeset | what the `gtest` job additionally runs | Jobs |
|---|---|---|
| **core** (server/general, libs!=iresearch) | -- | **4** |
| **iresearch** | iresearch-tests + iresearch-load | **4** |
| **extension** submodule | extension tests | **4** |
| **`third_party/duckdb`** or **any other `third_party/`** | iresearch-tests + iresearch-load + extension + sqlite | **4** |
| **scripts/ or cmake only** | -- | **4** |

Orthogonal: **+`validate-pg`** when pg test files changed; **+`report`**
(sequential, after). Multipliers: **arm64 / "all arch"** ≈ x2; **"sanitize
unit"** -> +N; **"run everything"** -> the duckdb-set contents (still 4 jobs).

**Fleet fit (5 amd64): 4 always fits -- no queueing in the normal case.** On a
duckdb bump the `gtest` job (gtest + sqlite + extension + iresearch) is the long
pole, but it's **one build** (duckdb compiled once) and uninstrumented.

## Sanitizers

- **ASAN + TSAN on functional by default** (cheap at this scope -- ~1-3 min each
  without sqlite; run as parallel jobs). Functional is **full incl. our
  `test_slow`** -- the old `test_slow` exclusion only existed for the slow sqlite
  tests, which are leaving the PR.
- **Unit (gtest) uninstrumented by default** (sanitized unit is slow -> opt-in).
- **UBSAN:** folds into the ASAN build; **wired but OFF** (broken now) -- a
  one-line toggle to enable later.
- **MSAN:** also a future PR sanitizer; **wired but OFF** (broken now) -- easy
  enable later. Until then it's nightly/opt-in.
- Making the sanitizer set **trivially configurable** is a design requirement.
- **sqlsmith** (and similar) run on PR like other tests; if observed too slow
  while building this PR, dropped from the PR before merge -- an empirical
  judgment call, not pre-designed.

## Build option sets (CI configures via script flags, not `cmake --preset`)

CI sets these via script flags (not `cmake --preset`). gtest and functional are
**already two separate builds today** (different `GTEST`/`FAULT_INJECTION`). PR
changes: add the sanitizer dimension to functional, and **move sqlite + extension
execution onto the plain `gtest` build**. That fold is free -- sqlite/extension
need **asserts (`SDB_DEV=On`)**, **don't need `SDB_FAULT_INJECTION`**, and are
**unaffected by `SDB_GTEST`**, which is exactly the gtest build's config.

| Set | Build flags | Used by |
|---|---|---|
| **gtest / plain** | RelWithDebInfo, `SDB_DEV=On` (**asserts on**), `SDB_GTEST=On`, **`SDB_FAULT_INJECTION=Off`**, `USE_IPO=Off`, JE, no sanitizer | **gtest** (basics + connector) -- and in the **same build**: **sqlite / extension / iresearch-tests / iresearch-load** when their category is hit (builds `serened`/`unittest`/`iresearch-load-tests` as needed). sqlite/extension just need asserts; `GTEST`/`FI` don't matter to them. |
| **functional** | RelWithDebInfo, `SDB_DEV=On`, `SDB_GTEST=Off`, **`SDB_FAULT_INJECTION=On`**, `USE_IPO=Off`, JE; **+ ASAN / + TSAN** | PR **functional** -- our sqllogic (no sqlite) + recovery + drivers, two parallel sanitizer jobs. **sqlite + extension are NOT here** (moved to the plain build; their asan/tsan timing is unvalidated). |
| **perf** | RelWithDebInfo, **no-LTO** (`USE_IPO=Off`), `SDB_DEV/GTEST/FAULT_INJECTION=Off` | PR perf-build smoke (local binary); later reused for perf tests/benchmarks/profiling (issue 3) |
| **release** | Release, **LTO** (`USE_IPO=On`), static, `SDB_DEV/GTEST/FAULT_INJECTION=Off` | release job (packaged artifacts) |

## Perf-build smoke (replaces `compile-check`)

- Build the optimized **perf** option set, then run a smoke on **that local
  binary**: drivers + our sqllogic smoke. **No recovery, no gtest** (the perf
  set has fault-injection + gtest off -- driver/sqllogic are knob-independent).
- Purpose: catch bugs that only reproduce on fast/optimized runs (races/UB that
  debug+sanitizer timing masks). One run.

## Scheduling (runner improvement, benefits any run)

- **sqllogic + recovery run slowest-first** off a **persisted per-test timing
  cache** (keyed by path + engine/instrumentation), like gtest-parallel. We
  already emit `Finished in N ms`.
- **Recovery** additionally moves from **static round-robin -> a dynamic work
  queue** (free workers pull next), killing idle-tail imbalance. Optional
  weighting for heavy stress/`_huge` tests so they don't oversubscribe CPU/IO.

## Rename

- **`searchbench` -> `iresearch-load`** everywhere it's a test (target
  `iresearch-load-tests`, gtest `LoadTest*` / `load_test.cpp`,
  workflow/script names, env). It's a search-path load test, not a benchmark.
- **Leave external infra names** as-is (GitHub secrets `SEARCHBENCH_S3_*`, S3
  bucket/object keys) -- those need ops, not a find-replace.

## CI / scripts refactor

`tests/` becomes **test cases/data only** (`.test` files, gtest `.cpp`,
fixtures). **All executable orchestration moves under `scripts/`**: `.sh` only
(drop the `.bash` arango legacy), phase-grouped, **no numeric / `ci-` /
`in-docker` prefixes** (execution order belongs to the workflow). Compose files
co-located with the script that drives them.

```
scripts/
  build/     configure.sh, targets.sh, docker-env.sh        # asan/tsan/dev/perf/release flag sets
  test/      run.sh, gtest.sh, sqllogic.sh, recovery.sh, drivers.sh + drivers.compose.yml,
             validate-pg.sh + validate-pg.compose.yml, extensions.sh (+ fixture compose),
             iresearch-load.sh, iresearch-load-corpus.sh           # real runners, dev + CI both call these
  package/   build.sh, rta-deb.sh + .compose.yml, rta-docker.sh + .compose.yml, rta-tarball.sh + .compose.yml
  ci/        thin orchestration/env that calls scripts/test/* (fold in if it's just glue)
  release.sh, coverage.sh
  (existing checks/ + codegen stay; send_notification -> .sh)
```

- Dev commands change (e.g. `./scripts/test/sqllogic.sh ...`) -- **CONTRIBUTING.md
  updated** with the new map; full move now while the repo is small.

## Output locations

- **Everything generated goes under `out/`; repo root stays clean.**
  `out/test-results/` (junit xml), `out/logs/`, `out/coverage/`, `out/perf/`.
- **Idempotent:** every script `mkdir -p`s what it needs; works with or without
  `out/` (or any subdir) present; `rm -rf out` is always safe and self-heals.
- Sweep current strays in: `docker.env`, `drivers-tests.log`,
  `perf.data`/`.old`/`.svg`, root `logs/`, local datadirs. One `.gitignore`
  entry for `out/`. CI overrides the path only for artifact upload.

## Nightly

- The **lossless catch-all**: full sqllogic **incl. sqlite + `.test_slow`**, full
  recovery, **all** extension tests, **all** sanitizers (incl. TSAN/MSAN/UBSAN
  once fixed), iresearch-load.
- **Parallel via separate CI jobs** (job-level fan-out, as `jobs-schedule.yml`
  already does) -- sqlite / slow / extensions / each sanitizer / iresearch-load are
  their own parallel jobs, not serialized. (Distinct from the in-runner
  slowest-first scheduling above.)
- **Coverage made viewable** -- a link, not a buried artifact (PR-level coverage
  is the separate issue 2).

## Release job

- Build packages for **amd64 + arm64** with the **release** option set
  (Release + LTO + static).
- **RTA: run the smoke 3x, once each against the installed docker / deb /
  tarball artifact** (not the local binary), per arch. The smoke **includes
  driver tests** (knob-independent -- no fault-injection/gtest needed) plus the
  sqllogic smoke -- same *content* as the perf-build smoke, different target.
- Plus the full battery **minus sqlite**, then push images + GitHub release.

## Dropped -- ignored throughout

`fuerte` (built-but-unrun today), `vpack`, `rocksdb` -- being removed soon; no
work spent on them. See memory `project_components_being_dropped`.
