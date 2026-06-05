# CI — full picture

How every CI run is shaped. One reusable build (`build-reusable.yml`) is called
N times by an orchestrator; each call is one parallel **job** defined by a single
build axis — `CONFIG` — plus a handful of run-flags.

```
            ┌──────────────────────── build-reusable.yml (one job) ───────────────────────┐
 CONFIG ───▶│ how it's built (and, for perf, what's left out)                              │
 flags  ───▶│ Derive flags ─▶ Build (docker, ninja) ─▶ run-suites ─▶ [coverage] ─▶ upload  │
            └─────────────────────────────────────────────────────────────────────────────┘
```

## The axis: CONFIG

`perf` is the one optimized build — and the only one that skips the unit-test
binaries and fault injection (so it can't run gtest/unittest/iresearch or
recovery). Every other config builds identically to `dev` (asserts + tests + FI)
and just layers on a sanitizer or coverage; `EXTRA` then decides what *runs*, not
what's built.

```
CONFIG     SDB_DEV  SDB_GTEST  FAULT_INJECTION  SANITIZERS          USE_COVERAGE
dev          On        On           On             —                   —
asan         On        On           On             Address             —
tsan         On        On           On             Thread              —
msan         On        On           On             MemoryWithOrigins   —
ubsan        On        On           On             Undefined           —
perf         Off       Off          Off            —                   —
coverage     On        On           On             —                   SourceBased
```

Run-flags (the orchestrator sets these per job):
- `IRESEARCH`, `EXTENSION` — build + run iresearch / unittest (extension) suites.
- `SQLITE`, `SQLSMITH` — run the sqlite subtree / sqlsmith fuzzing (+ the r driver).
- `RUN_EXTRA` — **sanitizer configs only**: run the full in-scope set instead of just ours + drivers.

## What each CONFIG runs (`run-suites.sh`)

```
suite                     dev   asan/tsan      asan/tsan      perf   coverage
                                (default)      (+RUN_EXTRA)
──────────────────────────────────────────────────────────────────────────────
gtest (serenedb-tests)     ✓       —              ✓            —        ✓
unittest / extension      if EXT   —          if EXT           —        ✓
iresearch (+ load)        if IRS   —          if IRS           —        ✓
sqllogic (ours)            ✓       ✓              ✓            ✓        ✓
recovery                   ✓       — (soon)       — (soon)     —        ✓
drivers                    ✓       ✓              ✓            ✓        ✓
sqlite                    if DUCK  —          if DUCK       if DUCK     ✓
sqlsmith (+ r driver)     if DUCK  —          if DUCK       if DUCK     ✓
```

- **perf** has no test binaries and no fault injection, so it never runs
  gtest/unittest/iresearch or recovery — just the serened smoke (ours + drivers)
  plus sqlite + sqlsmith when the diff touched duckdb.
- **asan/tsan** default to ours + drivers. Recovery is temporarily disabled under
  sanitizers (doesn't pass yet — will join the default soon). `RUN_EXTRA` widens
  them to the full in-scope set (everything dev runs, minus recovery for now).
- **dev** runs everything in scope, with asserts — including sqlite + sqlsmith on
  a duckdb diff.
- **coverage** = dev that also instruments; nightly runs it with every flag on.

`if EXT / IRS / DUCK` = gated by the diff: `EXTENSION` (duckdb/third_party),
`IRESEARCH` (iresearch paths), `SQLITE`/`SQLSMITH` (duckdb bump).

---

## 1) PR, triggered by diff (no marks)

`classify-changes.sh` diffs `origin/main...HEAD` and sets flags. The four build
jobs always spawn; the **diff only widens what they run**.

```
                         ┌─ classify (diff) ─┐
   PR push ──▶ trigger ──┤                   ├──▶  dev   (gtest + ours + recovery + drivers [+iresearch][+extension])
                         │  iresearch?        │     asan  (ours + drivers)
                         │  extension?        │     tsan  (ours + drivers)
                         │  duckdb?           │     perf  (ours + drivers)
                         │  pg?               │     validate-pg  (only if pg changed)
                         └───────────────────┘     report
```

Classifier rules → flags (first path match wins, top to bottom):

```
changed path                                   ─▶  flags set
───────────────────────────────────────────────────────────────────────────
third_party/duckdb*  (core or duckdb_* ext)    ─▶  duckdb=T (+ iresearch=T, extension=T)
libs/iresearch/** · tests/libs/iresearch/** ·  ─▶  iresearch=T
  third_party/iresearch.build/** · resources/tests/iresearch/**
third_party/*        (any other submodule)     ─▶  iresearch=T, extension=T
tests/sqllogic/pg/** · tests/sqllogic/any/pg/**─▶  pg=T  (+ changed pg-test list)
anything else (server/, libs/, scripts, …)     ─▶  (nothing extra)
```

So per diff (no marks — asan/tsan stay at their ours+drivers default):

```
diff kind          dev runs                                           perf runs                 validate-pg
─────────────────────────────────────────────────────────────────────────────────────────────────────────
core only          gtest + ours + recovery + drivers                  ours + drivers             —
iresearch          + iresearch                                        ours + drivers             —
duckdb (core/ext)  + iresearch + extension + sqlite + sqlsmith        ours + drivers + sqlite+sqlsmith  —
other third_party  + iresearch + extension                            ours + drivers             —
pg tests           gtest + ours + recovery + drivers                  ours + drivers             changed pg subset
```

`asan` and `tsan` run **ours + drivers** on every PR regardless of diff (that's
their default); `EXTRA_ASAN`/`EXTRA_TSAN` is what widens them (§2). sqlite +
sqlsmith land on **dev** (with asserts) and **perf** (fast) on a duckdb diff.

---

## 2) PR, with marks (additive opt-ins)

Marks come from the trigger checkboxes (`pr-build-trigger.yml`) →
`build-manual.yml` inputs. Each is purely additive on top of §1.

```
mark (checkbox)                  effect
──────────────────────────────────────────────────────────────────────────────
Ignore diff, run everything      classify skipped; all flags ON on every job;
  (EVERYTHING)                   RUN_EXTRA on for asan+tsan; validate-pg runs ALL pg
Extra ASAN                       asan runs the full in-scope set (RUN_EXTRA): gtest +
                                 iresearch? + extension? + sqlite? + sqlsmith?
Extra TSAN                       tsan runs the full in-scope set, same as above
All architectures (LOCATION=all) every job runs on linux-amd64 AND linux-arm64
```

Gating, precisely:

```
dev.IRESEARCH   = asan.IRESEARCH = tsan.IRESEARCH = EVERYTHING || classify.iresearch
dev.EXTENSION   = asan.EXTENSION = tsan.EXTENSION = EVERYTHING || classify.extension
dev.SQLITE      = dev.SQLSMITH   = EVERYTHING || classify.duckdb
perf.SQLITE     = perf.SQLSMITH  = EVERYTHING || classify.duckdb
asan.SQLITE/SQLSMITH = tsan.* = EVERYTHING || classify.duckdb   (only honored when RUN_EXTRA)
asan.RUN_EXTRA  = EVERYTHING || EXTRA_ASAN
tsan.RUN_EXTRA  = EVERYTHING || EXTRA_TSAN
validate-pg     = EVERYTHING || classify.pg     (no toggle -- purely diff-driven)
location matrix = (LOCATION == all) ? [amd64, arm64] : [LOCATION]
```

`EXTRA_ASAN/TSAN` only changes what an already-running job *runs* (it never adds a
job and never changes the build — asan/tsan always build like dev). It can't pull
in out-of-scope suites: on a core diff `RUN_EXTRA` adds only gtest; on a duckdb
diff it adds gtest + iresearch + extension + sqlite + sqlsmith.

Two diff kinds, each shown with EXTRA_ASAN **off** then **on**:

*core* diff:

```
            EXTRA_ASAN off                     EXTRA_ASAN on
dev    gtest + ours + recovery + drivers       (same)
asan   ours + drivers                           gtest + ours + drivers        ← +gtest (recovery still off)
tsan   ours + drivers                           ours + drivers
perf   ours + drivers                           ours + drivers
```

*duckdb* diff:

```
            EXTRA_ASAN off                                         EXTRA_ASAN on
dev    gtest+iresearch+extension+ours+recovery+drivers+sqlite+sqlsmith   (same)
asan   ours + drivers                                               gtest+iresearch+extension+ours+drivers+sqlite+sqlsmith  ← full set, sanitized
tsan   ours + drivers                                               ours + drivers   ← no EXTRA_TSAN
perf   ours + drivers + sqlite + sqlsmith                           ours + drivers + sqlite + sqlsmith
```

---

## 3) Nightly (`jobs-schedule.yml`, cron)

```
cron               jobs
────────────────────────────────────────────────────────────────────────────
0 5 * * *  (daily) coverage      CONFIG=coverage, all flags   (builds + runs EVERYTHING, instrumented)
                   everything    CONFIG=dev, all flags         (everything with asserts, no instrumentation)
                   release-check make-package (RTA=required)   (see §4)
0 1 * * 1  (Mon)   sanitizer     CONFIG=msan  } RUN_EXTRA + all flags
0 1 * * 2  (Tue)   sanitizer     CONFIG=asan  } (full in-scope set under the
0 1 * * 3  (Wed)   sanitizer     CONFIG=ubsan } sanitizer; recovery still off,
0 1 * * 4  (Thu)   sanitizer     CONFIG=tsan  } 10k fuzz queries)
                   ─▶ notify
```

`coverage` is the one instrumented build; it covers gtest + every suite in a
single profile (`relwithdebinfo-dev-cov`). `everything` is the same suite set on
the fast dev build.

---

## 4) Release (`make-package.yml`)

Independent of CONFIG — hardcodes the production build and packages it.

```
make-package (RTA = off | test | required)
  └─ Build   build.bash serened tzdata    BUILDMODE=Release, USE_IPO=On, static,
  │                                        SDB_DEV=Off, GTEST=Off, FI=Off
  └─ Package  → serenedb-*.tar.gz + serenedb*.deb
  └─ Deb RTA      install the .deb,  run drivers against the running service
  └─ Tarball RTA  unpack the tarball, run drivers against the running service
  └─ rta_ok output (deb + tarball RTA passed)  ─▶ release.yml gates push/release on it
```

Used by: nightly `release-check` (RTA=required, no upload) and `release.yml`
(manual releases, UPLOAD_ARTIFACTS=true). `test` = run RTA but don't block;
`required` = block. Under `required`, `release.yml` blocks the Docker push and the
GitHub release unless docker RTA passed **and** every packaged leg's `rta_ok` is
true (fails closed).

---

## Cache layout (`/mnt/data`, per machine)

```
/mnt/data/sdb-ci/                  ← umbrella (2777); future caches go here, no host re-run
        ├── timing/<engine>.<key>.tsv      sqllogic + recovery slowest-first cache
        └── gtest-parallel/<key>/          gtest-parallel slowest-first cache
/mnt/data/.ccache                  ← compiler cache       } large, dedicated dirs,
/mnt/data/.cargo-target-sqllogic   ← sqllogictest-rs build } kept out of the umbrella
/mnt/data/searchbench              ← iresearch-load corpus (S3 download)
/mnt/data/cores                    ← core dumps (core_pattern)
```

`<key>` = `cache-key.sh` from the build config, e.g. `relwithdebinfo-dev` (dev),
`relwithdebinfo-opt` (perf), `relwithdebinfo-dev-address` (asan),
`relwithdebinfo-dev-cov` (coverage) — so an optimized/sanitized/coverage build
never shares a timing cache with `dev`.

---

## One-line mental model

```
classify(diff)            ─▶ dev + asan + tsan + perf [+ validate-pg]
  dev                        gtest + iresearch? + extension? + ours + recovery + drivers + sqlite? + sqlsmith?
  asan / tsan (default)      ours + drivers                            (recovery soon)
  perf                       ours + drivers + sqlite? + sqlsmith?       (no tests, no FI)
+ EXTRA_ASAN / EXTRA_TSAN ─▶ that sanitizer runs the full in-scope set (what dev runs, minus recovery)
+ EVERYTHING              ─▶ all flags on, RUN_EXTRA on asan+tsan, all pg
+ LOCATION=all            ─▶ × {amd64, arm64}
nightly                   ─▶ coverage + everything(dev) + weekday-sanitizer + release-check
release                   ─▶ make-package: Release/IPO/static + deb&tarball RTA (required blocks push/release)
```

`?` = diff-gated: iresearch if `classify.iresearch`, extension if
`classify.extension`, sqlite/sqlsmith if `classify.duckdb`.
