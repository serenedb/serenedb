# DuckDB test suites

Driver for every DuckDB-level test suite we vendor: DuckDB core's own test tree
plus each extension's. They run through DuckDB's `unittest` binary against the
same serenedb build that statically links those extensions.

This is the DuckDB-level layer. The serened-level layer (pg-wire end-to-end) is
the regular sqllogic tree under [tests/sqllogic/](../sqllogic/).

## Prereqs

- A configured build dir (`build/`). The `unittest` binary is built by default;
  the driver will `ninja unittest` for you if needed. Configure with
  `-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` to skip it (the driver then refuses to run).
- `docker` for the `postgres_scanner` suite only, and only when `PGHOST` isn't
  already set: the runner then brings up `postgres:18.3` on a free port via
  [docker-compose.postgres.yml](docker-compose.postgres.yml). CI sets `PGHOST`,
  so it reuses the postgres already in the compose stack.

## Running

```bash
tests/duckdb/run.sh                    # every suite
tests/duckdb/run.sh --suite core       # just duckdb core
tests/duckdb/run.sh --suite avro,inet  # a subset
tests/duckdb/run.sh --suite interop    # DuckDB files against the official duckdb/duckdb image
tests/duckdb/run.sh --jobs 8           # 8 test files at a time (default: nproc)
tests/duckdb/run.sh --list             # suite names
```

The binary registers core's test tree plus each statically linked extension's
(`LOAD_TESTS`/`TEST_DIR` in `.github/config/extensions/<ext>.cmake`), so a suite
is selected purely by name filter. Those filters are also what makes `.test_slow`
run: those files carry Catch2's hidden `[.]` tag, which an unfiltered run skips.

Test files run in parallel the way `tests/sqllogic/run.sh --jobs` runs them:
`--jobs` (or `DUCKDB_JOBS`) is handed to `unittest`, which lists the matching
tests and keeps that many copies of itself running, one test file each,
`.test_slow` files first. Every copy has its own scratch dir
(`duckdb_unittest_tempdir/<pid>/`), `HOME` and spill directory (an in-memory
database spills under the scratch dir; a database file keeps its `<db>.tmp`).
The `postgres_scanner` suite is the exception: its files share the fixture's
`postgresscanner` database and reuse table names, so it runs as a second,
sequential `unittest` call.

The log lands in `out/test-results/duckdb.log` (override with `REPORTS_DIR`). In CI
the run comes from `048-ci-in-docker-run-duckdb-tests.bash` via
[docker-compose.duckdb.yml](../sqllogic/docker-compose.duckdb.yml), with the suite
list (`DUCKDB_SUITES`) derived from the diff by `scripts/ci/classify-changes.sh`:
duckdb itself or any dependency it shares selects every suite, while an
extension-only dependency selects just that extension.

## Branches

Every repo in the table below is a `serenedb/<name>` fork. Each update builds a new branch `vYYYY.MM.DD` in every fork from scratch, and serenedb's gitlinks point at those heads (`scripts/check_submodule_pointers.py` refuses a gitlink that no version branch contains). A version branch is never rewritten once created; the next update makes a new one.

The first-parent history of a version branch, oldest first:

1. upstream `main` at the update;
2. DuckDB's own patches for that extension, the files under `.github/patches/extensions/<ext>/` of the new duckdb, one `duckdb ext patch: <file>` commit each, in file order. They adapt the extension to DuckDB `main`'s API, and upstream applies them itself sooner or later: a patch whose content upstream already has (`git patch-id --stable`) is dropped, never re-applied;
3. real merges of the repo's release branches (`v2.0-cyanoptera`, then `v1.5-variegata`, where it has them), with every conflict resolved inside the merge commit. This tip is the **boundary**: `<boundary>..HEAD` is exactly our patchset;
4. our patchset: linear, one concern per commit, conventional-commit subjects, every commit formatted on its own;
5. duckdb only: one final `regen:` commit with everything the generators produce.

Upstream merges pull requests as merge commits (duckdb's PR titles are the merge subjects), so `git log --first-parent` reads as one entry per PR.

### Review PRs

An update is reviewed patchset by patchset. Every fork whose upstream is in the `duckdb` organization, or that is a DuckDB extension (`duckdb_markdown`), has one review PR from `mbkkt/update-duckdb` into `mbkkt/update-duckdb-base`. Other vendored forks (re2, OpenBLAS) have none. The review PRs are permanent: they are never merged or closed, and every update reuses them, so a patch's review history stays in one place.

1. Build the update on `mbkkt/update-duckdb` (the recipe below) and force-push it while the update is in review.
2. Force-push `mbkkt/update-duckdb-base` to the fork's boundary. The PR then shows exactly our patchset.
3. Link every review PR from the serenedb PR of the update. Its gitlinks point at the `mbkkt/update-duckdb` heads while it is in review, so `check-submodule-pointers` fails until the next step.
4. Before the serenedb PR merges, push each head as a new `vYYYY.MM.DD` branch, the date of the update, and move the gitlinks to it. A version branch is never force-pushed.

| fork | review PR |
|---|---|
| `third_party/duckdb` | https://github.com/serenedb/duckdb/pull/137 |
| `third_party/duckdb_httpfs` | https://github.com/serenedb/duckdb-httpfs/pull/5 |
| `third_party/duckdb_avro` | https://github.com/serenedb/duckdb-avro/pull/2 |
| `third_party/duckdb_iceberg` | https://github.com/serenedb/duckdb-iceberg/pull/26 |
| `third_party/duckdb_postgres` | https://github.com/serenedb/duckdb-postgres/pull/10 |
| `third_party/duckdb_inet` | https://github.com/serenedb/duckdb-inet/pull/2 |
| `third_party/duckdb_markdown` | https://github.com/serenedb/duckdb_markdown/pull/2 |
| `third_party/duckdb_azure` | https://github.com/serenedb/duckdb-azure/pull/2 |
| `third_party/duckdb_spatial` | https://github.com/serenedb/duckdb-spatial/pull/4 |
| `third_party/database-connector` | https://github.com/serenedb/database-connector/pull/3 |
| `third_party/avro` | https://github.com/serenedb/avro/pull/1 |

### The update of 2026-10-05

| submodule | upstream | `main` | merged | ext patches | boundary |
|---|---|---|---|---|---|
| `third_party/duckdb` | duckdb/duckdb | `e829ad1529` | `v2.0-cyanoptera` `36265ef34d`, `v1.5-variegata` `069cc9f9b5` | none | `0a82d6a5dc` |
| `third_party/duckdb_httpfs` | duckdb/duckdb-httpfs | `53b78e97a5` | `v1.5-variegata` `b26737e` | 0003-duplicate-secret-option-error | `954e6d913b` |
| `third_party/duckdb_avro` | duckdb/duckdb-avro | `859d56d` | `v1.5-variegata` `a54bd17` | none | `b108c9d5e3` |
| `third_party/duckdb_iceberg` | duckdb/duckdb-iceberg | `221db9bb4f` | `v1.5-variegata` `5dcf5070c5` | 0002-alter-info-column-path, 0002-logical-type-info-header | `72e8d691f2` |
| `third_party/duckdb_postgres` | duckdb/duckdb-postgres | `f9db66ec5a` | `v1.5-variegata` `1ddd672176` | 0002-builtin-parser | `e72abd6178` |
| `third_party/duckdb_inet` | duckdb/duckdb-inet | `61ce2d7245` | none | none | `61ce2d7245` |
| `third_party/duckdb_markdown` | teaguesterling/duckdb_markdown | `769f8c0e39` | none | none | `769f8c0e39` |
| `third_party/duckdb_azure` | duckdb/duckdb-azure | `951a0ab` | `v1.5-variegata` `73bd62b` | 0001-fix-azure-storage-cstdint | `5a0c59d34e` |
| `third_party/duckdb_spatial` | duckdb/duckdb-spatial | `2b072abd2a` | `v1.5-variegata` `9bfcf30e` | all 17: 0003 to 0013 in file order, then 0007-function-set-shared-ptr | `dae76d3f` |
| `third_party/database-connector` | duckdb/database-connector | `73d27b7` | `v1.5-variegata` `0a8505f` | none | `5ee92ce63e` |
| `third_party/avro` | apache/avro | `28cb08c15` | duckdb/duckdb-avro-c's 18 commits `35ff8b997..51ab9b2d3`, cherry-picked (its merges carry no resolutions) | none | `36e295afc` |

- inet has no release branches, and markdown's `v1.5-variegata` and spatial's `v2.0-cyanoptera` are contained in their `main`, so none of them is merged. DuckDB's inet patches target the v1.4 C++ layout while inet `main` is a C-API extension, so they are not applied; our port commit carries that adaptation.
- In spatial, `0007-function-set-shared-ptr` applies only after `0013`, and `0014-spatial-join-logical-cast` is not applied: `v1.5-variegata` already has its change.
- DuckDB writes its iceberg patches against the iceberg commit it pins, older than iceberg `main`. Iceberg `main` already has `0001-can-autoload-extension-database` and its own port of `0001-table-function-signature-options`, so neither is applied, and `0002-alter-info-column-path` goes in with `git apply --3way`.
- duckdb-avro-c's 1.11 release history is not merged: apache never merges it into `main`.

### Our patchset by area (duckdb core)

- **Parser and grammar:** PostgreSQL's surface in the core grammar (CREATE FUNCTION/PROCEDURE, roles and grants, FDW servers, text search dictionaries, subscriptions, REINDEX, LISTEN/NOTIFY, DISCARD, SET/RESET/SHOW scoping, TRUNCATE CASCADE/RESTART, pg_dump's sequence forms, opclass CREATE INDEX, VACUUM options, SELECT INTO, regex/SIMILAR TO/BETWEEN SYMMETRIC/JSON operators, tokenizer operators) and PostgreSQL literals (booleans, digit separators, ISO 8601 durations, `'{...}'` arrays).
- **Catalog:** roles, databases, foreign servers and tokenizers as catalog entries with stable oids and permissions; renames of every entry kind through one template; schema sets shared across a rename; `SqlCompatibility::POSTGRES`; dependency `owned_by`; catalog-log WAL records; triggers stored with their tables.
- **Commit path and storage:** the catalog log on upstream's group commit, the pre-checkpoint hook, two-phase commit with the server's stores, SereneDB storage versions in the low half of `StorageVersion`, and the refusal of SereneDB-only state in DuckDB files.
- **Compression and scans:** dict_fsst FSST+ layouts, filters evaluated inside bitpacking, ALP, RLE and dict_fsst, the compiled zonemap checker, `TableFilterPushdown`.
- **Functions and binding:** PostgreSQL functions and casts, implicit-cast ranking of string literals, the date_trunc family with monotone predicate ranges, bucket rewrites, aggregate input dedup.
- **Table functions and indexes:** point lookups for CSV, parquet, JSON, text and DuckDB files, `consume_top_n`, `set_scan_order`, external index hooks keyed by stable column id.
- **Client API and execution:** typed parameter hints, a caller-driven result collector and inline single-task driving for the pg-wire session, session-scoped `threads`, sink-lock reductions.
- **Common layer:** `duckdb::mutex` as `absl::Mutex`, absl hash containers, string_view APIs, simdutf validation, fmt formatting.
- **Dependencies:** fmt, fast_float, re2, zstd, brotli, lz4, snappy, zlib-ng (for miniz), jemalloc, abseil and ada come from serenedb's `third_party` instead of duckdb's bundled copies; httplib is gone and httpfs is curl-only.
- **ICU:** the icu extension carries the text layer (break iteration, normalization, casing, locales) and the Unicode property tables that iresearch, the server and re2 use instead of ICU itself.
- **Tests:** expectations for PostgreSQL rendering and our error texts.

### The update recipe

Run it in every fork, the parents first (duckdb, then the extensions, then serenedb's gitlinks):

```bash
cd third_party/<submodule>
git config rerere.enabled true
git fetch upstream                                  # by URL if there is no remote: the table's upstream column
git switch --no-track -C mbkkt/update-duckdb upstream/main
for p in <new duckdb>/.github/patches/extensions/<ext>/*.patch; do    # skip what upstream already has
  git apply "$p" && git add -A && git commit -m "duckdb ext patch: $(basename "$p" .patch)"
done
git merge upstream/v2.0-cyanoptera                  # duckdb only
git merge upstream/v1.5-variegata
git cherry-pick <previous boundary>..<previous vYYYY.MM.DD>   # duckdb: stop before its regen: commit
```

Then regenerate, format, build, run every suite here and the serenedb sqllogic, recovery and gtest runs, and push the head and the boundary to the review PR's branches. Once the review and CI are done, the head becomes `vYYYY.MM.DD` (created, never forced) and serenedb's gitlinks move to it, before the serenedb PR merges.

Rules for the rebuilt series:

- Resolve conflicts toward the final state; history is free. Fold a fix into the commit that introduced the problem (`fixup!` + autosquash), drop what upstream has (compare content with `git patch-id --stable`, never reachability: rewritten copies of upstream commits are not reachable from upstream), and keep a commit we still need even when upstream has a similar change, reduced to what upstream lacks.
- Nothing of ours is lost. Before pushing, account for every commit of the previous series in every fork: carried (same patch-id or subject), folded into another commit, or superseded by upstream, naming the upstream code that does the same. Then compare the two series as net diffs against their boundaries: a line ours added that is gone from the new tree needs one of those explanations, and passing tests are not one. Where upstream built the same thing as we did (WAL group commit, the curl client), keep upstream's design and port our improvements onto it (parallel syncs on network file systems, no body copies) instead of keeping only upstream's.
- No settings or pragmas that do nothing. A setting that upstream keeps only for DuckDB compatibility (a deprecated no-op, one "kept for legacy compatibility", a selector with one choice in our build) is deleted, together with its tests, goldens and docs, so `SET` reports it as unknown.
- Generated files never carry hand edits. In the merges and the cherry-picks, take upstream's side of a fully generated file; at the end, `scripts/duckdb_family.sh regen` runs DuckDB's generators in DuckDB's order on the last patch commit (`make generate-files`, then `scripts/capi_v2_regen.sh`) and commits everything they change as the one `regen:` commit. `regen --check` proves the commit is what the generators produce.
- Format each commit with `scripts/duckdb_family.sh format` before committing: DuckDB's own `scripts/format.py` with its pinned clang-format 11.0.1, black, cmake-format and typos. Before pushing, `scripts/duckdb_family.sh format --check --range <upstream main>..HEAD <fork>` proves every merge and commit of the series is formatted on its own. Upstream's own unformatted lines are left to a final `--all` pass.
- Never derive a boundary from authorship or from a local `main`: those refs are stale, and `git merge-base main HEAD` answers far too early.

## Suites and their configs

A suite with a `config/<suite>.json` runs with it as DuckDB's `--test-config`,
listing the tests we skip and why. Everything not on that list is a live regression gate on
the fork -- if a test starts failing, the fork broke it.

The skips fall into a few kinds, and the `reason` on every entry says which:

- **Deliberate SereneDB behaviour.** `LOAD`/`INSTALL` are unsupported (extensions
  are compiled into the server binary); the parser is one fixed grammar, with no
  parser, grammar or dialect extensions; `search_path` follows PostgreSQL; `//` on
  `DECIMAL` divides by IEEE 754 like `/`; httpfs is curl-only; spatial is built
  without GEOS, GDAL, PROJ and its persistent RTREE index. Tests for those features
  cannot pass and shouldn't.
- **Needs a live third-party endpoint.** A test that reaches a public bucket
  without a `require-env` guard fails intermittently on the endpoint's answer, not
  on anything the fork does.

The `cpp` suite is DuckDB's C++ test cases (`test/api`, `test/sql_export`, the
storage and appender tests, ...): every Catch2 case of the same `unittest`
binary that is not a sqllogic file, hidden (`[.]`) cases included. It has no
skip list: every case passes. CI runs it whenever it runs `core`.

`SDB_BUILD_DUCKDB_BENCHMARKS` (on by default) also builds DuckDB's
`benchmark_runner` (`$BUILD_DIR/third_party/duckdb/benchmark/benchmark_runner`)
with the same statically linked extensions. Measure only on `build_perf`, and
keep its artifacts out of the checkout the way
[benchmark/micro/parser/README.md](../../third_party/duckdb/benchmark/micro/parser/README.md)
shows: a scratch `--root-dir` with `benchmark` and `extension` symlinked from
`third_party/duckdb`.

## DuckDB file interop

The `interop` suite checks that a database file with a DuckDB storage version is a DuckDB file: vanilla DuckDB reads what SereneDB writes, and SereneDB reads what vanilla DuckDB writes. It is not a `unittest` suite. [interop/run.sh](interop/run.sh) drives `serened shell` and a vanilla `duckdb` copied out of the official `duckdb/duckdb` image (`docker create` + `docker cp` into `$BUILD_DIR/duckdb-<tag>/`; nothing is built, and the CI container gets the docker socket for it).

For each storage version in `INTEROP_STORAGE_VERSIONS` (`v1.0.0 v1.5.0`) and each writer, it writes a file as:

- `checkpoint`: [base.sql](interop/base.sql), checkpointed;
- `wal`: base.sql and [changes.sql](interop/changes.sql), all in the write-ahead log;
- `mixed`: base.sql checkpointed and changes.sql in the log, then the other engine replays and checkpoints it (`mixed-checkpointed-by-*`);
- `compression`: one table per method in `INTEROP_COMPRESSIONS`, forced with `force_compression`.

Each engine then opens its own copy of the file and runs [check.sql](interop/check.sql), the stage's `check_*.sql` and `use_*.sql`, a digest of every table and view, and `pragma_storage_info` of the compression tables. The two outputs must match byte for byte. The digest is the row count and the sum of `hash()` over the rows, because the engines print some values differently (a DOUBLE `0.0` is `0` in SereneDB). The copies are opened read-write with no checkpoint on shutdown: DuckDB 1.5 cannot open read-only a file whose log holds index data. `VANILLA_DUCKDB_TAG` (default `1.5.5`) picks the image, and `VANILLA_DUCKDB` a binary to use instead.

The suite runs with every other suite when duckdb or a dependency they share changes, and alone when only `interop/` does.

### Changing the DuckDB on-disk format

A DuckDB file must hold only what DuckDB reads, so SereneDB-only state (stored generated columns, the dict_fsst plus modes, owners and privileges, ...) never goes into one. Follow the rules in [CONTRIBUTING.md](../../CONTRIBUTING.md#duckdb-database-files), then:

- if DuckDB can store the change, add its DDL/DML to `base.sql` or `changes.sql` and a query to `check_base.sql` or `check_changes.sql`;
- if it cannot, add the refusal to `third_party/duckdb/test/sql/storage/serenedb_state_in_duckdb_file.test`;
- a new compression method or layout goes into `INTEROP_COMPRESSIONS`, when DuckDB has the method;
- run `tests/duckdb/run.sh --suite core,interop`.

## The postgres_scanner fixture

That suite is the only one with an external dependency, and three things about
its fixture are non-obvious:

- **It uses our `config/postgres_scanner.json`, not upstream's
  `attach_postgres.json`.** Upstream's config is for the inverse scenario
  (running DuckDB *core's* suite against postgres-as-storage); its
  `on_new_connection: USE pgdb;` breaks duckdb_postgres' own tests.
- **The locale is pinned to `C.UTF-8`** (here and in the CI compose file).
  DuckDB rewrites `col LIKE 'foo9%'` into `col >= 'foo9' AND col < 'foo:'`;
  under the postgres image's default `en_US.utf8`, `:` sorts before `9`, so the
  range excludes matching rows and `attach_like.test` fails. Upstream's CI runs
  a C-locale postgres and never hits this.
- **`tpch.lineitem` is synthetic (10k rows).** Upstream seeds tpch through
  DuckDB's dbgen; we don't ship the CLI, so the runner fakes the one table
  `attach_timeout_error.test` needs to trip its 1s statement_timeout.

## Traps

- **Never pass `--test-temp-dir`.** It also flips `DeleteTestPath` off, which
  turns the per-test `ClearTestDirectory()` into a no-op. Tests that do
  `load {TEST_DIR}/x.db` then inherit the previous test's database and fail with
  `Table with name ... already exists`. The default scratch dir
  (`duckdb_unittest_tempdir/<pid>/` under the test-dir) is gitignored in every
  vendored repo, so there is nothing to work around.
- **`unittest` is ~1.3GB.** That's why it's behind `SDB_BUILD_DUCKDB_UNITTESTS`
  and why CI only builds it when a `third_party/` diff puts these suites in scope.
- **Don't switch these runs to `-r junit`.** Catch2 v2 allows one reporter, so
  the junit one replaces the console output that carries every failure's query,
  expected value and actual value -- and it counts each skipped test as a
  failure, which turns a clean run into hundreds of phantom failures.
