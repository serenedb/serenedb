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

## Fork points

Every vendored duckdb repo carries a linear SereneDB patchset on top of one upstream commit. That commit is the **fork point**: `<fork>..HEAD` is exactly our patchset, and it is what a rebase onto a newer upstream replays.

| submodule | fork point | upstream | fork-point subject |
|---|---|---|---|
| `third_party/duckdb` | `958bef9a6b` | duckdb/duckdb | Make `QualifiedName` immutable and instead override the entire qualified name when modified (#23490) |
| `third_party/duckdb_httpfs` | `2a6481d0bf` | duckdb/duckdb-httpfs | Merge pull request #344 from OlexG/fix/s3-url-style-virtual-alias |
| `third_party/duckdb_avro` | `b1b9612069` | duckdb/duckdb-avro | Merge pull request #111 from duckdb/avro_get_metadata_callback |
| `third_party/duckdb_iceberg` | `18ec6a364e` | duckdb/duckdb-iceberg | Merge pull request #1127 from NiclasHaderer/nh/fix-flake |
| `third_party/duckdb_postgres` | `4f0c4cb16e` | duckdb/duckdb-postgres | Merge pull request #492 from MBkkt/patch-1 |
| `third_party/duckdb_inet` | `bf675673d9` | duckdb/duckdb-inet | Merge pull request #24 from Maxxen/main |
| `third_party/duckdb_markdown` | `340c0cd597` | teaguesterling/duckdb_markdown | fix: unwrap nested table bodies in legacy Pandoc table rows |
| `third_party/duckdb_azure` | `ed1c69cf05` | duckdb/duckdb-azure | pass etags through for improved caching (#182) |
| `third_party/duckdb_spatial` | `2b072abd2a` | duckdb/duckdb-spatial | Merge pull request #851 from Maxxen/main-dev |
| `third_party/database-connector` | `7777dac887` | duckdb/database-connector | Apply Identifier patch |

Six of the ten fork points are upstream *merge* commits, because those repos merge PRs; duckdb's, duckdb_azure's, duckdb_markdown's and database-connector's are plain commits (duckdb squash-merges upstream). So "the last merge commit before our first commit" is a good first guess but not a rule.

Do **not** infer the boundary from authorship: in `database-connector` the last five upstream commits and our first are hard to tell apart from the log alone. Do not use a local `main` either -- those refs are stale and `git merge-base main HEAD` answers far too early.

And if the local history was ever rewritten (rebase, replay), SHA reachability lies: rewritten copies of upstream commits are no longer reachable from upstream, so the check reports them as ours. Compare **content** instead -- `git show <c> | git patch-id --stable` against upstream's recent commits. That is how `database-connector`'s fork point was found to be `7777dac887` and not the much earlier `43e79061e5`.

To re-derive or re-validate one, fetch upstream by URL (no permanent remote needed) and check three things: the fork commit is reachable from upstream, the commit right after it is not, and the count of commits missing from upstream equals the patchset size.

```bash
cd third_party/<submodule>
git fetch https://github.com/duckdb/<upstream-repo>.git main   # parent repo: gh api repos/serenedb/<name> --jq .parent.full_name
first_ours=$(git rev-list HEAD --not FETCH_HEAD | tail -1)
fork=$(git rev-parse "$first_ours^")
git merge-base --is-ancestor "$fork" FETCH_HEAD          # fork is upstream's
! git merge-base --is-ancestor "$first_ours" FETCH_HEAD  # ours is not
test "$(git rev-list --count "$fork..HEAD")" = "$(git rev-list --count HEAD --not FETCH_HEAD)"
```

### Commits upstream has since applied itself

`duckdb_httpfs`'s `duckdb ext patch: 0002` .. `0006` have exact content matches upstream (`606018bb`, `5bf8ff0a`, `03bf787e`, `9c9f073b`, `7b436a4c`), applied there after our fork point. They are not wrong today -- our base predates them, so the patches are still needed -- but the next upstream bump makes them no-ops, and they should be dropped rather than re-applied. duckdb core's cherry-picked upstream fixes behave the same way.

Find them with the same patch-id comparison, over `<fork>..<upstream tip>`.

## Suites and their configs

Each suite runs with `config/<suite>.json`, a DuckDB `--test-config` listing the
tests we skip and why. Everything not on that list is a live regression gate on
the fork -- if a test starts failing, the fork broke it.

The skips fall into a few kinds, and the `reason` on every entry says which:

- **Deliberate SereneDB behaviour.** `LOAD`/`INSTALL` are unsupported (extensions
  are compiled into the server binary), and `httpfs_client_implementation` only
  accepts `curl`/`default` because httplib was dropped. Tests for those features
  cannot pass and shouldn't.
- **Needs the public internet or cloud credentials.** Some tests reach live S3 /
  Azure / HuggingFace endpoints without a `require-env` guard, so they fail in a
  sandboxed runner rather than skipping themselves.
- **Upstream expectation predates our fork.** The vendored extension pins are
  older than `third_party/duckdb`, so a few tests assert error-message wording
  that core has since changed.

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
