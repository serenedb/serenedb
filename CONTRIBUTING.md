# Contributing to SereneDB

Thanks for your interest in contributing! SereneDB is an early-stage project, and we appreciate every contribution.

## Getting Started

### Fork the repository

<p align="center">
  <img src="https://github.com/user-attachments/assets/82327bc5-331e-49af-8b5c-717f563b67d4" width="800" style="border-radius: 8px;">
</p>

### Clone the repository

```bash
git clone https://github.com/serenedb/serenedb.git
cd serenedb
git submodule update --init --depth 1 --jobs=$(nproc)
```

> **Using SSH?** Submodule URLs are HTTPS for easy cloning. If you prefer SSH, add this to your global git config:
> ```bash
> git config --global url."git@github.com:".insteadOf "https://github.com/"
> ```

### Build prerequisites

- Compiler: clang-21 / clang++-21
- Build system: Ninja
- CMake >= 3.26

We support a single toolchain and only upgrade forward.

### Build

```bash
cmake --preset lldb -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21
cd build/
ninja
```

Additional build presets are defined in `CMakePresets.json`:
- `lldb` -- Debug build (`build/`), works with lldb, gdb, or any debugger
- `clangd` -- RelWithDebInfo build (`build_clangd/`), works well with the clangd language server in VSCode
- `bench` -- Release build (`build_bench/`), static linking, production-like performance

### Debug info and disk use

Every binary links most of the server statically, so debug info dominates its size. Two settings keep a build directory small:

- **Split DWARF** (`SDB_SPLIT_DWARF`, on by default except on macOS, off in CI): debug info is written once, into a `.dwo` file beside each object, and the binaries only point at those files. lldb, gdb, perf, `llvm-symbolizer` and `addr2line` follow the pointers on their own, as long as the build directory is there. A binary copied out of it keeps its symbols and line numbers but loses inlined frames, variables and types; to keep those too, pack the debug info next to the copy:

  ```bash
  llvm-dwp -e build/bin/serened -o /path/to/copy/serened.dwp
  ```

  lldb and gdb pick up `<binary>.dwp` beside the binary automatically.
- **Thin archives**: static libraries (except on macOS) only reference their objects instead of holding copies, so they cannot be moved or installed without the build directory -- nothing in the build does that.

Tools and benchmarks share binaries instead of each linking their own: `serenedb-bench-micro <bench> [args...]` runs one micro benchmark (see [Performance](#performance)), and `iresearch-examples <example>` runs one of the iresearch examples. Both print what they offer when run without arguments.

### The embedded documentation index

`docs/` is compiled into the binary together with a prebuilt search index of it. The read-only `sdb_docs` functions and the shell's `.docs` read that image straight from the binary, so the server indexes nothing at startup and leaves nothing in the datadir.

The index cannot be produced from the sources the way the documentation text is, because building it needs the indexer that lives in the server being built. So `serened` builds it itself, right after it is linked:

1. `scripts/generate_docs.py --corpus` writes the documentation text to a file in the build directory.
2. `serened` is linked with an empty `.sdb_docs` section for the index (`server/docs/docs_index_image.cpp`); `server/docs/docs_index.ld` places it after `.bss`, alone in the last loadable segment.
3. `serened <datadir> --build_docs_index=<out> --docs_corpus=<file>` boots it on a throwaway datadir, indexes the documentation and the catalog of the objects it documents, and writes them to `<out>/docs` and `<out>/objects`, each with a layout file naming its column and field ids. It then exits before any listener is started.
4. `scripts/embed_docs_index.py` writes both directories into `.sdb_docs` of the binary that built them and grows the section and its segment to exactly their size. Nothing is loaded after that segment, so only the non-loaded sections behind it in the file move.

macOS has no linker scripts, so there `serened` reserves a fixed 4 MiB region instead and step 4 fills it in place. If the index outgrows it, the macOS build fails and says so; raise `kCapacity` in `docs_index_image.cpp`.

`serenedb-tests` is linked and embedded the same way, so the documentation tests run against what `serened` ships.

Steps 3 and 4 run every time `serened` is linked, which includes every change to `docs/`. So the image always matches the server it lives in, and there is nothing to keep in sync by hand. To skip it, configure with `-DSDB_EMBEDDED_DOCS=OFF`: that build carries no documentation, so `.docs` and `sdb_docs` have nothing to read.

### PostgreSQL catalog code

Everything SereneDB copies from PostgreSQL's system catalogs is generated, never written by hand: built-in types and the functions they reference, the oids of catalog tables, schemas, access methods and languages, each catalog table's columns (type, NOT NULL, default and lookup key), `pg_catalog` and `information_schema` view definitions, the rows of the `information_schema.sql_*` tables, setting descriptions for `pg_settings`, the keyword list `quote_ident` uses, and the signatures of stub set-returning functions. The output is `server/pg/catalog/**/*.gen.inc`.

`scripts/generate_pg_catalog.py` reads a running PostgreSQL and its source tree of the same version:

```bash
docker run -d --name pg18 -e POSTGRES_HOST_AUTH_METHOD=trust -p 55433:5432 postgres:18.3
git clone --depth 1 -b REL_18_3 https://github.com/postgres/postgres pg-src
python3 scripts/generate_pg_catalog.py --pg 127.0.0.1:55433 --pg-src pg-src
```

What SereneDB decides on its own is kept apart from the generated output:

- `scripts/pg_catalog/config.py` lists the views SereneDB implements natively as tables, SereneDB's own `sdb_*` tables, how each PostgreSQL column type is stored, the row estimates the planner sees, the settings it exposes, and the few PostgreSQL types it substitutes in stub functions.
- `server/pg/catalog/views/overrides/<schema>.<view>.sql` holds a view body SereneDB rewrites; the generator uses it in place of PostgreSQL's.
- `server/pg/catalog/tables/*.cpp` produce the rows of each catalog and set SereneDB's constant column values. A catalog without a file there exists with no rows.

To move to a new PostgreSQL release, rerun the generator against that release and fix what no longer compiles. Column names are checked at compile time, and a debug build checks every column's storage type at startup.

### Launch

```bash
./build/bin/serened ./build_data --listen='postgres://0.0.0.0:7890'
```

Connect via psql: `psql -h localhost -p 7890 -U postgres`

### Test

The test tree is split by what runs the test and what it covers:

- `tests/sqllogic/any/...` -- sqllogic against any engine (PG and SereneDB); use for behaviour we expect from both.
- `tests/sqllogic/sdb/...` -- sqllogic against SereneDB only (SereneDB-specific syntax / extensions).
- `tests/sqllogic/pg/...` -- sqllogic against Postgres only (used to validate the spec).
- `tests/sqllogic/recovery/...` -- sqllogic with crash injection (`SET sdb_faults = '...'`) plus a restart; each test runs against a fresh serened + datadir.
- `tests/server/<area>/...`, `tests/iresearch/...` -- gtest unit tests; use for isolated C++ logic where a sqllogic test would be awkward (library classes / pure functions / hard-to-reproduce bugs).
- `tests/bench/micro/...` -- microbenchmarks for performance claims.
- `tests/duckdb/` -- driver for the **DuckDB-level** suites: DuckDB core's own test tree and each vendored extension's, via DuckDB's `unittest` binary. Built only when configured with `-DSDB_BUILD_DUCKDB_UNITTESTS=ON`.

When a change needs a test:

- Bug fix: always, unless you can argue the bug is uncoverable. Crash / recovery bugs go under `tests/sqllogic/recovery/`.
- New feature / behaviour change: sqllogic test in the right subtree above. Add a unit test too if there's isolated C++ logic worth pinning.
- CMake-only changes: rely on CI.
- Doc-only changes live in a separate repo and don't apply here.

Races are testable in sqllogic, so a concurrency bug still gets a test:

- `connection <name>` before a record runs it on that named session.
- `statement async ok`, `statement async error`, `query async` and `system async ok` run the record in the background. On a named connection, async records keep that connection's order and overlap other connections. Without a connection, each async record gets a fresh one.
- `wait` blocks until every background record has finished; a later synchronous record on a busy named connection waits for that connection only.
- Records are dispatched in file order, and dispatching a record on a busy named connection first waits for that connection. To keep a step in flight while others run, put the steps that should overlap on different connections right after it, then `wait`.
- `control max-async-connections N` caps parallel background records (default 10); `control always-async on` makes every record async.
- There are no loops: widen a race window by repeating records.
- `tests/sqllogic/pg/simple/async.test` is a minimal example.

```bash
# All sqllogic tests
./tests/sqllogic/run.sh --single-port 7890 --debug true

# Specific tests
./tests/sqllogic/run.sh --single-port 7890 --test 'tests/sqllogic/any/pg/simple/*.test' --debug true

# Recovery tests (auto-restarts serened on injected crashes; needs build/bin/serened)
./tests/sqllogic/run_recovery_tests.sh --runner ../../third_party/sqllogictest-rs
```

C++ unit tests:

```bash
./build/bin/iresearch-tests "--gtest_filter=*PhraseFilterTestCase*"
./build/bin/serenedb-tests "--gtest_filter=*VPackLoadInspectorTest*"
./build/bin/serenedb-tests "--gtest_filter=*DataSourceWithSearchTest*"
```

### Testing CI workflows locally

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

### CI images carry every dependency

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

#### Adding or changing a CI dependency

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

### Running DuckDB's own test suites

DuckDB core and each vendored extension ship sqllogic-style test suites under
`third_party/duckdb/test/` and `third_party/duckdb_<name>/test/`. They run
through DuckDB's `unittest` binary, which is built by default (opt out with
`-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` if you want to skip its ~1.3GB output).

```bash
./tests/duckdb/run.sh                    # every suite
./tests/duckdb/run.sh --suite core       # just duckdb core
./tests/duckdb/run.sh --suite interop    # DuckDB files against the official duckdb/duckdb image
./tests/duckdb/run.sh --list             # suite names
```

Each suite carries a checked-in skip list (`tests/duckdb/config/<suite>.json`)
naming the SereneDB divergences that can't pass, with a reason per entry;
everything else is a regression gate on the fork. See
[tests/duckdb/README.md](tests/duckdb/README.md) before adding to it.

The serened-level postgres_scanner tests
(`tests/sqllogic/sdb/pg/duckdb_postgres/*_pgscan.test_slow`) ride the regular
sqllogic runner -- the `_pgscan.` filename suffix triggers
`launch_postgres()` in `tests/sqllogic/run.sh` automatically.

## Third-party dependencies

Dependencies are git submodules under `third_party/`, usually forks under `github.com/serenedb`, so build fixes can go into the fork. `third_party/CMakeLists.txt` builds them from source (`sdb_update_module` + `add_subdirectory`), so every file gets the same compiler, flags and standard library as our own code. Configure a dependency there with `set(<OPTION> <value> CACHE <type> "" FORCE)` before its `add_subdirectory`.

- **One ISA baseline.** `cmake/OptimizeForArchitecture.cmake` puts the baseline into `CMAKE_C_FLAGS` and `CMAKE_CXX_FLAGS`: Haswell features on amd64, `-march=armv8-a+crc+crypto` on arm64. A dependency must not add its own `-march`, `-mcpu`, `-mtune` or `-mno-*` to the whole library: a later `-march` replaces ours, and a lower one drops below it. Turn such options off (`ZXC_NATIVE_ARCH OFF`, zlib-ng's `WITH_NATIVE_INSTRUCTIONS OFF`, `DUCKDB_OPTIMIZATION_PROFILE NONE`) or fix the fork.
- **Runtime dispatch above it.** If a library can pick faster code for the CPU it runs on (AVX-512, SVE), enable that instead of building the whole library for one fixed level: OpenBLAS `DYNAMIC_ARCH` with a `DYNAMIC_LIST`, faiss `FAISS_OPT_LEVEL=dd`, zlib-ng `WITH_RUNTIME_CPU_DETECTION`. Flags above the baseline belong only on the kernels that the library reaches after a CPU check. For a header-only library that picks its backend at compile time, call the backends behind our own check, as `iresearch/analysis/text/sz/stringzilla.hpp` does for StringZilla.
- **One C++ standard.** C++ builds with `CMAKE_CXX_STANDARD` (`-std=c++26`); the LLVM runtimes are the only exception. Pass it through the library's own variable when it has one (`SIMDUTF_CXX_STANDARD ${CMAKE_CXX_STANDARD}`). Otherwise remove the library's own `CMAKE_CXX_STANDARD` in the fork, as was done for ada. A dependency must not set `CMAKE_BUILD_TYPE` either.
- **Check `compile_commands.json`** after adding or updating a dependency. Every file should carry the baseline and `-std=c++26`; only the dispatched kernels may go above the baseline:

  ```bash
  jq -r '.[] | select(.file | contains("third_party/<name>/")) | .command' build/compile_commands.json \
    | grep -oE -- '-std=[^ ]+|-m(arch|cpu|tune)=[^ ]+|-mno-[^ ]+' | grep -v frame-pointer | sort | uniq -c
  ```

## Branching, commits, PRs

- **Branch from `main`**, one focused change per PR.
- **Branch name:** `<author>/<topic>` (e.g. `mbkkt/fix-view-indexes-recovery`). Topic is free-form.
- **Conventional commit prefix** in the PR title: `feat:`, `fix:`, `perf:` (most common), or one of `refactor:`, `chore:`, `docs:`, `test:`, `ci:`, `build:`, `style:`, `misc:`. Don't invent new ones -- if none fit, ask.
- **Squash-merge:** the PR title is the final commit subject and the PR description is the body. Branch-internal commit messages are discarded, so they can be anything.
  - Exception: if your branch has exactly one commit and you let GitHub open the PR for you, GitHub will pre-fill the PR title and description from that commit -- so in that case keep the commit message PR-ready.
- **Pre-commit hooks** run as a PR check. You don't have to install them locally; if you want to check before pushing, run `pre-commit run --all-files`.
- **CI must pass** and one maintainer must approve before merge.

## Documentation

A user-visible change lands with its documentation in the same PR. New SQL syntax, functions, settings, CLI flags, catalog objects, wire-protocol behavior -- anything a user can reach -- is undocumented until `docs/` says so, and reviewers ask for the page before approving a `feat:`. Behavior that changes gets its existing page updated in the same PR.

- **`docs/` is the source of truth**, and two consumers read it: the website, which renders it as its Docusaurus tree, and the server itself, which embeds it as the `sdb_docs` schema when built with `SDB_EMBEDDED_DOCS`.
- **Frontmatter:** every page needs `title` and `split`, where `split` is `page` (index the whole page as one unit) or `headings` (index each heading separately -- use it for long reference pages). `scripts/generate_docs.py` rejects anything else, which fails the build.
- **New folder:** add a `_category_.json` beside the pages with `label` and `position`, or the sidebar falls back to the folder name.

### Documenting with runnable examples

SQL examples are backed by sqllogic tests, so an example that stops working fails CI instead of shipping.

- Put the example in a test under `tests/sqllogic/sdb/pg/site_docs/` (or `tests/sqllogic/recovery/site_docs/`) and mark its block with a `# DOCS_TEST: <name>` comment.
- Reference it from the page as `<SqlLogicTest id="<file>/<name>" />`, where `<file>` is the test's path relative to `site_docs` with the extension dropped. So `tests/sqllogic/sdb/pg/site_docs/quick-start.test` plus `# DOCS_TEST: example_003` gives `id="quick-start/example_003"`.
- Import the component once per page with `import SqlLogicTest from "@site/src/components/SqlLogicTest";`, and pass `hideResult` to render the query without its output.
- An `id` that matches no marker renders **nothing** -- no error, no warning, just a missing example. Grep for the marker after you write the tag.

## Storage compatibility

These rules cover everything SereneDB writes: database files and their write-ahead logs, the search-table WAL and search index directories.

Files are not reproducible byte for byte, and making them so is not a goal. The same data and statements can write different bytes: hash tables iterate in a different order in every process, and parallel builds, checkpoints, refreshes and merges run in a different order every time. Compatibility is about what a reader gets back, so compatibility tests compare contents, never the bytes of a file.

A file never holds stale memory, though: a compression method writes every byte of the segment size it reports, padding and alignment gaps included, so no file carries bytes left in a buffer by another table or database. `StorageVersionTest.CheckpointWritesNoStaleBufferBytes` writes the same data with each compression method twice, from buffers filled with zeros and with ones, and requires identical data blocks. Legacy FSST is left out: it samples its input at random.

Only two places record a storage version, a `serenedb_vN` value of DuckDB's `StorageVersion`:

- The headers of each database file (`engine_v1/<oid>/data.db`). The file's write-ahead log and the database's search-table WAL follow it.
- `segments_N` of each search index directory. The directory's other files are only reached through it.

SereneDB always writes `SERENEDB_LATEST`, and only into its own databases (`CREATE DATABASE`): an `ATTACH` of a DuckDB database refuses a SereneDB storage version, and nothing attaches a SereneDB database by path. A reader opens the versions from `SERENEDB_VERSION_LOWER` to `SERENEDB_VERSION_UPPER` and refuses the rest: a higher one as written by a newer release, a lower one as older than it reads (`duckdb::StorageVersionError`; the constants are in `third_party/duckdb/src/include/duckdb/storage/storage_info.hpp`).

Most changes need no new version:

- **New field or option.** Give it a default that keeps today's behaviour, write it only when it differs from the default, and read the default when it is missing (`WritePropertyWithDefault` with `ReadPropertyWithDefault` or `ReadPropertyWithExplicitDefault`, or a struct member with a default member initializer). Newer releases read older data as the default, and older releases keep reading data that leaves it at the default. They refuse data that uses it, because every reader checks the end of each object. A new `generate_ngrams` option is added this way.
- **Removed field.** Read it with `ReadDeletedProperty`; a struct member becomes `irs::utils::Deleted<T>` of its old type.
- **New value** of an enum or a variant, or a new codec: append it. An older release refuses data that uses a value it does not know. A new compression method is added this way: older releases refuse the `.col` block or the column segment that uses it.
- **Never** reuse a field id, reorder struct members, or change a default, a type or what a field means.

Add a `serenedb_vN` only for a change that older releases would misread instead of refusing, or to stop reading old data. Add it to `third_party/duckdb/src/storage/version_map.json` and run `scripts/generate_storage_info.py` there; `SERENEDB_LATEST` and `SERENEDB_VERSION_UPPER` follow it, and older releases refuse everything the new one writes. To stop reading the data of earlier releases, also point `SERENEDB_VERSION_LOWER` at the new value; data below it has to be upgraded through an earlier release first. Keeping older database files readable (`SERENEDB_VERSION_LOWER` below `SERENEDB_LATEST`) takes one more change, which a `static_assert` in `RequestSereneDBStorageVersion` asks for: attach raises such a file only in memory, so it has to be checkpointed before anything writes to its logs. Make a version change in its own PR and list it in the release notes. A value is never reused.

### Search index files

Every file of an iresearch segment (`segments_N`, `.sm`, `.doc`, `.pos`, `.pay`, `.idx`, `.col`) is the file's data from offset 0, then a footer (a `BinarySerializer` object holding `data_crc32c` and the file's own fields in a `meta` object), then 8 bytes with the footer's CRC32C and its length. `format_utils::WriteFooter` writes it; `format_utils::ReadFooter` checks the checksum and reads the footer. Without a callback, neither side has a `meta` object, and a reader without a callback refuses a footer that has one. `segments_N` stores the storage version as its first field.

- **Field ids:** every object numbers its fields from 0. Name them with `kField...` constants next to the file's writer (`index_meta::kFieldPayload`, `segment_meta::kFieldFiles`, ...), and read them through the same constants.
- **Empty lists** are not written, and are read as optional (`ReadOptionalList`, `ReadOptionalObject`), unless the presence of the list itself means something (the file list of a `.sm`).
- **Callbacks:** footer, list and payload callbacks take `duckdb::BinarySerializer&` and `duckdb::BinaryDeserializer&` (`BinarySerializer::List&` and `BinaryDeserializer::List&` for list elements), never the `Serializer` or `Deserializer` base or `auto&`, so every call into the serializer is direct.
- **New data layout** (block encoding, term dictionary, ...): select it with a new field, and keep reading the old layout while it is supported.
- **Every field is read:** `segments_N` is read with its payload reader. `DirectoryReader` and `DirectoryReader::Reopen` take one, and an index with a payload but no reader is refused.
- The footer trailer and the leading `storage_version` field of `segments_N` never change.

### Database files

Database files are DuckDB database files. Their checkpoint and write-ahead log entries (`.wal`, and the `.wal.checkpoint` and `.wal.recovery` files beside it) are `BinarySerializer` objects with the field ids of `third_party/duckdb/src/include/duckdb/storage/serialization/*.json`: a new field is a json member with a new id and a `default`, and a removed one is marked deleted. A log entry that matches its checksum but cannot be read stops the database from opening instead of being dropped like a torn tail. The log header never gains a field; a framing change bumps `WAL_VERSION_NUMBER`. A file with a SereneDB storage version opens only at a SereneDB storage version, and a file with a DuckDB storage version only at a DuckDB one or with none given. SereneDB opens its own files at `SERENEDB_LATEST` (`RequestSereneDBStorageVersion`), so it refuses a plain DuckDB file, and a plain `ATTACH` refuses a SereneDB file, before anything is read from it.

### DuckDB database files

A database file with a DuckDB storage version (a plain `ATTACH`, `serened shell`) must stay readable by the DuckDB release of that version, and SereneDB reads what that release writes. Field ids and enum values outside SereneDB's ranges belong to upstream:

- **Fields.** A SereneDB field of an upstream class takes 16384 plus the id upstream's numbering would give it: 16484 in a class whose fields start at 100, 16584 at 200. A field from 16384 (`SERENEDB_FIELD_ID_BASE`) up is refused when written to a DuckDB file. With `"version": "serenedb_v1"` on its json member it is skipped there instead: use that for state DuckDB drops as well, such as object ids, constraint names and sequence ownership. A class SereneDB added is reached only through a SereneDB enum value, so its fields keep the usual ids.
- **Enum values.** A SereneDB value of a stored upstream enum starts at 200 (`SERENEDB_ENUM_VALUE_BASE`), and the enum is listed in `IsSereneDBEnumValue`. Writing such a value to a DuckDB file is refused.
- **Data layouts.** A new compression method or block layout (the dict_fsst plus modes, FOR-packed RLE) is chosen only when `IsSereneDBStorageVersion` holds for the storage version being written.
- **Log entries.** Where SereneDB logs an operation in another shape than DuckDB, a DuckDB file keeps DuckDB's: `CREATE SCHEMA` logs the name, and table and view renames log `RenameTableInfo` and `RenameViewInfo`. `ALTER TABLE ADD UNIQUE`, which DuckDB cannot replay, is refused.
- **Catalog.** Objects in a DuckDB file get no owner or privileges (`StoresPermissions` in `server/auth/enforce.cpp`).
- **In-memory databases** have a SereneDB storage version, so they take every SereneDB feature.

`tests/duckdb/run.sh --suite interop` checks both directions against the official `duckdb/duckdb` image; see [tests/duckdb/README.md](tests/duckdb/README.md).

### Data directory

```
engine_v1/
  catalog.wal          the catalog log: the definitions of every database
  <database oid>/      one database
    data.db            its DuckDB file, with data.db.wal beside it
    search.wal.<tick>  its search-table WAL
    <object oid>/      a search table, or an inverted index on a table or view
```

- Every directory has one owner, and only the owner creates or removes it. `catalog::DatabaseDirectory` owns `<database oid>/`. The database's catalog entry, its attachment (until DuckDB has closed the files, through `AttachedDatabase::HoldUntilClosed`), every storage of the database and every pending removal hold it, so after a drop it removes the directory last, once all of them let go. A storage removes its `<object oid>/` after its own drop or a rolled-back create; `DROP DATABASE` marks only the database.
- A directory is created, and its parent fsynced, before the statement that creates its object commits. Paths are oids, so a rename moves nothing.
- Boot removes what no live object owns: each `<database oid>/` that names no database right after the catalog log replays, before bootstrap may create the default database again, and each `<object oid>/` that names no storage inside an attached database once its objects are loaded. That covers a crash between a create and its commit and one between a drop and the removal. A missing catalog log beside database directories that hold anything stops the boot.

### Serialized structs

Blobs stored in catalog entries (tokenizer configs, the inverted index payload), the view-backed index manifest and the segment references of the search-table WAL are written with `irs::utils::WriteTuple` and read with `ReadTuple`. An aggregate is a `BinarySerializer` object whose field ids are the positions of its members, and a member equal to its value in a value-initialized aggregate is not written. A struct boost::pfr cannot reflect (one holding a `std::vector<std::unique_ptr<T>>`) declares `SerdeFields(value)` returning `std::tie` of its members, in declaration order.

### Search-table WAL

The WAL of a database's search tables is a series of segments in the database's directory, `search.wal.<first tick>` with the tick as 16 hex digits. Each frame is `[u64 size][u64 checksum][record]`. The record is a `BinarySerializer` object holding `tick` and then its sections and their ops, each with their own field ids. It records no storage version: the WAL belongs to one database and follows that database's file. The frame and the leading `tick` field never change.

## VSCode Setup

### Profile

We have a VSCode profile which has already all the extensions which are needed (for instance for code navigation). Here is how to set it up:

0. Open a folder with SereneDB.
1. Create a `serenedb-cpp.code-profile` file in the root and paste the profile config below.
2. Open a VSCode command palette via default combination: Ctrl+Shift+P / Cmd+Shift+P for macOS.
3. Write in the palette `Open Profiles` and choose `Preferences: Open Profiles (UI)`.
4. In the UI of the profiles click on the down arrow which is located left to the `New Profile` button.
5. Choose import profile and specify a path to the `serenedb-cpp.code-profile`.
6. Create the profile and switch to it.
7. If a message appears offering to download the clangd server, accept it.

Now you can use C++ code navigation by Ctrl+Click (Cmd+Click for macOS)!

<details>
<summary>Profile config</summary>

```json
{
  "name": "SereneDB C++ template",
  "settings": "{\"settings\":\"{\\n    \\\"window.titleBarStyle\\\": \\\"custom\\\",\\n    \\\"files.trimFinalNewlines\\\": true,\\n    \\\"files.insertFinalNewline\\\": true,\\n    \\\"workbench.settings.applyToAllProfiles\\\": [\\n        \\\"files.insertFinalNewline\\\",\\n        \\\"files.trimFinalNewlines\\\",\\n        \\\"editor.inlayHints.enabled\\\",\\n        \\\"remote.autoForwardPorts\\\",\\n        \\\"files.autoSave\\\",\\n        \\\"editor.minimap.enabled\\\"\\n    ],\\n    \\\"editor.inlayHints.enabled\\\": \\\"off\\\",\\n    \\\"remote.autoForwardPorts\\\": false,\\n    \\\"files.autoSave\\\": \\\"afterDelay\\\",\\n    \\\"settingsSync.ignoredSettings\\\": [\\n        \\\"-clangd.path\\\"\\n    ],\\n    \\\"clangd.arguments\\\": [\\n        \\\"--compile-commands-dir=${workspaceFolder}/build\\\",\\n        \\\"--function-arg-placeholders=0\\\",\\n        \\\"--header-insertion=never\\\"\\n    ],\\n    \\\"window.newWindowProfile\\\": \\\"Default\\\",\\n    \\\"editor.minimap.enabled\\\": false,\\n    \\\"compilerexplorer.compilationDirectory\\\": \\\"${workspaceFolder}/build_rel\\\",\\n    \\\"editor.defaultFormatter\\\": \\\"llvm-vs-code-extensions.vscode-clangd\\\",\\n    \\\"extensions.ignoreRecommendations\\\": true,\\n    \\\"clangd.checkUpdates\\\": true,\\n    \\\"editor.tabSize\\\": 2,\\n    \\\"workbench.remoteIndicator.showExtensionRecommendations\\\": false\\n}\\n\"}",
  "extensions": "[{\"identifier\":{\"id\":\"github.remotehub\",\"uuid\":\"fc7d7e85-2e58-4c1c-97a3-2172ed9a77cd\"},\"displayName\":\"GitHub Repositories\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"harikrishnan94.cxx-compiler-explorer\",\"uuid\":\"68ef4789-1f8c-4d80-b929-cfb718979aa2\"},\"displayName\":\"C/C++ Compiler explorer\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"llvm-vs-code-extensions.vscode-clangd\",\"uuid\":\"103154cb-b81d-4e1b-8281-c5f4fa563d37\"},\"displayName\":\"clangd\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-containers\",\"uuid\":\"93ce222b-5f6f-49b7-9ab1-a0463c6238df\"},\"displayName\":\"Dev Containers\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-ssh\",\"uuid\":\"607fd052-be03-4363-b657-2bd62b83d28a\"},\"displayName\":\"Remote - SSH\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-ssh-edit\",\"uuid\":\"bfeaf631-bcff-4908-93ed-fda4ef9a0c5c\"},\"displayName\":\"Remote - SSH: Editing Configuration Files\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.vscode-remote-extensionpack\",\"uuid\":\"23d72dfc-8dd1-4e30-926e-8783b4378f13\"},\"displayName\":\"Remote Development\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-explorer\",\"uuid\":\"11858313-52cc-4e57-b3e4-d7b65281e34b\"},\"displayName\":\"Remote Explorer\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-repositories\",\"uuid\":\"cf5142f0-3701-4992-980c-9895a750addf\"},\"displayName\":\"Remote Repositories\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-server\",\"uuid\":\"105c0b3c-07a9-4156-a4fc-4141040eb07e\"},\"displayName\":\"Remote - Tunnels\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"vadimcn.vscode-lldb\",\"uuid\":\"bee31e34-a44b-4a76-9ec2-e9fd1439a0f6\"},\"displayName\":\"CodeLLDB\",\"applicationScoped\":false}]"
}
```

</details>

<p align="center">
  <img src="https://github.com/user-attachments/assets/02f2e2f9-b9d6-407d-832a-2517254dee98" width="800" style="border-radius: 8px;">
</p>

### Debugging

VSCode provides a convenient way to debug code. Create a `.vscode/launch.json` file:

```json
{
  "configurations": [
    {
      "type": "lldb",
      "request": "attach",
      "name": "attach-to-serened",
      "program": "${workspaceFolder}/build/bin/serened"
    },
    {
      "type": "lldb",
      "request": "launch",
      "name": "iresearch",
      "program": "${workspaceFolder}/build/bin/iresearch-tests",
      "args": ["--gtest_filter=*PhraseFilterTestCase*"],
      "cwd": "${workspaceFolder}"
    }
  ]
}
```

Click **Run and Debug** on the left sidebar (Shift+Ctrl+D / Shift+Cmd+D). This adds two actions -- `attach-to-serened` for attaching to a running instance and `iresearch` to launch unit tests with the debugger. Use the dropdown next to the green triangle to pick one.

<p align="center">
  <img src="https://github.com/user-attachments/assets/fa246b5d-ebea-4598-8705-c252fbff5a0d" width="800" style="border-radius: 8px;">
</p>

### Sqllogic test highlighting

`.test` files are Plain Text until you install the VSCode extension that ships
with the runner: it colors SQL bodies as SQL, sqllogictest-rs directives as
keywords, and `#` comments as comments. It lives beside the parser whose syntax
it tracks, so the build-and-install steps -- including what to run when the
`code` CLI is not on `$PATH`, as over SSH -- are in
[`third_party/sqllogictest-rs/README.md`](third_party/sqllogictest-rs/README.md).

## C++ Code Style

Based on common sense, Google C++ style guide, and Abseil best practices. These rules apply to serenedb and iresearch code.

Style issues shouldn't block PRs -- anything not caught automatically can be fixed later. This is a living document.

### Tools

Single supported toolchain: latest stable clang, CMake, VSCode. We use the latest C++ standard.

### Naming

Enforced by [`.clang-tidy`](.clang-tidy) and [pre-commit](.pre-commit-config.yaml).

### Formatting

Handled by [`.clang-format`](.clang-format) and [pre-commit](.pre-commit-config.yaml). No style discussions in PRs.

The DuckDB family (the duckdb submodules, `database-connector`, `duckdb_clickhouse`) follows DuckDB's own style and generators instead: [`scripts/duckdb_family.sh`](scripts/duckdb_family.sh) formats it with DuckDB's `scripts/format.py` and builds the duckdb fork's `regen:` commit; [`tests/duckdb/README.md`](tests/duckdb/README.md) has the rules for a DuckDB update.

### Include Ordering

Handled by [`.clang-format`](.clang-format).

### Headers

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Header_Files) with differences:

- `#pragma once` instead of include guards
- Forward declarations only in dedicated `fwd.h` files (one per directory max)
- `.hpp` for headers, `.tpp` for template implementations, `.cpp` for sources
- Avoid pimpl (exception: abstracting over multiple library backends)
- Tests mirror source directory structure
- Avoid duplicating directory name in filename
- `inline` only for linkage; use force inline for optimization hints
- Templates/inline everything is bad -- binary size matters
- Avoid manual template instantiation in `.cpp` (switch-like dispatch is ok)

### Scoping

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Scoping):

- No `using namespace` (forbidden in headers)
- Write code inside the real namespace
- `inline namespace` for versioning only
- Namespace aliases, `using enum/class/struct` ok in sources (forbidden in headers)
- Single anonymous namespace over multiple `static` declarations
- `constexpr` over `const`
- `constinit` / magic static / `inline static` to avoid static init order issues
- Avoid code in global namespace

### Initialization

- Prefer braced init `{}` over `make_*` for pair/tuple (faster to compile)
- No raw `new`/`delete` -- use `make_*` functions
- Prefer braced init over parenthesized constructors
- POD-like types: use designated initializers `{.foo = 1, .bar = 2,}`
- Default `operator==`/`<=>`/`=` and constructors when possible
- Trailing comma required for multi-line initializer lists
- Forbidden: `Type var{};` and `Type var = {};` -- just omit for default construction
- Prefer `auto` with factory functions: `auto x = MakeFoo()`
- `const` strongly recommended on methods, references, and pointees; on variables it's the author's call
- Prefer `emplace`-like functions
- Prefer `const auto*` over plain `auto` for pointers

### Classes

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Classes):

- Trailing comma in enum/enum class
- Free functions over member functions for structs
- Structs over `std::pair`/`std::tuple`
- Structs: everything public. Classes: private members (except static/constexpr)
- Avoid friends
- Avoid public init functions -- do work in constructors
- Prefer explicit constructors

### Functions

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Functions):

- Trailing return types
- Lambda without args: `[] {}`
- Overloads are fine, but avoid ambiguous ones like `const T&` vs `std::shared_ptr<const T>&`
- Default args banned for virtual functions -- use overloads

### Comments

- Every file needs a license header; pre-commit adds/checks it, so don't write one by hand. (The license block is the *only* place the banner style is allowed.)
- Elsewhere, plain `//` comments only. No doxygen, no decorative separators of any flavour -- `// ---`, `/*** ... ***/`, `////////`, `//===`, etc. They're noise in normal code and especially bad as section dividers.
- Comment only what the code can't say itself: a hidden constraint, an
  invariant, a workaround for a specific bug, or the *why* behind a
  non-obvious shape. A function or struct can earn a one-line intro
  stating its role. Don't describe *what* the body does -- the body
  already does.
- Don't justify changes in the source. "We used to do X, we now do Y
  because..." belongs in the PR description / commit message. The source
  is read by someone who has never seen the prior version, so describe
  the current contract positively, not relative to what it replaced.
- Asserts are contracts; the expression is the documentation. Skip the
  message when it would just translate the expression into English
  (`SDB_ASSERT(i < n)` is enough). Add one only when the failure scenario
  isn't visible in the expression: a domain rule, an unusual comparison
  shape (e.g. `"running sum overflow"` for `a + b >= a`), or a design
  constraint the comparison enforces.

### Error Handling

- PostgreSQL/frontend code: use `THROW_SQL_ERROR`
- Common/backend code: both `absl::Status` and `throw` are acceptable
- Consider performance: `absl::Status` with a only code is not allocate
- `SDB_ASSERT` for debug-only checks
- `SDB_ENSURE` for debug crash + release throw
- `SDB_VERIFY` for crash in both debug and release

### Async

- Use C++20 coroutines (`co_await` / `co_return`) with `yaclib::Future` for async code
- Avoid raw threads and callbacks in database logic
- Sync primitives are for deep implementation details only

### Logging

- Use `SDB_LOG(level, topic, ...)` macros from `iresearch/utils/log.hpp`
- Shortcuts: `SDB_ERROR(topic, ...)`, `SDB_INFO(topic, ...)`

### Integer Types

- Prefer explicitly sized types: `int32_t`, `uint64_t`, `uint8_t` over bare `int`
- Size enums explicitly: `enum class Foo : uint8_t { ... }`

### [[nodiscard]]

- Apply `[[nodiscard]]` to types where ignoring the return value is a bug: `Result`, `ErrorCode`, `Future`
- Apply to methods where callers must check the result

### Templates

- Prefer `template + static_assert` over concepts when possible -- gives better errors and compiles faster
- Use C++20 concepts when `static_assert` would be awkward (e.g. constrained overload sets)
- Avoid SFINAE / `enable_if` in new code

### Library Preferences

- `absl::Hash` over `std::hash`; `absl::*_hash_*` over `std::unordered_*`
- `absl::btree_*` over `std::set`/`std::map` when appropriate
- `std::span<const T>` over `std::initializer_list<T>` in parameters
- `magic_enum` for enum names
- `absl::c_any_of` (etc.) over `std::any_of(begin, end)`. Fall back to `std::ranges` when no `absl::c_*` exists (e.g. `std::ranges::sort(range, {}, proj)`).
- Prefer imperative loops over ranges pipelines
- String operations: `absl::StrCat`, `absl::Substitute`, `absl::StrJoin`, `absl::StrSplit`
- No `fmt`/`printf` unless necessary; use `absl::SPrintf` or `std::format` (Velox code)
- Avoid streams API (`operator<<`/`>>`) in new code. See also `absl::StreamFormat`
- Implicit conversion to bool: prefer `if (auto x = something())` over `if (auto x = something(); x)`
- Nullptrs: [Google style](https://google.github.io/styleguide/cppguide.html#0_and_nullptr/NULL), default constructor is ok for smart pointers
- Pre-increment/pre-decrement: [Google style](https://google.github.io/styleguide/cppguide.html#Preincrement_and_Predecrement)
- Casting: [Google style](https://google.github.io/styleguide/cppguide.html#Casting)
- Avoid RTTI
- `noexcept`: see dedicated section below
- No `&&` references for trivially copyable types
- Don't misuse `std::forward` and `std::move`
- `std::string_view` almost everywhere except C API boundaries
- References over pointers when ownership doesn't matter

### noexcept

- Destructors must be `noexcept` (implicit, but be explicit if non-trivial)
- Move constructors and move assignment must be `noexcept` (required for efficient container operations)
- Other functions: only mark `noexcept` when truly noexcept or required for correctness
- Don't add `noexcept` speculatively -- it's a contract that's hard to remove later

### Idioms

- Treat raw pointers, smart pointers, and `std::optional` uniformly via
  contextual `bool` and `operator*`. Applies everywhere a `bool` is
  expected -- `if` / ternary / `SDB_ASSERT` / `&&` / `||` / `return`, not
  just `if`:
  - `if (p)` / `if (!p)`, not `if (p != nullptr)`.
  - `if (opt)`, not `if (opt.has_value())`.
  - `*p` / `*opt`, not `opt.value()` (`.value()` adds a redundant throw
    once you've verified the optional is engaged).
- Don't add an explicit `std::string{...}` conversion until the code
  fails to compile without it (e.g. `set.contains(sv)`, not
  `set.contains(std::string{sv})`).
- Don't add includes speculatively -- only when clangd or the compiler
  asks for them.

### Memory and Ownership

- `unique_ptr` by default for owned resources
- `shared_ptr` only when ownership is genuinely shared -- justify it
- No raw owning pointers in new code
- Use `make_unique` / `make_shared` -- never bare `new`/`delete`
- Prefer stack allocation and value types over heap allocation
- Use `std::string_view`, `std::span` for non-owning references to data

### Performance

- Avoid allocations in hot paths
- Avoid virtual calls in hot paths (prevents inlining, which is the main cost)
- Large buffers should be heap-allocated separately, not inlined as arrays/members in objects (inflates object size, fitting poorly into allocator size classes)
- Prefer contiguous memory (vectors, arrays) over node-based containers (lists, maps)
- Measure before optimizing -- don't guess
- Binary size matters: excessive inlining/templates hurt icache and build times
- Validate performance claims with microbenchmarks under `tests/bench/micro/` (Google Benchmark). Register one with `add_bench(<name>)` in that directory's `CMakeLists.txt` -- `<name>.cpp` either registers `BENCHMARK`s or defines its own `Main` with `sdb::bench::AddMain` -- build with `ninja serenedb-bench-micro`, run it as `build/bin/serenedb-bench-micro <name> [--benchmark_filter=...]`. The same binary answers to `search-benchmark-game-build` and `search-benchmark-game-query`, the search benchmark game's tools.
- Use the `bench` cmake preset for production-like numbers.
- A microbench fits when the change is a few well-scoped functions. When the
  change is broader (a whole query path, an end-to-end pipeline, anything that
  doesn't sit neatly inside one fixture), drive a small standalone repro script
  through `perf stat` / `perf record` instead -- it locates the hot spot
  without forcing the change into a microbench shape that doesn't fit.

### Testing

- gtest framework: `TEST()` for standalone, `TEST_F()` for shared fixtures, `TEST_P()` for parameterized.
- Async tests use `yaclib::WaitGroup` for synchronization.
- Test files mirror source structure: `server/foo/bar.cpp` -> `tests/server/foo/bar_test.cpp`.
- Test names describe behavior, not implementation.
- Prefer an explicit `SDB_ASSERT` contract over a comment about an invariant; reach for `SDB_ENSURE` / `SDB_VERIFY` only when the guarantee is genuinely hard to follow locally.

(For when each test type applies and where new tests go, see the top-level **Test** section.)

### Third-Party Dependencies

- All third-party deps are forked to the `serenedb/` GitHub org
- Added as git submodules in `third_party/`, pinned to a specific commit or tag
- To add a new dependency: fork to `serenedb/`, add submodule, update `.gitmodules`
- Discuss with maintainers before adding new dependencies

### Working with Submodules

Submodules are cloned with `--depth 1` (shallow) by default, which means only the pinned commit is fetched and no branches are visible. If you need to actively develop inside a submodule (e.g. `third_party/duckdb`), run:

```bash
cd third_party/<submodule>
git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*"
git fetch origin --unshallow
git checkout <your-branch>
```

This configures the submodule to fetch all branches (persists for your local clone) and lets you work with it like a normal repo -- `git push`, `git pull`, `git branch`, etc. will all work as expected.

To apply this to every submodule at once, run from the repo root:

```bash
git submodule foreach --recursive 'git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*" && git fetch origin --unshallow'
```

---

# Thank you for your contribution <3

<p align="center">
  <img src="https://github.com/user-attachments/assets/86dedb73-478f-4344-9dcb-320200435b99" width="300" style="border-radius: 8px;">
</p>
