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

- Compiler: clang-21 / clang++-21; CI builds with clang-23, so code must compile with both
- Build system: Ninja
- CMake >= 3.28

The toolchain only moves forward.

### Build

```bash
cmake --preset lldb -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21
cd build/
ninja
```

Additional build presets are defined in `CMakePresets.json`:
- `lldb` -- Debug build (`build/`), works with lldb, gdb, or any debugger
- `clangd` -- RelWithDebInfo build (`build_clangd/`), works well with the clangd language server in VSCode
- `asan`, `tsan`, `msan` -- RelWithDebInfo builds (`build_asan/`, `build_tsan/`, `build_msan/`) under AddressSanitizer, ThreadSanitizer and MemorySanitizer with origin tracking, on the system allocator, dynamically linked and without io_uring
- `bench` -- Release build (`build_bench/`), static linking, configured like the release packages; no frame pointers
- `perf` -- RelWithDebInfo build (`build_perf/`), static linking, `-O3` with frame pointers, for profiling

`lldb`, `clangd` and the sanitizer presets build with dev asserts, fault injection and the gtest binaries; `bench` and `perf` build none of them, so recovery tests and anything that sets `sdb_faults` can't run there.

### Debug info and disk use

Every binary links most of the server statically, so debug info dominates its size. Two settings keep a build directory small:

- **Split DWARF** (`SDB_SPLIT_DWARF`, on by default except on macOS, off in CI): debug info is written once, into a `.dwo` file beside each object, and the binaries only point at those files. lldb, gdb, perf, `llvm-symbolizer` and `addr2line` follow the pointers on their own, as long as the build directory is there. A binary copied out of it keeps its symbols and line numbers but loses inlined frames, variables and types; to keep those too, pack the debug info next to the copy:

  ```bash
  llvm-dwp -e build/bin/serened -o /path/to/copy/serened.dwp
  ```

  lldb and gdb pick up `<binary>.dwp` beside the binary automatically.
- **Thin archives**: static libraries (except on macOS) only reference their objects instead of holding copies, so they cannot be moved or installed without the build directory -- nothing in the build does that.

Tools and benchmarks share binaries instead of each linking their own: `serenedb-bench-micro <bench> [args...]` runs one micro benchmark (see [Performance](.claude/rules/cpp.md#performance)), and `iresearch-examples <example>` runs one of the iresearch examples. Both print what they offer when run without arguments.

### The embedded documentation index

`docs/` and a search index of it are built into `serened` and `serenedb-tests`, where the `sdb_docs` functions and the shell's `.docs` read them; a `docs/` change needs only a rebuild.

- To skip it, configure with `-DSDB_EMBEDDED_DOCS=OFF`; that build has no documentation to read.
- On macOS the index gets a fixed 4 MiB region. When the build fails because the index outgrew it, raise `kCapacity` in `server/docs/docs_index_image.cpp`.

### PostgreSQL catalog code

Everything SereneDB copies from PostgreSQL's system catalogs is generated, never written by hand: the built-in types SereneDB supports and the functions they reference, the oids of catalog tables, schemas, access methods and languages, each catalog table's columns (type, NOT NULL and lookup key), `pg_catalog` and `information_schema` view definitions, the keyword list `quote_ident` uses, and stubs of the functions the views call. The output is `server/pg/catalog/generated/*.gen.inc`, and nothing else lives in that directory. Only what PostgreSQL defines is copied; what describes SereneDB itself, such as which types exist or what a setting's value is, is written by hand.

`scripts/generate_pg_catalog.py` reads the PostgreSQL source in the `third_party/postgres` submodule. It runs PostgreSQL's own `genbki.pl` over the catalog headers and `.dat` files and reads the `postgres.bki` it writes, the same file initdb bootstraps from. It also reads the SQL files initdb runs after bootstrap (`system_views.sql`, `information_schema.sql`, `system_functions.sql`), the privileges `initdb.c` sets and the keywords in `kwlist.h`. It needs Perl and no PostgreSQL server:

```bash
git submodule update --init third_party/postgres
python3 scripts/generate_pg_catalog.py
```

A system object has one OID in every database and every release. Below 16384 the OIDs are PostgreSQL's; SereneDB's own system objects use 16384 to 49151 (`server/pg/catalog/oids.h`), DuckDB's builtins in its system catalog 49152 to 65535, and the objects users create 65536 and up.

initdb numbers the objects it creates from those SQL files while it runs, so the source holds no OIDs for them. `server/pg/catalog/system_oids.lock` keeps their OIDs, and the OIDs of SereneDB's builtin functions and collations. The system views, `information_schema` and its domains and tables have the OIDs PostgreSQL 18.6's initdb gave them. A builtin function whose kind, name and argument types are exactly a PostgreSQL function's has PostgreSQL's OID, and every other builtin function and collation has one from SereneDB's range. The generator keeps the file and nothing else edits it: a new object gets the next free OID, a removed one stays as a `retired` line, and no line is ever renumbered or reused.

The builtin functions and collations come from the DuckDB catalog, which only a running server has, so after adding, removing or changing the signature of one, rerun the generator with a serened built from the change:

```bash
python3 scripts/generate_pg_catalog.py --serened build/bin/serened
```

Until then the new function or collation has an OID from the unfrozen range (`kFirstUnfrozenFunction`, `kFirstUnfrozenCollation`), and `tests/sqllogic/sdb/pg/system/system_oids.test` fails.

What SereneDB decides on its own is kept apart from the generated output:

- `scripts/pg_catalog/config.py` names the PostgreSQL objects SereneDB refers to by constant: schemas, databases, tablespaces, access methods, languages, procedures and collations.
- `server/pg/types.cpp` maps each PostgreSQL type SereneDB supports to a DuckDB type. `pg_type` lists every PostgreSQL type; one without a mapping exists only in the catalog, like the pseudo-types: its name resolves (`::regtype`, `format_type`) but no value of it can be created. `server/pg/types.h` says how each column type is stored. `server/pg/catalog/builtin/builtin.h` defines SereneDB's own types; a PostgreSQL type of the same name, such as `tsquery`, is not copied.
- `server/query/config_variables.cpp` lists every setting SereneDB accepts, with its value, description and what `pg_settings` shows for it; a PostgreSQL setting SereneDB does not implement is accepted only when clients or `pg_dump` scripts set it, and its description says it has no effect.
- `server/pg/catalog/views/{pg_catalog,information_schema}/<view>.cpp` holds the SQL of a PostgreSQL view whose body SereneDB rewrites, as a C++ string constant; the generated views keep PostgreSQL's bodies and the registry uses the file's SQL in their place. `views/sdb/<view>.cpp` defines one of SereneDB's own views whole, with its oid from `oids.h`. The generator registers every such file in `generated/registry.gen.inc` and stops on a file in the PostgreSQL folders named after no view.
- `server/pg/catalog/functions/{pg_catalog,information_schema}/` holds the functions of each schema, grouped by the PostgreSQL source file that defines them (`acl.cpp`, `dbsize.cpp`, `format_type.cpp`, `regproc.cpp`, `ruleutils.cpp`, the rest in `misc.cpp`); `functions/functions.cpp` registers them. `macros.cpp` holds the functions SereneDB writes in SQL, as C++ string constants. Every `pg_catalog` function the generated views call has a generated stub, which startup registers only when neither SereneDB nor DuckDB defines a function of that name: a stub with output columns returns no rows, one with a numeric result returns 0 (NULL for a NULL argument), any other NULL.
- `server/pg/catalog/tables/{pg_catalog,information_schema,sdb}/<table>.cpp` produces the rows of one catalog table and sets SereneDB's constant column values; a NOT NULL column a row leaves unset reads as its type's zero, any other as NULL. The table's class also states the row estimate the planner sees (`kRows`), lookup keys PostgreSQL does not declare (`kColumnKeys`) and whether only superusers may read it (`kSuperuserOnly`). A file named after a PostgreSQL view makes that view a table, as `pg_settings` is. The rows of the `information_schema.sql_*` tables are SereneDB's own claims, written there by hand, and `sql_features`, which would list a claim per SQL standard feature, has none; `sdb/` holds SereneDB's own `sdb_*` tables, which live in `pg_catalog` and define their columns and oid themselves. The generator registers every such file in `generated/registry.gen.inc` and stops on a file in the PostgreSQL folders named after no catalog table or view. A catalog without a file exists with no rows.

The same script owns the three outputs that come from this repository alone: `generated/registry.gen.inc`, `tests/sqllogic/sdb/pg/site_docs/compatibility/system_table_claims.test`, which checks what `docs/compatibility/system-table-compatibility.md` claims for each relation, and `data_type_claims.test` next to it, which checks what `docs/compatibility/data-type-compatibility.md` claims for each type; the generator also stops when that page misses a PostgreSQL type. Pre-commit runs `python3 scripts/generate_pg_catalog.py --check`, which fails while any output is stale; without the submodule checked out it checks only those three.

To move to a new PostgreSQL release, bring that release's files into `third_party/postgres` (a pull request to a version branch of serenedb/postgres), rerun the generator and fix what no longer compiles. Column names are checked at compile time, and a debug build checks every column's storage type at startup.

### Launch

```bash
./build/bin/serened ./build_data --listen='postgres://0.0.0.0:7890'
```

Connect via psql: `psql -h localhost -p 7890 -U postgres`

## Guides

Everything else has a page of its own under `.claude/rules/`. Claude Code loads a page when it opens a file the page covers, and "Branching, commits, PRs" in every session.

- [Branching, commits, PRs](.claude/rules/pull-requests.md), forks included.
- [C++](.claude/rules/cpp.md): what all our C++ shares, performance included, and the pages for [our code](.claude/rules/cpp-serenedb.md), [the DuckDB family](.claude/rules/cpp-duckdb.md) and [the other submodules](.claude/rules/cpp-third-party.md).
- [Tests](.claude/rules/testing.md) and [writing sqllogic tests](.claude/rules/sqllogic.md).
- [CI](.claude/rules/ci.md).
- [Documentation](.claude/rules/docs.md).
- [Storage compatibility](.claude/rules/storage.md): changing anything SereneDB writes to disk.

---

# Thank you for your contribution <3

<p align="center">
  <img src="https://github.com/user-attachments/assets/86dedb73-478f-4344-9dcb-320200435b99" width="300" style="border-radius: 8px;">
</p>
