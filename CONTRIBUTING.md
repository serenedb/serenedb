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

- Compiler: clang-23 / clang++-23, the same as CI. `sudo scripts/setup_dev_toolchain.sh` installs it from apt.llvm.org on Ubuntu and points `clang`, `clang++`, `lld`, `lldb`, `clangd` and the other LLVM tools at it.
- Build system: Ninja
- CMake >= 3.28

The toolchain only moves forward.

### Build

```bash
cmake --preset lldb
cd build/
ninja
```

Additional build presets are defined in `CMakePresets.json`:
- `lldb` -- Debug build (`build/`), works with lldb, gdb, or any debugger
- `clangd` -- RelWithDebInfo build (`build_clangd/`), works well with the clangd language server in VSCode
- `asan`, `tsan`, `msan` -- RelWithDebInfo builds (`build_asan/`, `build_tsan/`, `build_msan/`) under AddressSanitizer, ThreadSanitizer and MemorySanitizer with origin tracking, on the system allocator, dynamically linked
- `bench` -- Release build (`build_bench/`), static linking, configured like the release packages: ThinLTO, no frame pointers and remapped machine code, so profile with the `perf` preset
- `perf` -- RelWithDebInfo build (`build_perf/`), static linking, `-O3` with frame pointers, for profiling

`lldb`, `clangd` and the sanitizer presets build with dev asserts, fault injection and the gtest binaries; `bench` and `perf` build none of them, so recovery tests and anything that sets `sdb_faults` can't run there.

The build type sets the defaults of the codegen options; each can be set on its own:

| | Debug | RelWithDebInfo | Release |
|---|---|---|---|
| Optimization | `-O0` | `-O3` | `-O3` |
| `SDB_FRAME_POINTERS` | on | on | off |
| `SDB_DEBUG_INFO` | `FULL` | `STACK` | `STACK` |
| `SDB_LTO` (ThinLTO) | off | off | on |
| `SDB_REMAP_EXECUTABLE` (serened's `--remap_executable` default) | off | off | on |

`SDB_DEBUG_INFO` is `NONE`, `STACK` (line tables: symbolized stacks and perf, inlined frames included) or `FULL` (variables and types, tuned for lldb). Every preset builds with `FULL`, CI with `STACK`, and the release packages ship their `STACK` debug info separately, in a `-dbgsym` tarball and a `-dbg` package.

### Debug info and disk use

Every binary links most of the server statically, so debug info dominates its size. Two settings keep a build directory small:

- **Split DWARF** (`SDB_SPLIT_DWARF`, on by default except on macOS, off in CI): debug info is written once, into a `.dwo` file beside each object, and the binaries only point at those files. lldb, gdb, perf, `llvm-symbolizer` and `addr2line` follow the pointers on their own, as long as the build directory is there. A binary copied out of it keeps its symbols and line numbers but loses inlined frames, variables and types; to keep those too, pack the debug info next to the copy:

  ```bash
  llvm-dwp -e build/bin/serened -o /path/to/copy/serened.dwp
  ```

  lldb and gdb pick up `<binary>.dwp` beside the binary automatically.
- **Relative paths**: no build output holds an absolute path. `__FILE__` and coverage name sources relative to the repo root (`server/...`); debug info names the build directory relative to the repo root and sources relative to it, so lldb, perf and `llvm-symbolizer` find sources and `.dwo` files when run from the repo root (or point lldb at the checkout with `settings set target.source-map . <checkout>`). The build directory must not be the repo root itself. With ccache's `base_dir` set to a directory above your checkouts and `hash_dir = false`, checkouts at different paths share the cache, CI's included.
- **Thin archives**: static libraries (except on macOS) only reference their objects instead of holding copies, so they cannot be moved or installed without the build directory -- nothing in the build does that.

Tools and benchmarks share binaries instead of each linking their own: `serenedb-bench-micro <bench> [args...]` runs one micro benchmark (see [Performance](.claude/rules/cpp.md#performance)), and `iresearch-examples <example>` runs one of the iresearch examples. Both print what they offer when run without arguments.

### The search-benchmark-game build

`-DIRESEARCH_BENCH=ON` configures a tree for `search-benchmark-game` alone: its two executables link an `iresearch` that leaves out vector search, geo, fastText, every tokenizer but `split_text`, every scorer but BM25, and match offsets, over DuckDB with only its `icu` extension. Such a tree compiles about a sixth of what the full one does for that target, and it has no server, tests or micro benchmarks; `SDB_GTEST` is refused there.

```bash
cmake --preset perf -B build_sbg -DIRESEARCH_BENCH=ON
cmake --build build_sbg -t search-benchmark-game
```

### The embedded documentation index

`docs/` and a search index of it are built into `serened` and `serenedb-tests`, where the `sdb_docs` functions and the shell's `.docs` read them; a `docs/` change needs only a rebuild.

- To skip it, configure with `-DSDB_EMBEDDED_DOCS=OFF`; that build has no documentation to read.
- On macOS the index gets a fixed 4 MiB region. When the build fails because the index outgrew it, raise `kCapacity` in `server/docs/docs_index_image.cpp`.

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
