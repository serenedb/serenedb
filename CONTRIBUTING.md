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
- `bench` -- Release build (`build_bench/`), static linking, configured like the release packages; no frame pointers
- `perf` -- RelWithDebInfo build (`build_perf/`), static linking, `-O3` with frame pointers, for profiling

`lldb` and `clangd` build with dev asserts, fault injection and the gtest binaries; `bench` and `perf` build none of them, so recovery tests and anything that sets `sdb_faults` can't run there.

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

`docs/` is compiled into the binary together with a prebuilt search index of it. The read-only `sdb_docs` functions and the shell's `.docs` read that image straight from the binary, so the server indexes nothing at startup and leaves nothing in the datadir.

The index cannot be produced from the sources the way the documentation text is, because building it needs the indexer that lives in the server being built. So `serened` builds it itself, right after it is linked:

1. `scripts/generate_docs.py --corpus` writes the documentation text to a file in the build directory.
2. `serened` is linked with an empty `.sdb_docs` section for the index (`server/docs/docs_index_image.cpp`); `server/docs/docs_index.ld` places it after `.bss`, alone in the last loadable segment.
3. `serened <datadir> --build_docs_index=<out> --docs_corpus=<file>` boots it on a throwaway datadir, indexes the documentation and the catalog of the objects it documents, and writes them to `<out>/docs` and `<out>/objects`, each with a layout file naming its column and field ids. It then exits before any listener is started.
4. `scripts/embed_docs_index.py` writes both directories into `.sdb_docs` of the binary that built them and grows the section and its segment to exactly their size. Nothing is loaded after that segment, so only the non-loaded sections behind it in the file move.

macOS has no linker scripts, so there `serened` reserves a fixed 4 MiB region instead and step 4 fills it in place. If the index outgrows it, the macOS build fails and says so; raise `kCapacity` in `docs_index_image.cpp`.

`serenedb-tests` is linked and embedded the same way, so the documentation tests run against what `serened` ships.

Steps 3 and 4 run every time `serened` is linked, which includes every change to `docs/`. So the image always matches the server it lives in, and there is nothing to keep in sync by hand. To skip it, configure with `-DSDB_EMBEDDED_DOCS=OFF`: that build carries no documentation, so `.docs` and `sdb_docs` have nothing to read.

### Launch

```bash
./build/bin/serened ./build_data --listen='postgres://0.0.0.0:7890'
```

Connect via psql: `psql -h localhost -p 7890 -U postgres`

## Guides

Everything else has a page of its own under `.claude/rules/`. Claude Code loads a page when it opens a file the page covers, and "When you change ..." and "Branching, commits, PRs" at the start of every session.

- [Tests](.claude/rules/testing.md): where a test goes, when a change needs one, how to run every suite, and what CI runs.
- [Writing sqllogic tests](.claude/rules/sqllogic.md), races included.
- [CI workflows and images](.claude/rules/ci.md): running a workflow locally, and adding a dependency to the CI images.
- [C++ Code Style](.claude/rules/cpp.md), performance included.
- [Documentation](.claude/rules/docs.md): what a change documents in `docs/`, and SQL examples that run as tests.
- [Storage compatibility](.claude/rules/storage.md): changing anything SereneDB writes to disk.
- [Third-party dependencies](.claude/rules/third-party.md): how they are built, adding one, and working inside a submodule.
- [When you change ...](.claude/rules/when-you-change.md): the follow-up step each kind of change needs.
- [Branching, commits, PRs](.claude/rules/pull-requests.md)
- [VSCode Setup](.claude/rules/vscode.md)

---

# Thank you for your contribution <3

<p align="center">
  <img src="https://github.com/user-attachments/assets/86dedb73-478f-4344-9dcb-320200435b99" width="300" style="border-radius: 8px;">
</p>
