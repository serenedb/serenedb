---
paths:
  - "third_party/**"
  - "cmake/**"
  - ".gitmodules"
---

# Other submodules and dependencies

Every submodule except the DuckDB family ([cpp-duckdb.md](cpp-duckdb.md)); the fork rules are in [pull-requests.md](pull-requests.md).

- Patch in the submodule's own style: format the lines you change with its `.clang-format` when it has one (`git clang-format`), match the code around them otherwise, and never reformat lines you didn't change.
- The root `CMakeLists.txt` owns compile policy. A dependency never sets the C++ standard, the build type, `-march`/`-mcpu`/`-mtune`/`-mno-*`, PIC, frame pointers, `-g`, `NDEBUG` or visibility for its tree: turn its option off, or delete the override in our copy. Exceptions: the LLVM runtimes and per-architecture dispatch kernels.
- One ISA baseline, Haswell on amd64 and `armv8-a+crc+crypto` on arm64, with the library's runtime dispatch above it where it has one (OpenBLAS `DYNAMIC_ARCH`, faiss `FAISS_OPT_LEVEL=dd`, zlib-ng `WITH_RUNTIME_CPU_DETECTION`). After adding or updating a dependency, check `compile_commands.json`: every file carries the baseline and `-std=c++26`, and only dispatched kernels go above it.
- Configure a dependency in `third_party/CMakeLists.txt` with `set(<OPTION> <value> CACHE <type> "" FORCE)` before its `add_subdirectory`.
- A new dependency is discussed with the maintainers first ([third_party/README.md](../../third_party/README.md)) and listed in `third_party/LICENSES.md`. Before calling one unused, search the whole tree, the other submodules included; benchmarks don't count as users.
- Pattern matching goes through RE2: a fast path goes inside our RE2 fork, where every caller gets it.
- A generated submodule changes only through its script (`third_party/libstemmer_c`: `scripts/update_libstemmer.sh`).
- Submodules are shallow. To work in one: `git config remote.origin.fetch "+refs/heads/*:refs/remotes/origin/*" && git fetch origin --unshallow`.
