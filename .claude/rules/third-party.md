---
paths:
  - "third_party/**"
  - ".gitmodules"
---

# Third-party dependencies

Dependencies are git submodules under `third_party/`, usually forks under `github.com/serenedb`, so build fixes can go into the fork. `third_party/CMakeLists.txt` builds them from source (`sdb_update_module` + `add_subdirectory`), so every file gets the same compiler, flags and standard library as our own code. Configure a dependency there with `set(<OPTION> <value> CACHE <type> "" FORCE)` before its `add_subdirectory`.

- **One ISA baseline.** `cmake/OptimizeForArchitecture.cmake` puts the baseline into `CMAKE_C_FLAGS` and `CMAKE_CXX_FLAGS`: Haswell features on amd64, `-march=armv8-a+crc+crypto` on arm64. A dependency must not add its own `-march`, `-mcpu`, `-mtune` or `-mno-*` to the whole library: a later `-march` replaces ours, and a lower one drops below it. Turn such options off (`ZXC_NATIVE_ARCH OFF`, zlib-ng's `WITH_NATIVE_INSTRUCTIONS OFF`, `DUCKDB_OPTIMIZATION_PROFILE NONE`) or fix the fork.
- **Runtime dispatch above it.** If a library can pick faster code for the CPU it runs on (AVX-512, SVE), enable that instead of building the whole library for one fixed level: OpenBLAS `DYNAMIC_ARCH` with a `DYNAMIC_LIST`, faiss `FAISS_OPT_LEVEL=dd`, zlib-ng `WITH_RUNTIME_CPU_DETECTION`. Flags above the baseline belong only on the kernels that the library reaches after a CPU check. For a header-only library that picks its backend at compile time, call the backends behind our own check, as `iresearch/analysis/text/sz/stringzilla.hpp` does for StringZilla.
- **One C++ standard.** C++ builds with `CMAKE_CXX_STANDARD` (`-std=c++26`); the LLVM runtimes are the only exception. Pass it through the library's own variable when it has one (`SIMDUTF_CXX_STANDARD ${CMAKE_CXX_STANDARD}`). Otherwise remove the library's own `CMAKE_CXX_STANDARD` in the fork, as was done for ada. A dependency must not set `CMAKE_BUILD_TYPE` either.
- **Check `compile_commands.json`** after adding or updating a dependency. Every file should carry the baseline and `-std=c++26`; only the dispatched kernels may go above the baseline:

  ```bash
  jq -r '.[] | select(.file | contains("third_party/<name>/")) | .command' build/compile_commands.json \
    | grep -oE -- '-std=[^ ]+|-m(arch|cpu|tune)=[^ ]+|-mno-[^ ]+' | grep -v frame-pointer | sort | uniq -c
  ```

## Adding a dependency

- Discuss with maintainers before adding new dependencies
- To add a new dependency: fork to `serenedb/`, add submodule, update `.gitmodules`

## Working with Submodules

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
