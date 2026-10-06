---
name: update-dependency
description: Bump, add or remove a third_party dependency (a git submodule, usually a serenedb fork) - fork version branch, gitlink bump, CMake wiring, compile flags check, the tests to run. Use when updating a library under third_party/ or adding a new one. For the DuckDB-family forks follow tests/duckdb/README.md instead.
argument-hint: "<third_party/name> [<target tag or commit>]"
---

# Updating a third_party dependency

Dependencies are submodules under `third_party/`, mostly forks under `github.com/serenedb`, built from source by `third_party/CMakeLists.txt` with our compiler, flags and C++ standard. CONTRIBUTING.md "Third-party dependencies" is the policy; this is the procedure. Adding a new dependency needs a maintainer's agreement first.

DuckDB, its extensions (`third_party/duckdb_*`) and `database-connector` have their own update recipe and patchset rules: `tests/duckdb/README.md`.

## 1. Where things stand

```bash
git config -f .gitmodules --get submodule.third_party/<name>.url
git ls-tree HEAD third_party/<name>
git -C third_party/<name> fetch --unshallow --tags origin || git -C third_party/<name> fetch --tags origin
git -C third_party/<name> branch -r --contains HEAD
```

Submodules are cloned shallow; unshallow before reading history (CONTRIBUTING "Working with Submodules").

## 2. Prepare the fork

- Check whether upstream already fixed what you need before patching anything.
- Base the update on the upstream release tag, carry our existing patches onto it (list them with `git log <old upstream tag>..<old pin>`), and keep each patch a minimal, self-contained commit: every edit to upstream sources is paid again at the next update. Deleting a file we don't build beats editing it.
- The gitlink must end up on a fork version branch `vYYYY.MM.DD` (pre-commit `check-submodule-pointers` rejects feature-branch heads and pins older than main's). Open the fork PR against a version branch and bump the pointer after it merges.

## 3. Wire it in

- `third_party/CMakeLists.txt`: `sdb_update_module(${GIT_EXECUTABLE} "<name>" ${CMAKE_CURRENT_SOURCE_DIR} "<sentinel file>")`, options as `set(<OPTION> <value> CACHE <type> "" FORCE)` before `add_subdirectory`.
- The dependency must not set project policy: no own `-march`/`-mcpu`/`-mtune`/`-mno-*`, `CMAKE_CXX_STANDARD`, `CMAKE_BUILD_TYPE`, PIC or frame-pointer flags. Turn its options off or patch them out in the fork. Runtime CPU dispatch (AVX-512, SVE) is enabled instead of a fixed higher baseline.
- One copy of each library: when DuckDB or another dependency bundles its own copy, point it at ours.

## 4. Check

- Flags of every file of the dependency (from CONTRIBUTING.md):

  ```bash
  jq -r '.[] | select(.file | contains("third_party/<name>/")) | .command' build/compile_commands.json \
    | grep -oE -- '-std=[^ ]+|-m(arch|cpu|tune)=[^ ]+|-mno-[^ ]+' | grep -v frame-pointer | sort | uniq -c
  ```

  Expect one `-std=c++26` group and the baseline; only dispatched kernels may go above it.
- Build every target, not only `serened` (`ninja -C <build dir>`): tests, benchmarks and examples link the library too.
- Run the tests that exercise it; `scripts/ci/classify-changes.sh` (`SUITE_OF_DIR`) shows which suites CI picks for the directory.
- Removing a dependency: search the whole tree including other `third_party/` libraries before calling it unused.

## 5. Commit

- Fork commits stay in the fork. The superproject change is the gitlink bump plus the CMake and source adaptations; with `AUTO_UPDATE_MODULES=ON` a reconfigure checks the new gitlink out everywhere.
- PR title (see the `pr-description` skill): `chore:` for a plain bump or removal (`chore: remove minizip-ng dependency`), `fix:` or `perf:` when the bump is the fix or the speedup, saying what it fixes.
