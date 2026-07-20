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
- `tests/server/<area>/...`, `tests/libs/<lib>/...` -- gtest unit tests; use for isolated C++ logic where a sqllogic test would be awkward (library classes / pure functions / hard-to-reproduce bugs).
- `tests/bench/micro/...` -- microbenchmarks for performance claims.
- `tests/postgres_scanner/`, `tests/avro/`, `tests/httpfs/` -- drivers that run the **vendored DuckDB extension** test suites via DuckDB's own `unittest` binary. Built only when configured with `-DSDB_BUILD_DUCKDB_UNITTESTS=ON`.

When a change needs a test:

- Bug fix: always, unless you can argue the bug is uncoverable. Crash / recovery bugs go under `tests/sqllogic/recovery/`.
- New feature / behaviour change: sqllogic test in the right subtree above. Add a unit test too if there's isolated C++ logic worth pinning.
- CMake-only changes: rely on CI.
- Doc-only changes live in a separate repo and don't apply here.

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
./build/bin/serenedb-tests_basics "--gtest_filter=*VPackLoadInspectorTest*"
./build/bin/serenedb-tests_connector "--gtest_filter=*DataSourceWithSearchTest*"
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

### Running vendored DuckDB extension tests

The `postgres_scanner`, `avro`, `httpfs` extensions ship with their own
sqllogic-style test suites under `third_party/duckdb_<name>/test/`. They
run through DuckDB's `unittest` binary, which is built by default
(opt out with `-DSDB_BUILD_DUCKDB_UNITTESTS=OFF` if you want to skip
its ~1GB output).

```bash
# postgres_scanner -- postgres fixture comes up via docker
./tests/postgres_scanner/run.sh
```

The serened-level postgres_scanner tests
(`tests/sqllogic/sdb/pg/duckdb_postgres/*_pgscan.test_slow`) ride the regular
sqllogic runner -- the `_pgscan.` filename suffix triggers
`launch_postgres()` in `tests/sqllogic/run.sh` automatically.

## Branching, commits, PRs

- **Branch from `main`**, one focused change per PR.
- **Branch name:** `<author>/<topic>` (e.g. `mbkkt/fix-view-indexes-recovery`). Topic is free-form.
- **Conventional commit prefix** in the PR title: `feat:`, `fix:`, `perf:` (most common), or one of `refactor:`, `chore:`, `docs:`, `test:`, `ci:`, `build:`, `style:`, `misc:`. Don't invent new ones -- if none fit, ask.
- **Squash-merge:** the PR title is the final commit subject and the PR description is the body. Branch-internal commit messages are discarded, so they can be anything.
  - Exception: if your branch has exactly one commit and you let GitHub open the PR for you, GitHub will pre-fill the PR title and description from that commit -- so in that case keep the commit message PR-ready.
- **Pre-commit hooks** run as a PR check. You don't have to install them locally; if you want to check before pushing, run `pre-commit run --all-files`.
- **CI must pass** and one maintainer must approve before merge.

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

## C++ Code Style

Based on common sense, Google C++ style guide, and Abseil best practices. These rules apply to serenedb and iresearch code.

Style issues shouldn't block PRs -- anything not caught automatically can be fixed later. This is a living document.

### Tools

Single supported toolchain: latest stable clang, CMake, VSCode. We use the latest C++ standard.

### Naming

Enforced by [`.clang-tidy`](.clang-tidy) and [pre-commit](.pre-commit-config.yaml).

### Formatting

Handled by [`.clang-format`](.clang-format) and [pre-commit](.pre-commit-config.yaml). No style discussions in PRs.

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

- Use `SDB_LOG(level, topic, ...)` macros from `basics/log.h`
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
- Validate performance claims with microbenchmarks under `tests/bench/micro/` (Google Benchmark). Register one with `add_bench(<name>)` in that directory's `CMakeLists.txt`, build with `ninja serenedb-bench-micro`, run from `build/bin/serenedb-bench-micro-<name>`.
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
