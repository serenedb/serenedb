---
paths:
  - "server/**"
  - "iresearch/**"
  - "examples/iresearch/**"
  - "tests/**/*.cpp"
  - "tests/**/*.h"
  - "tests/**/*.hpp"
  - "third_party/duckdb/src/**"
  - "third_party/duckdb/extension/**"
  - "third_party/duckdb_*/src/**"
  - "third_party/database-connector/src/**"
---

# C++ Code Style

Based on common sense, Google C++ style guide, and Abseil best practices. These rules apply to serenedb and iresearch code, and to the DuckDB family (the duckdb submodules, `database-connector`, `duckdb_clickhouse`) in DuckDB's own style: there DuckDB's formatting, naming, `template <class T>` and helpers (`D_ASSERT`, `make_uniq`, `StringUtil`, its exception types, logging and settings) stay, and our pre-commit C++ checks don't run, so nothing catches a slip.

Style issues shouldn't block PRs -- anything not caught automatically can be fixed later. This is a living document.

## Tools

Single supported toolchain: the versions in `CONTRIBUTING.md` "Build prerequisites". We use the latest C++ standard.

## Naming

The conventions are in [`.clang-tidy`](../../.clang-tidy) (`readability-identifier-naming`); clangd shows them, but nothing enforces them.

## Formatting

Handled by [`.clang-format`](../../.clang-format) and [pre-commit](../../.pre-commit-config.yaml). No style discussions in PRs.

## Include Ordering

Handled by [`.clang-format`](../../.clang-format).

## Headers

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Header_Files) with differences:

- `#pragma once` instead of include guards
- Forward declarations in the directory's `fwd.h` where one exists
- `.h` for headers in `server/` and `tests/server/`, `.hpp` in `iresearch/` and `tests/iresearch/`, either in `server/connector/` (pre-commit `check-cpp-extensions`); `.tpp` for template implementations, `.cpp` for sources
- Avoid pimpl (exception: abstracting over multiple library backends)
- Avoid duplicating directory name in filename
- `inline` only for linkage; `IRS_FORCE_INLINE` for optimization hints
- Templates/inline everything is bad -- binary size matters
- Avoid manual template instantiation in `.cpp` (switch-like dispatch is ok)

## Scoping

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Scoping):

- No `using namespace` (forbidden in headers)
- Write code inside the real namespace
- `inline namespace` for versioning only
- Namespace aliases, `using enum/class/struct` ok in sources (forbidden in headers)
- Single anonymous namespace over multiple `static` declarations
- `constexpr` over `const`
- `constinit` / magic static / `inline static` to avoid static init order issues
- Avoid code in global namespace

## Initialization

- Prefer braced init `{}` over `make_*` for pair/tuple (faster to compile)
- Prefer braced init over parenthesized constructors
- POD-like types: use designated initializers `{.foo = 1, .bar = 2,}`
- Default `operator==`/`<=>`/`=` and constructors when possible
- Trailing comma required for multi-line initializer lists
- Forbidden: `Type var{};` and `Type var = {};` -- just omit for default construction
- Prefer `auto` with factory functions: `auto x = MakeFoo()`
- `const` strongly recommended on methods, references, and pointees; on variables it's the author's call
- Prefer `const auto*` over plain `auto` for pointers

## Classes

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Classes):

- Trailing comma in enum/enum class
- Free functions over member functions for structs
- Structs over `std::pair`/`std::tuple`
- Structs: everything public. Classes: private members (except static/constexpr)
- Avoid friends
- Avoid public init functions -- do work in constructors
- Prefer explicit constructors

## Functions

Similar to [Google style](https://google.github.io/styleguide/cppguide.html#Functions):

- Leading return types; a trailing one (`auto f() -> T`) only where the leading form can't be written
- Lambda without args: `[] {}`
- Overloads are fine, but avoid ambiguous ones like `const T&` vs `std::shared_ptr<const T>&`
- Default args banned for virtual functions -- use overloads

## Comments

- Every file starts with the license block; copy it from a sibling. Pre-commit `check-disclaimer` rejects a file without it and, in a header, wants a blank line and `#pragma once` right after it. (The license block is the *only* place the banner style is allowed.)
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

## Error Handling

- PostgreSQL/frontend code: use `THROW_SQL_ERROR`, so the real SQLSTATE goes on the wire. In `server/`, pre-commit `check-no-raw-throw` rejects any other `throw` except a rethrow (`throw;`), `irs::SqlException` and `duckdb::NotImplementedException`
- Common/backend code outside `server/`: both `absl::Status` and `throw` are acceptable
- Consider performance: an `absl::Status` holding only a code doesn't allocate
- `SDB_ASSERT` and `SDB_ENSURE` are debug checks of invariants: they fire in dev builds (`SDB_DEV`: the `lldb`, `clangd` and sanitizer presets). `SDB_ENSURE` also throws an internal error in other builds, but nothing relies on that: a condition that can really fail gets real handling.
- `SDB_VERIFY` crashes in every build.
- Prefer an `SDB_ASSERT` to a comment about an invariant; reach for `SDB_ENSURE` or `SDB_VERIFY` only when the guarantee is genuinely hard to follow locally.
- The assert's expression is the documentation. Skip the message when it would just translate the expression into English (`SDB_ASSERT(i < n)` is enough). Add one only when the failure scenario isn't visible in the expression: a domain rule, an unusual comparison shape (e.g. `"running sum overflow"` for `a + b >= a`), or a design constraint the comparison enforces.

## Async

- Use C++20 coroutines (`co_await` / `co_return`) with `yaclib::Future` for async code
- Avoid raw threads and callbacks in database logic
- Sync primitives are for deep implementation details only
- Locks in new code: `absl::Mutex` (`duckdb::mutex` is the same type), with `ReaderLock` where a `std::shared_mutex` would go; wait with `Await`/`LockWhen`, an `absl::CondVar` only when really needed. Never `std::mutex`, `std::shared_mutex`, `std::condition_variable(_any)` or yield loops
- Work runs on the existing pools, never on a hand-rolled `std::thread` pool: query execution on DuckDB's `TaskExecutor`/`BaseExecutorTask`; blocking or latency-tolerant background work on `BackgroundScheduler` (`server/scheduler/background_scheduler.h`), whose retry loops back off with `Delay` and stop once `IsStopping()`; the io threads only do socket IO
- No new `thread_local`

## Logging

- `SDB_TRACE`, `SDB_DEBUG`, `SDB_INFO`, `SDB_WARN` and `SDB_ERROR(TOPIC, ...)` from `iresearch/utils/log.hpp` join their arguments with `absl::StrCat`; `SDB_FATAL` also exits. The `_IF` forms take a condition after the topic.

## Integer Types

- Prefer explicitly sized types: `int32_t`, `uint64_t`, `uint8_t` over bare `int`
- Size enums explicitly: `enum class Foo : uint8_t { ... }`

## [[nodiscard]]

- Apply `[[nodiscard]]` to types whose value must not be dropped: guards and awaitables (`Finally`, `AsioAwaitable`)
- Apply to methods where callers must check the result

## Templates

- Prefer `template + static_assert` over concepts when possible -- gives better errors and compiles faster
- Use C++20 concepts when `static_assert` would be awkward (e.g. constrained overload sets)
- Avoid SFINAE / `enable_if` in new code
- `template <typename T>`, never `template <class T>` (pre-commit `fix-template-typename` rewrites it)
- An `auto` parameter instead of a template parameter when the code doesn't need the type's name

## Library Preferences

- Never implement what already exists: look for it in abseil (`absl::c_*` algorithms, strings, containers, synchronization), `server/utils/`, `iresearch/utils/` and DuckDB, and use, extend or patch that instead of building a parallel copy. A hand-written loop that an `absl::c_*` algorithm expresses is a duplicate too.
- No trivial pass-through wrappers around a library call: call the library directly.
- `std::span<const T>` over `std::initializer_list<T>` in parameters
- `magic_enum` for enum names
- `absl::c_*` (`absl/algorithm/container.h`) over `std::ranges` algorithms and over `std::any_of(begin, end)`; pre-commit `check-banned-calls` rejects a `std::ranges` algorithm that has an `absl::c_*` form. A projection becomes a lambda (`absl::c_sort(v, [](const T& l, const T& r) { return l.key < r.key; })`), and a sub-range keeps the iterator form (`std::sort(first, mid, comp)`). `std::ranges` stays for what absl lacks (`unique`, `min`, `to`, views).
- Imperative loops over `std::views` pipelines
- Strings: `absl::StrCat`, `absl::StrAppend`, `absl::StrJoin`, `absl::StrSplit`
- `absl::Substitute` when one argument appears in several positions and no printf-like formatting is needed
- printf-like formatting: `absl::StrFormat`, `absl::StrAppendFormat`, `absl::StreamFormat`, `absl::FPrintF`, or `absl::SNPrintF` into a fixed buffer
- `fmt` only where none of those fits, as the replacement for `std::format`
- Never `printf`, `fprintf`, `snprintf` or `std::format` (pre-commit `check-banned-calls` rejects them); the one exception is the crash handler, which must not allocate
- Number parsing: `fast_float::from_chars`, checking its `ec`; `std::sto*`, `std::from_chars`, `strto*` and `ato*` are rejected by `check-banned-calls`
- Avoid streams API (`operator<<`/`>>`) in new code. See also `absl::StreamFormat`
- Implicit conversion to bool: prefer `if (auto x = something())` over `if (auto x = something(); x)`
- Nullptrs: [Google style](https://google.github.io/styleguide/cppguide.html#0_and_nullptr/NULL), default constructor is ok for smart pointers
- Pre-increment/pre-decrement: [Google style](https://google.github.io/styleguide/cppguide.html#Preincrement_and_Predecrement)
- Casting: [Google style](https://google.github.io/styleguide/cppguide.html#Casting)
- Avoid RTTI
- `noexcept`: see dedicated section below
- No `&&` references for trivially copyable types
- Don't misuse `std::forward` and `std::move`
- References over pointers when ownership doesn't matter

## Strings and containers

- `std::string_view` for strings owned elsewhere and `std::span` for arrays, almost everywhere; a `std::string` only where the value must outlive its source, and at a C API boundary that needs a terminating null.
- Hash containers: `irs::containers::FlatHashMap`, `FlatHashSet`, `NodeHashMap` or `NodeHashSet` with `absl::Hash`, never `std::unordered_*` or `std::hash` (pre-commit `check-banned-calls` rejects `std::unordered_*` in `server/` and `iresearch/`). `absl::btree_*` over `std::set`/`std::map` when appropriate.
- In the DuckDB family, `unordered_map` and `unordered_set` are `absl::node_hash_map` and `absl::node_hash_set` in our fork (references stay valid, as with the std ones), and `case_insensitive_map_t` and `case_insensitive_set_t` are built on them. The few `std::unordered_*` written out there are libc++'s.
- Look up with the `std::string_view` or `const char*` you have, never a temporary `std::string`: `options[name]`, `options.find(name)`, `options.contains(name)`, never `options[std::string{name}]`. The absl and `irs::containers` containers and DuckDB's above take it in `find`, `contains`, `count`, `operator[]` and `try_emplace`; `std::map` and `std::set` only in `find`, `count` and `contains` (our libc++ defaults them to `std::less<>`); libc++'s `std::unordered_*` in none.
- Never `find` followed by an insert of the same key: `try_emplace` or `operator[]` looks the key up once.
- Key a map by `std::string_view` when the strings outlive it.
- Don't add an explicit `std::string{...}` conversion until the code fails to compile without it.

## noexcept

- Destructors must be `noexcept` (implicit, but be explicit if non-trivial)
- Move constructors and move assignment must be `noexcept` (required for efficient container operations)
- Other functions: only mark `noexcept` when truly noexcept or required for correctness
- Don't add `noexcept` speculatively -- it's a contract that's hard to remove later

## Idioms

- Treat raw pointers, smart pointers, and `std::optional` uniformly via
  contextual `bool` and `operator*`. Applies everywhere a `bool` is
  expected -- `if` / ternary / `SDB_ASSERT` / `&&` / `||` / `return`, not
  just `if`:
  - `if (p)` / `if (!p)`, not `if (p != nullptr)`.
  - `if (opt)`, not `if (opt.has_value())`.
  - `*p` / `*opt`, not `opt.value()` (`.value()` adds a redundant throw
    once you've verified the optional is engaged).
- Don't add includes speculatively -- only when clangd or the compiler
  asks for them.
- Prefer `emplace`-like functions; `emplace_back` takes aggregate members positionally.
- A const/non-const accessor pair is one deducing-`this` template.
- Add a virtual function, hook, field or setting only together with code outside its own file that uses it; delete a setting once nothing reads it.
- A new cap or limit is a SET variable read through `SettingRef` (`server/query/config.h`), not a constant; a new SereneDB-only one is named `sdb_<name>`.
- Production headers carry no test-only accessors or helpers.
- SQL the server issues itself is built with DuckDB's C++ API (statements and expressions), not as text for the parser, at least on hot paths. SQL from clients is handled by the parser: never detect or rewrite it with text or regex matching; patch the parser or transformer instead.
- No time-based heuristics: never gate engine behaviour on measured durations or wall-clock freshness; use exact, structural signals.
- Never hand-edit generated files; change the source of truth and rerun the generator (see "When you change ..." below).

## Memory and Ownership

- `unique_ptr` by default for owned resources
- `shared_ptr` only when ownership is genuinely shared -- justify it
- No raw owning pointers in new code
- Use `make_unique` / `make_shared` -- never bare `new`/`delete`
- Prefer stack allocation and value types over heap allocation

## Performance

- Avoid every unnecessary copy and allocation, however small; in hot paths avoid allocations altogether
- Avoid virtual calls in hot paths (prevents inlining, which is the main cost)
- Large buffers should be heap-allocated separately, not inlined as arrays/members in objects (inflates object size, fitting poorly into allocator size classes)
- Prefer contiguous memory (vectors, arrays) over node-based containers (lists, maps)
- Measure before optimizing -- don't guess
- Measure on a quiet machine: check `uptime` and `ps -eo user,pcpu,comm --sort=-pcpu | head` first, never time while a build or test runs on the box, and discard numbers that overlapped one
- Binary size matters: excessive inlining/templates hurt icache and build times
- Validate performance claims with microbenchmarks under `tests/bench/micro/` (Google Benchmark). Register one with `add_bench(<name>)` in that directory's `CMakeLists.txt` -- `<name>.cpp` either registers `BENCHMARK`s or defines its own `Main` with `sdb::bench::AddMain` -- build with `ninja serenedb-bench-micro`, run it as `<build dir>/bin/serenedb-bench-micro <name> [--benchmark_filter=...]`. The same binary answers to `search-benchmark-game-build` and `search-benchmark-game-query`, the search benchmark game's tools.
- Measure and profile with the `perf` preset (`-O3` with frame pointers). `bench` is configured like the release packages and omits frame pointers, so `perf record -g` call graphs break there.
- When optimizing for a benchmark, don't tune for its workload at the expense of the use cases it doesn't measure.
- A microbench fits when the change is a few well-scoped functions. When the
  change is broader (a whole query path, an end-to-end pipeline, anything that
  doesn't sit neatly inside one fixture), drive a small standalone repro script
  through `perf stat` / `perf record` instead -- it locates the hot spot
  without forcing the change into a microbench shape that doesn't fit.

## Testing

- gtest framework: `TEST()` for standalone, `TEST_F()` for shared fixtures, `TEST_P()` for parameterized.
- Async tests use `yaclib::WaitGroup` for synchronization.
- A gtest goes beside its siblings and is named like them.
- Test names describe behavior, not implementation.

(For when each test type applies and where new tests go, see [Tests](testing.md).)

## When you change ...

Each of these has a follow-up step that nothing runs for you.

- **A new C++ file:** add it to its target's source list in the nearest `CMakeLists.txt`. `scripts/find_unused_sources.py` lists the files nothing compiles.
- **A setting:** it is defined in `server/query/config_variables.cpp` and documented in the table in `docs/configuration/overview.md`.
- **A serened command-line flag:** `python3 tests/drivers/python/cli_help.py override --bin <build dir>/bin/serened` rewrites the reference that `docs/configuration/cli.mdx` renders; the python driver suite fails until it matches.
- **What pg_catalog and information_schema support:** update `docs/compatibility/system-table-compatibility.md`, then `python3 scripts/generate_system_table_claims.py` regenerates the test that pins it.
- **Generated sources:** the word-break and case tables in `iresearch/` come from `scripts/generate_unicode_tables.py`, and the PostgreSQL views and functions in `server/pg/system_views.h` and `server/pg/system_functions.h` from `scripts/update_system_catalog.py`. The DuckDB family's are in [third-party.md](third-party.md).
- **Anything written to disk:** [Storage compatibility](storage.md).
