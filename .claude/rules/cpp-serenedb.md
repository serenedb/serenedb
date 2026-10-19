---
paths:
  - "server/**"
  - "iresearch/**"
  - "tests/**"
  - "examples/**"
---

# Our C++

Naming is in `.clang-tidy` (clangd shows it), formatting and include order in `.clang-format`. The pre-commit checks named below run on this code only. Style nits don't block a PR; the "never" rules do.

## Files

- Headers: `#pragma once`; `.h` in `server/` and `tests/server/`, `.hpp` in `iresearch/` and `tests/iresearch/`, either in `server/connector/` (`check-cpp-extensions`); `.tpp` for template implementations.
- Every file starts with the license block, copied from a sibling (`check-disclaimer`).
- Code lives in its real namespace, never the global one; no `using namespace`; namespace aliases and `using enum` only in sources; one anonymous namespace instead of `static` functions.
- `constexpr` over `const`; `constinit`, a function-local static or an `inline static` against static-init order problems.
- No pimpl, except over several library backends; no manual template instantiation in a `.cpp`; `inline` only for linkage (`IRS_FORCE_INLINE` for a hint). Templates and inlining cost icache and build time: use them where they pay.

## Types and functions

- Sized integer types (`int32_t`, `uint64_t`) and sized enums (`enum class Foo : uint8_t`).
- Structs over `std::pair`/`std::tuple`; structs fully public, classes with private members; free functions over member functions on structs; no friends; the constructor does the work, never an init function; explicit constructors.
- Overrides are `final`, not `override`.
- Leading return types, a trailing one only where the leading form can't be written; one-line methods in the header; no default arguments on virtual functions (they bind statically).
- `Type var;` for a class type with a default constructor, never `Type var{};`; scalars, pointers and trivial aggregates get an initializer. Braced init over parentheses and `make_pair`, designated initializers for aggregates, `emplace_back` with aggregate members positionally, trailing commas in multi-line initializer lists and enums.
- `template <typename T>`, never `class` (`fix-template-typename`); `static_assert` over concepts unless an overload set needs constraints; no SFINAE; an `auto` parameter when the type needs no name.
- A const/non-const accessor pair is one deducing-`this` template.
- `[[nodiscard]]` on guards, awaitables and results a caller must check; `noexcept` on destructors and moves, elsewhere only when truly non-throwing.

## Values and errors

- Pointers, smart pointers and `std::optional` test as booleans (`if (p)`, `if (!opt)`) and dereference with `*`; never `!= nullptr`, `has_value()` or `value()`.
- `std::optional` only where no value can mean "none": an empty vector or a null pointer usually can.
- `std::unique_ptr` for ownership, `std::shared_ptr` only for real shared ownership, `make_unique`/`make_shared`; no raw owning pointers; references where ownership doesn't matter; values on the stack over the heap; no `&&` for trivially copyable types.
- Errors are `THROW_SQL_ERROR` (`check-no-raw-throw` rejects any other `throw` in `server/` except a rethrow, `irs::SqlException` and `duckdb::NotImplementedException`).
- Cleanup on every way out is an `absl::Cleanup` declared before the call, never `catch (...) { Clean(); throw; }`.
- `SDB_ASSERT(cond)`, without a message, states an invariant, also where a comment would; `SDB_ENSURE`/`SDB_VERIFY` only for guarantees that are hard to see locally. A condition that can really fail gets real handling.

## Libraries

- Never reimplement what abseil, `server/utils/`, `iresearch/utils/` or DuckDB has: use, extend or patch it. No pass-through wrappers around a library call.
- Algorithms: `absl::c_*` over `std::ranges` and iterator pairs (`check-banned-calls`); a projection becomes a lambda; `std::ranges` only for what absl lacks (`unique`, `min`, `to`, views). Loops over `std::views` pipelines.
- Strings: `absl::StrCat`/`StrAppend`/`StrJoin`/`StrSplit`, `absl::Substitute` for repeated arguments, `absl::StrFormat`/`FPrintF`/`SNPrintF` for printf-like formatting, `fmt` only where none fits. Never the printf family or `std::format` (`check-banned-calls`; the crash handler's `snprintf` is the exception). No streams.
- Numbers: `fast_float::from_chars`, checking `ec`; never `std::sto*`, `std::from_chars`, `strto*`, `ato*` (`check-banned-calls`).
- `magic_enum` for enum names; `std::span<const T>` over `std::initializer_list<T>` parameters; no RTTI.

## Strings and containers

- A borrowed string is a `std::string_view` (an array a `std::span`); own a `std::string` only when it outlives its source or a C API needs the terminating null.
- Never build a temporary key: absl containers take the `string_view` you have in `find`, `contains`, `operator[]`, `at`, `try_emplace`, `insert_or_assign` and `erase` (`std::map`/`std::set`: [cpp.md](cpp.md)).
- One lookup per key: `try_emplace` or `operator[]`, never `find` and then insert.
- Hash containers are absl's (`irs::containers::FlatHashMap`/`FlatHashSet`, `NodeHashMap` when references must survive inserts), never `std::unordered_*` (`check-banned-calls`) or `std::hash`; `absl::btree_*` over `std::map`/`std::set` unless iterators must survive changes.

## Concurrency

- Async code is C++20 coroutines over `yaclib::Future`; no raw threads or callbacks in database logic.
- Locks are `absl::Mutex` (`ReaderLock` for shared access), waiting with `Await`/`LockWhen`; never `std::mutex`, `std::shared_mutex`, `std::condition_variable(_any)` or yield loops.
- Work runs on the existing pools: query work on DuckDB's task scheduler, blocking or background work on `BackgroundScheduler`; the io threads only do socket IO. No new `thread_local`.
- A race only TSan can see (benign by design) uses `duckdb::StoreRacy`/`LoadRacy`/`FillRacy`, atomics only under TSan; a real race gets real atomics with the right ordering.

## Server code

- SQL the server issues itself is built with DuckDB's C++ API, not as text. Client SQL is the parser's job: never match or rewrite it as text; patch the parser or transformer.
- Internal work uses the `ClientContext` already in play, passed as the first parameter; never a new `duckdb::Connection`.
- A new cap or limit is a `SET` variable read through the `Get*Setting` functions of `query/config.h`, named `sdb_<name>` when SereneDB-only.
- Production headers carry no test-only accessors.

## Tests and benchmarks

- gtest: `TEST`, `TEST_F` for shared fixtures, `TEST_P` for parameterized; beside its siblings and named like them, for the behaviour it checks; async tests sync with `yaclib::WaitGroup`.
- Microbenchmarks live in `tests/bench/micro/`, registered with `add_bench(<name>)` and run as `serenedb-bench-micro <name>`.

## When you change ...

- **A new C++ file:** add it to its target in the nearest `CMakeLists.txt` (`scripts/find_unused_sources.py` lists the files nothing builds).
- **A setting:** define it in `server/query/config_variables.cpp` and document it in the table in `docs/configuration/overview.md`.
- **A serened flag:** `python3 tests/drivers/python/cli_help.py override --bin <build>/bin/serened` regenerates the CLI reference the docs render.
- **What pg_catalog and information_schema support:** update `docs/compatibility/system-table-compatibility.md`, then run `python3 scripts/generate_system_table_claims.py`.
- **Generated tables:** the unicode tables come from `scripts/generate_unicode_tables.py`, `server/pg/system_views.h` and `system_functions.h` from `scripts/update_system_catalog.py`.
- **Anything written to disk:** [storage.md](storage.md).
