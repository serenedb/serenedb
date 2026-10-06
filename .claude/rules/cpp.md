---
paths:
  - "server/**"
  - "iresearch/**"
  - "tests/**/*.{cpp,h,hpp,tpp}"
---

# SereneDB C++: review rules beyond CONTRIBUTING.md

These are rulings from the maintainers' reviews; CONTRIBUTING.md stays the base. In `server/` and `iresearch/`, pre-commit `check-banned-calls` rejects `std::unordered_*`, the printf family and `std::sto*`/`std::from_chars`/`strto*`/`ato*`, and `fix-template-typename` rewrites `template <class T>`.

## Containers, strings, formatting

- Our own maps and sets: `irs::containers::FlatHashMap`, `FlatHashSet`, `NodeHashMap` (`iresearch/utils/containers/`), never `std::unordered_*`. DuckDB's `case_insensitive_map_t` stays only where a DuckDB API hands it over (`CreateInfo::options`).
- Containers with heterogeneous lookup (absl, `irs::containers`): look up with the borrowed key (`map.find(sv)`, `set.contains(sv)`, `map.try_emplace(sv)`), never a temporary `std::string` key, never `find` followed by an insert of the same key. DuckDB's maps have no such lookup and need the `std::string`.
- Borrowed strings are `std::string_view` (parameters too). Avoid every unnecessary copy and allocation, however small.
- Formatted output: `absl::StrFormat`, `absl::StrCat`, `absl::FPrintF`, `absl::PrintF`, `absl::SNPrintF` into a fixed buffer. A signal handler, which must not allocate, is the exception.
- Number parsing: `fast_float::from_chars` (`third_party/fast_float`, integers with a base too); check its `ec`.

## Errors

- `THROW_SQL_ERROR(ERR_CODE(...), ERR_MSG(...))`, so the real SQLSTATE goes on the wire. Pre-commit `check-no-raw-throw` rejects any other `throw` in `server/` except a rethrow (`throw;`) and `duckdb::NotImplementedException`.
- Keep the message at the throw site; don't fold duplicated error text into a throwing helper.

## Concurrency

- `absl::Mutex` (`duckdb::mutex` is the same type); wait with `Await`/`LockWhen`, an `absl::CondVar` only when really needed. Never `std::mutex`, `std::condition_variable(_any)` or yield loops.
- Parallel work goes through DuckDB's `TaskExecutor`/`BaseExecutorTask`, never hand-rolled `std::thread` pools.
- No new `thread_local`.

## Working with DuckDB

- Reuse or patch DuckDB machinery; don't build a parallel copy of it. Host behaviour for fork catalog objects goes through `DuckCatalog::Make<X>Entry` plus a serenedb entry subclass; per-session behaviour through `ClientContextState`. New SQL commands are Create/Drop/Alter statements with their own operator, not PRAGMA callbacks.
- Pass `duckdb::ClientContext&` as the first parameter and look resources up where used. Never open extra `duckdb::Connection`s for internal work.
- Caps and limits are `sdb_` SET variables read through `SettingRef`, not constants.

## Shape

- `template <typename T>`, never `template <class T>`.
- `if (p)` / `if (!p)`, never `!= nullptr`.
- `emplace_back`, with aggregate members passed positionally.
- A const/non-const accessor pair is one deducing-`this` template.
- No virtual, hook, field or setting without a named consumer outside its own file; delete settings that stopped doing anything.
- Production headers carry no test-only accessors or helpers.
- Never hand-edit generated files; change the source of truth and rerun the generator.
