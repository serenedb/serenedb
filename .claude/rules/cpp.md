---
paths:
  - "server/**"
  - "iresearch/**"
  - "tests/**/*.{cpp,h,hpp,tpp}"
---

# SereneDB C++: review rules beyond CONTRIBUTING.md

These are rulings from the maintainers' reviews; CONTRIBUTING.md stays the base.

## Containers, strings, formatting

- Maps and sets: `containers::FlatHashMap`, `FlatHashSet`, `NodeHashMap` (`iresearch/utils/containers/`). Never `std::unordered_map` or DuckDB's `case_insensitive_map_t`/`unordered_map`.
- Look up with the borrowed key (`map.find(sv)`, `set.contains(sv)`, `map.try_emplace(sv)`); never build a temporary `std::string` key, never `find` followed by an insert of the same key.
- Borrowed strings are `std::string_view` (parameters too). Avoid every unnecessary copy and allocation, however small.
- Formatted output: `absl::StrFormat`, `absl::StrCat`, `absl::FPrintF`, `absl::PrintF`. Never `std::printf`, `std::fprintf`, `std::snprintf`.
- Number parsing: `fast_float::from_chars` (`third_party/fast_float`), never `std::sto*` or `std::from_chars`.

## Errors

- Anything that can reach a client: `THROW_SQL_ERROR(ERR_CODE(...), ERR_MSG(...))`, so the real SQLSTATE goes on the wire. DuckDB exception types only inside scalar-function and cast execution callbacks.
- Keep the message at the throw site; don't fold duplicated error text into a throwing helper.

## Concurrency

- `absl::Mutex` (`duckdb::mutex` is the same type); wait with `Await`/`LockWhen`, an `absl::CondVar` only when really needed. Never `std::mutex`, `std::condition_variable(_any)` or yield loops.
- Parallel work goes through DuckDB's `TaskExecutor`/`BaseExecutorTask`, never hand-rolled `std::thread` pools.
- No new `thread_local`.

## Working with DuckDB

- Reuse or patch DuckDB machinery; don't build a parallel copy of it. Host behaviour for fork catalog objects goes through `DuckCatalog::Make<X>Entry` plus a serenedb entry subclass; per-session behaviour through `ClientContextState`. New SQL commands are Create/Drop/Alter statements with their own operator, not PRAGMA callbacks.
- Pass `duckdb::ClientContext&` as the first parameter and look resources up where used. Never open extra `duckdb::Connection`s for internal work.
- Vectors: branch on FLAT and CONSTANT before falling back to `UnifiedVectorFormat`; no `Flatten` or per-cell `GetValue` on hot paths.
- Table functions: size the chunk with `output.SetChildCardinality(n)` before writing rows (`SetCardinality` is deprecated; rows past the vector size read back as NULL); at most `STANDARD_VECTOR_SIZE` rows per call (a macro, not `duckdb::STANDARD_VECTOR_SIZE`); a NULL string slot still holds `duckdb::string_t{}`.
- Caps and limits are `sdb_` SET variables read through `SettingRef`, not constants.

## Shape

- `template <typename T>`, never `template <class T>`.
- `if (p)` / `if (!p)`, never `!= nullptr`.
- `emplace_back`, with aggregate members passed positionally.
- A const/non-const accessor pair is one deducing-`this` template.
- No virtual, hook, field or setting without a named consumer outside its own file; delete settings that stopped doing anything.
- Production headers carry no test-only accessors or helpers.
- Never hand-edit generated files; change the source of truth and rerun the generator.
