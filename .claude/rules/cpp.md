---
paths:
  - "server/**"
  - "iresearch/**"
  - "tests/**/*.{cpp,h,hpp,tpp}"
---

# SereneDB C++: review rules beyond CONTRIBUTING.md

Read CONTRIBUTING.md "C++ Code Style" first; these are the maintainers' review rulings it doesn't cover.

## Containers and copies

- Containers with heterogeneous lookup (absl, `irs::containers`): look up with the borrowed key (`map.find(sv)`, `set.contains(sv)`, `map.try_emplace(sv)`), never a temporary `std::string` key, never `find` followed by an insert of the same key. DuckDB's maps have no such lookup and need the `std::string`.
- Avoid every unnecessary copy and allocation, however small.

## Concurrency

- `absl::Mutex` (`duckdb::mutex` is the same type); wait with `Await`/`LockWhen`, an `absl::CondVar` only when really needed. Never `std::mutex`, `std::condition_variable(_any)` or yield loops.
- Work runs on the existing pools, never on a hand-rolled `std::thread` pool: query execution on DuckDB's `TaskExecutor`/`BaseExecutorTask`; blocking or latency-tolerant background work on `BackgroundScheduler` (`server/scheduler/background_scheduler.h`), whose retry loops back off with `Delay` and stop once `IsStopping()`; the io threads only do socket IO.
- No new `thread_local`.

## Working with DuckDB

- Reuse or patch DuckDB machinery; don't build a parallel copy of it.
- Caps and limits are `sdb_` SET variables read through `SettingRef`, not constants.

## Shape

- `template <typename T>`, never `template <class T>`.
- `if (p)` / `if (!p)`, never `!= nullptr`.
- `emplace_back`, with aggregate members passed positionally.
- A const/non-const accessor pair is one deducing-`this` template.
- No virtual, hook, field or setting without a named consumer outside its own file; delete settings that stopped doing anything.
- Production headers carry no test-only accessors or helpers.
- Never hand-edit generated files; change the source of truth and rerun the generator.
