# Audit punch list -- mbkkt/refactor cleanup

Temporary file; delete at end of this work.

## Source
- Main review workflow `wd7srafxa`: 138 agents, 151 raw findings, 106 verified.
- Init-order investigation: confirmed DuckDB can move to slot 0 with no feature dependency.

## Batches

### Batch A -- Real bugs (high severity)

- [x] **A1. Partial-start exception skips every stop()** -- `server/rest_server/serened.cpp:85-96`. If `general.start()` throws after `engine.start()` succeeded, none of ssl/db_path/scheduler/.../engine get stopped. Threads leak, RocksDB doesn't close cleanly. Fix: push stoppers (`std::vector<absl::Cleanup>` or scope_exit equivalent) after each successful start; iterate reverse on the way out, each in a try/log/continue.
- [x] **A2. UAF on `gManager` during DuckDB shutdown** -- `server/query/log_types.cpp:36`. `UninstallLogManagerSink` stores nullptr and `_db.reset()` happens immediately after. Concurrent in-flight `ShouldLog`/`Write` calls can deref freed memory. Fix: `std::atomic<std::shared_ptr<duckdb::Logger>>` via `GlobalLoggerReference()`, snapshot via load() at the call site.
- [x] **A3. `WBReader::UpdateMaxTick` is a stub** -- `server/rocksdb_engine_catalog/rocksdb_recovery_manager.cpp:223`. `_max_tick_found`/`_max_hlc_found` stay 0, so the `UpdateTickServer`/`NewTickHybridLogicalClock` calls are no-ops. Delete the dead pipeline; rely on `RocksDBSettingsManager::loadSettings` for tick recovery.
- [x] **A4. `signal_handling::Shutdown()` is empty** -- `server/general_server/signal_handling.cpp:162`. `CHangupHandler` dereferences `SchedulerFeature::gScheduler` unconditionally; after `_scheduler.reset()` a SIGHUP UAFs. Fix: `Shutdown()` installs `SIG_IGN` for SIGHUP before returning.
- [x] **A5. Signal handler uses non-AS-safe logger** -- `server/general_server/signal_handling.cpp:50`. `CExitHandler` does `SDB_INFO`/`SDB_FATAL` (mutex + non-AS-safe FatalErrorExit). Fix: drop `SDB_INFO` (wait-loop logs from non-handler context); second-signal `SDB_FATAL` -> `Log(FATAL, CRASH, ...)` (SignalSafeWrite) + `_exit(EXIT_FAILURE)`; guard `info->si_pid` against null.
- [x] **A6. `SignalSafeWrite` truncation off-by-one** -- `libs/basics/logger/logger.cpp:119`. When payload exactly fills 4KiB, trailing `'\n'` overwrites last payload byte. Fix `min` to `kBufCap - 1 - pos`.

### Batch B -- Structural win (high severity)

- [x] **B1. Move `DuckDBEngine::Initialize()` to slot 0 in RunServer** + `Shutdown()` to last. Per init-order investigation, DuckDB has zero feature dependencies. Eliminates `StderrFallback` + `atomic<Sink*>` shim. `logger.cpp` `Log()` becomes `if topic == CRASH -> signal_safe_write; else sink->write(...)`. Also remove `Initialize/Shutdown` calls from `PostgresFeature::ctor` and `PostgresFeature::stop`.
- [x] **B2. Collapse dual shutdown state** -- delete `lifecycle::gCtrlC` + `_abort_waiting` cv + `AppServer::isStopping` + `beginShutdown` + `_shutdown_condition` + `ProgressHandler` + `addReporter` + `_progress_reports` vector. Signal handler calls `lifecycle::BeginShutdown()` directly. `AppServer::wait()` polls `lifecycle::IsStopping()` every 100ms. Replace ProgressHandler+vector with `setStateHook(std::function<void(State)>)` (one reporter ever).

### Batch C -- Volume deletes (medium)

- [x] **C1. Dead Low-priority IIFE** -- `server/general_server/scheduler.cpp:208-229`. Three return paths all return false; lambda is dead. Plus `min_tick_for_replication` and `_metrics_wal_released_tick_replication` in `rocksdb_background_thread` -- replication-subsystem vestige.
- [x] **C2. Dead `registerPostRecoveryCallback` subsystem** -- zero callers. Delete method, empty `_pending_recovery_callbacks` vector, `recoveryDone()` that waits on zero futures, the `scheduler_feature.h`/yaclib includes.
- [x] **C3. ~12 dead feature accessors** -- `name()` on all 12 features, `EndpointFeature::httpEndpoints`, `SslServerFeature::chooseSslContext` + `_sni_server_index`, `dumpTLSData` + `SplitPem`/`DumpPem` + `_cafile_content`, `GeneralServerFeature::reloadTLS` + `GeneralServer::reloadTLS`, `EngineFeature::started()` + `_started`, `CatalogFeature::Cleanup`, `SchedulerFeature::maximalThreads`, `DatabasePathFeature::setDirectory`, `SearchEngine::failQueriesOnOutOfSync` + `_fail_queries_on_out_of_sync`, `setDefaultParallelism` SDB_GTEST block. Demote `_skip_wal_recovery` / `_search_execution_threads_limit` to locals in `start()`.
- [x] **C4. Delete `log::Initialize/Shutdown/Flush/IsActive/GetLogRequestParameters`** -- 14 call sites are leftover ceremony (`DuckDBEngine::Shutdown` already calls `UninstallLogManagerSink`). `GetLogRequestParameters` ternary in http/h2_comm_task collapses to `fullUrl()`. Also delete `InitialLogLevel()` getter.
- [x] **C5. `log_types.cpp` dedup** -- delete `ConfigureLogManagerDefaults` (DatabaseInstance ignores `config.options.log_config`; defaults re-applied in `InstallLogManagerSink`). Delete `CrashLogType` registration (CRASH short-circuits to SignalSafeWrite). Make `ShouldLog` truly noexcept (drop `std::string topic_str{topic}` -- use `topic.data()` since constants are string-literal-backed). Reference `::sdb::log::HTTP`/`::sdb::log::SSL` instead of duplicating string literals at log_types.cpp:157/179.
- [x] **C6. `bool start() -> void start()`** -- Scheduler / RocksDBSyncThread / RocksDBBackgroundThread always `return true`. Drop bogus "Returns true on success" comments and the SDB_FATAL-on-false guards.

### Batch D -- Small surface (medium)

- [ ] **D1. Deprecated `std::atomic_store(shared_ptr)`** -- C++26 removes the free-function overloads (we set `CXX_STANDARD 26`). Migrate `_handler_factory` (and 2 other sites in `inverted_index_shard.h`, `local_catalog.cpp`) to `std::atomic<std::shared_ptr<T>>`. Fix the current `memory_order_relaxed` -> acquire/release.
- [ ] **D2. `EngineFeature(SerenedServer&)` discards param** + `IoContext::_server` never read. Drop both; make all 12 features default-constructible; simplify `IoContext` ctors and drop the AppServer& parameter; drop explicit IoContext copy ctor (`std::atomic<unsigned> _clients` already makes it non-copyable; vector uses `emplace_back` into reserved capacity).
- [ ] **D3. Per-iter mutex thrash in jthread holders** -- `rocksdb_background_thread.cpp:112-115` takes the mutex 3x per iteration to read a bool. Collapse to one lock per iteration. In `scheduler.cpp:139-142` use `absl::MutexLock guard` instead of raw `Lock()/Unlock()`. Drop `joinable()` guard in scheduler.cpp:145-147.
- [ ] **D4. `SchedulerFeature` ctor over-clamps** -- 50 lines clamping members that aren't bound by any flag; `SDB_WARN` refs nonexistent flags. Reduce to ~4 lines.
- [ ] **D5. `EndpointFeature` double-clamp + dead state** -- `endpoint_feature.cpp:48-53` second `_backlog_size > SOMAXCONN` check is unreachable. Inline `buildEndpointLists()` into ctor. Demote `_endpoints`/`_reuse_address`/`_backlog_size` to ctor locals.
- [ ] **D6. `GeneralServerFeature::_servers` vector with 1 element** -- `buildServers()` always emplaces exactly one. Replace `std::vector<std::unique_ptr<rest::GeneralServer>>` with `std::unique_ptr<rest::GeneralServer>`. Drop buildServers; drop for-loops in start/stop.
- [ ] **D7. `FlushFeature::_stopped` is unreachable** -- strict LIFO means all `registerFlushSubscription` callers torn down before flush.stop(). Delete field, branch, assignment.
- [ ] **D8. Dead `#ifdef SDB_FAULT_INJECTION _failure_points`** in GeneralServerFeature -- vector never written; failure-point config runs through `config_variables.cpp`.
- [ ] **D9. `SerenedServer` alias shim** -- delete `serened.h`/`serened_single.h`/`serened_includes.h`. Use `app::AppServer` directly. Move AppServer ctor/dtor inline. Move `gInstance` to private. Drop dead `State::Uninitialized`/`State::Aborted` + magic_enum cases.
- [ ] **D10. lifecycle positional-args API** -- over-engineered. Replace `SetPositionalArgs(span)`/`PositionalArgs()` with `SetDataDirArg(string_view)`/`DataDirArg()`; size-check in parseOptions.

### Batch E -- Polish (low)

- [ ] **E1. Stale comments + naming** -- drop filepath references per project memory; rewrite SDB_DEV comment in scheduler.cpp to describe real invariant; delete stale "we use std::atomic_ref" comment; drop SerenedFeature::instance() phrase from serened.cpp; consistent magic_enum strings; optional PascalCase->snake_case once dead-method removals land.
- [ ] **E2. `thread_id.cpp:80-89`** -- drop `if (name != nullptr)` (callers pass literals); fix inverted "ThreadNameFetcher above" -> "below"; soften prctl async-signal-safe claim; drop `joinable()` guard in scheduler.cpp:145.
- [ ] **E3. Misc minor** -- drop `<span>` include after D10; drop `<atomic>` include if gCtrlC gone; drop unused absl includes from logger.h (`absl/algorithm/container.h`, `absl/functional/any_invocable.h`); add direct includes in ~33 dependent files.

## Done criteria

- All 25 items checked off, OR explicitly skipped with rationale.
- Build green, smoke green, pre-commit clean.
- This file deleted.

## Notes

- Item B1 supersedes audit item #13 (which was the conservative "move DuckDB out of pg ctor" version).
- After B2 lands, `lifecycle.h` no longer needs `<atomic>` for `gCtrlC` and `app_server.h` no longer needs `<absl/synchronization/mutex.h>`.
