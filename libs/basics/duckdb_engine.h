////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <duckdb.hpp>
#include <memory>

namespace sdb {

// Process-wide duckdb::DuckDB owner. Owns the single duckdb::DuckDB that
// every SDB_* call dispatches through (after InstallLogManagerSink wires
// the LogManager) and that connector/catalog/pg code holds CreateConnection
// handles to.
//
// Lifecycle is bracketed by Initialize() / Shutdown() at the very edges of
// the process:
//   * serened main()           : Initialize before process-wide init, Shutdown
//   after RunServer
//   * gtest test_main          : Initialize before RUN_ALL_TESTS, Shutdown
//   after
//   * search-benchmark-game    : Initialize at top of main(), Shutdown before
//   return
// Within that window gLogger (libs/basics/logger) is non-null and the
// SDB_* hot path skips its null-check.
//
// The Initialize() overload takes an optional pre-construct mutator: the
// server build hands in `sdb::server::query::ConfigureServerDBConfig` to
// register the SereneDB storage extension and config variables before the
// duckdb::DuckDB ctor runs. Tests / benches that don't care leave it at
// the default no-op.
class DuckDBEngine {
 public:
  static DuckDBEngine& Instance();

  // Pre-construct hook: invoked AFTER the lite defaults have been applied
  // to `config` but BEFORE `duckdb::DuckDB` is constructed. The server
  // build uses this to install the `serenedb` storage extension and the
  // SET config variables that the storage extension reads at attach time.
  using DBConfigMutator = void (*)(duckdb::DBConfig&);
  static void NoopMutator(duckdb::DBConfig&) noexcept {}

  // 1) Apply lite defaults (preserve_identifier_case=false,
  //    disable_database_invalidation=true, lambda_syntax=ENABLE_SINGLE_ARROW).
  // 2) Run `mutator(config)`.
  // 3) Construct duckdb::DuckDB(nullptr, &config).
  // 4) InstallLogManagerSink() -- hand the GlobalLogger pointer to
  //    sdb::log so SDB_* macros start flowing through LogManager.
  void Initialize(DBConfigMutator mutator = &NoopMutator);

  // UninstallLogManagerSink() (clears gLogger) then drop the DuckDB.
  void Shutdown();

  duckdb::DatabaseInstance& instance();
  duckdb::unique_ptr<duckdb::Connection> CreateConnection();

 private:
  DuckDBEngine() = default;
  std::unique_ptr<duckdb::DuckDB> _db;
};

}  // namespace sdb
