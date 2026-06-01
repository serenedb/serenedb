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

#include "basics/duckdb_engine.h"

#include <duckdb/logging/log_manager.hpp>
#include <duckdb/logging/logger.hpp>

#include "basics/assert.h"
#include "basics/logger/logger.h"

namespace sdb {

DuckDBEngine& DuckDBEngine::Instance() {
  static DuckDBEngine gInstance;
  return gInstance;
}

void DuckDBEngine::Initialize(DBConfigMutator mutator) {
  SDB_ASSERT(!_db);
  duckdb::DBConfig config;
  // PG folds unquoted identifiers to lowercase.
  config.SetOptionByName("preserve_identifier_case", duckdb::Value{false});
  config.SetOptionByName("disable_database_invalidation", duckdb::Value{true});
  // Existing serenedb code (array_remove etc.) uses the single-arrow lambda
  // syntax (`x -> ...`) which duckdb now warns about by default. Keep it
  // enabled until callers migrate to the new `lambda x: ...` form.
  config.SetOptionByName("lambda_syntax", duckdb::Value{"ENABLE_SINGLE_ARROW"});

  // Pre-construct hook: server build installs the `serenedb` storage
  // extension + SET-config variables here. Tests / benches pass no mutator.
  mutator(config);

  _db = std::make_unique<duckdb::DuckDB>(nullptr, &config);

  // DatabaseInstance::Configure seeds the LogManager with LogConfig() which
  // has enabled=false; flip enabled on so SDB_* (and duckdb's own log sites)
  // fire by default. Everything else (level, mode, storage) keeps duckdb's
  // defaults. No type pre-registration -- duckdb accepts arbitrary topic
  // strings and gates them via cfg.level alone.
  auto& manager = _db->instance->GetLogManager();
  duckdb::LogConfig cfg;
  cfg.enabled = true;
  manager.SetConfig(*_db->instance, cfg);
  log::SetLogger(&manager.GlobalLogger());
}

void DuckDBEngine::Shutdown() {
  // Detach the logger pointer BEFORE destroying the DuckDB; any late log
  // line during teardown of duckdb internals would otherwise chase a freed
  // LogManager.
  log::SetLogger(nullptr);
  _db.reset();
}

duckdb::DatabaseInstance& DuckDBEngine::instance() {
  SDB_ASSERT(_db);
  return *_db->instance;
}

duckdb::unique_ptr<duckdb::Connection> DuckDBEngine::CreateConnection() {
  SDB_ASSERT(_db);
  return duckdb::make_uniq<duckdb::Connection>(*_db);
}

}  // namespace sdb
