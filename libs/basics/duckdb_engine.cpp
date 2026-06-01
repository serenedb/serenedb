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
  config.SetOptionByName("preserve_identifier_case", duckdb::Value{false});
  config.SetOptionByName("disable_database_invalidation", duckdb::Value{true});
  config.SetOptionByName("lambda_syntax", duckdb::Value{"ENABLE_SINGLE_ARROW"});

  mutator(config);

  _db = std::make_unique<duckdb::DuckDB>(nullptr, &config);

  // Default to stdout so Docker / systemd / k8s log drivers capture the
  // stream as soon as the process starts. Operators who want to query logs
  // from SQL can flip to memory at any time:
  //   SET enable_logging = true;
  //   SET logging_storage = 'memory';
  //   SELECT * FROM duckdb_logs();
  auto& manager = _db->instance->GetLogManager();
  duckdb::LogConfig cfg;
  cfg.enabled = true;
  cfg.storage = duckdb::LogConfig::STDOUT_STORAGE_NAME;
  manager.SetConfig(*_db->instance, cfg);
  log::SetLogger(&manager.GlobalLogger());
}

void DuckDBEngine::Shutdown() {
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
