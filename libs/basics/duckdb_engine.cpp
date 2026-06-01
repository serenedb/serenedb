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

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>

#include <duckdb/logging/log_manager.hpp>
#include <duckdb/logging/logger.hpp>

#include "basics/assert.h"
#include "basics/log.h"

ABSL_DECLARE_FLAG(std::string, log_storage);
ABSL_DECLARE_FLAG(std::string, log_level);

namespace sdb {
namespace {

duckdb::LogLevel ParseLogLevel(std::string_view raw) {
  std::string s{raw};
  absl::AsciiStrToLower(&s);
  if (s == "trace") {
    return duckdb::LogLevel::LOG_TRACE;
  }
  if (s == "debug") {
    return duckdb::LogLevel::LOG_DEBUG;
  }
  if (s == "info") {
    return duckdb::LogLevel::LOG_INFO;
  }
  if (s == "warn" || s == "warning") {
    return duckdb::LogLevel::LOG_WARNING;
  }
  if (s == "error") {
    return duckdb::LogLevel::LOG_ERROR;
  }
  if (s == "fatal") {
    return duckdb::LogLevel::LOG_FATAL;
  }
  // Unknown value: fall back to the duckdb default rather than aborting --
  // logging shouldn't be the reason a server can't boot.
  return duckdb::LogConfig::DEFAULT_LOG_LEVEL;
}

}  // namespace

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

  // Operators who want to query logs from SQL can flip storage at any time:
  //   SET enable_logging = true;
  //   SET logging_storage = 'memory';
  //   SELECT * FROM duckdb_logs();
  auto& manager = _db->instance->GetLogManager();
  duckdb::LogConfig cfg;
  cfg.enabled = true;
  cfg.storage = absl::GetFlag(FLAGS_log_storage);
  cfg.level = ParseLogLevel(absl::GetFlag(FLAGS_log_level));
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
