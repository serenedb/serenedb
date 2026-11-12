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

#include "iresearch/utils/duckdb_engine.hpp"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>

#include <duckdb/common/case_insensitive_map.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/logging/log_manager.hpp>
#include <duckdb/logging/logger.hpp>
#include <duckdb/logging/logging.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/log.hpp"

ABSL_DECLARE_FLAG(std::string, log_storage);
ABSL_DECLARE_FLAG(std::string, log_path);
ABSL_DECLARE_FLAG(std::string, log_level);

namespace irs {
namespace {

duckdb::LogLevel ParseLogLevel(std::string_view raw) {
  auto s = absl::AsciiStrToLower(raw);
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
  return duckdb::LogConfig::DEFAULT_LOG_LEVEL;
}

}  // namespace

DuckDBEngine& DuckDBEngine::Instance() {
  static DuckDBEngine gInstance;
  return gInstance;
}

void DuckDBEngine::Configure(duckdb::DBConfig& config) {
  config.SetOptionByName("preserve_identifier_case", duckdb::Value{false});
  config.SetOptionByName("disable_database_invalidation", duckdb::Value{true});
  config.SetOptionByName("explain_output_format", duckdb::Value{"PG"});
  config.SetOptionByName("table_function_identifier_conversion",
                         duckdb::Value{"DISABLE_IMPLICIT_STRING"});
  // Dependency edges are built from what the binder resolved
  // (CreateInfo::dependencies), so the collection must be on for every bind.
  config.SetOptionByName("enable_view_dependencies",
                         duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("enable_macro_dependencies",
                         duckdb::Value::BOOLEAN(true));
  // PostgreSQL's COPY ... TO writes no CSV header unless HEADER is given;
  // DuckDB's writer defaults it on.
  config.SetOptionByName("copy_csv_header_default",
                         duckdb::Value::BOOLEAN(false));
  // `/` between two integers truncates in PostgreSQL, where DuckDB produces a
  // DOUBLE. A client that wants DuckDB's reading can still SET this back per
  // session.
  config.SetOptionByName("integer_division", duckdb::Value::BOOLEAN(true));
  config.SetOptionByName("ieee_floating_point_ops",
                         duckdb::Value::BOOLEAN(false));
  config.SetOptionByName("show_behavior", duckdb::Value("SETTING"));
  config.SetOptionByName("preserve_insertion_order",
                         duckdb::Value::BOOLEAN(false));
  config.SetOptionByName("autoinstall_known_extensions",
                         duckdb::Value::BOOLEAN(false));
  config.SetOptionByName("autoload_known_extensions",
                         duckdb::Value::BOOLEAN(false));
}

void DuckDBEngine::Initialize(DBConfigMutator mutator) {
  SDB_ASSERT(!_instance);
  duckdb::DBConfig config;
  Configure(config);
  mutator(config);

  _instance = duckdb::DuckDB::CreateEmpty(&config)->instance;
  // Attached iceberg tables read CURRENT metadata: the default txn-start
  // time travel errors out any catalog enumeration that walks a table
  // created after the reader's transaction began.
  _instance->config.SetOptionByName("iceberg_use_metadata_log",
                                    duckdb::Value::BOOLEAN(false));

  auto& manager = _instance->GetLogManager();

  duckdb::LogConfig cfg;
  cfg.enabled = true;
  cfg.storage = absl::AsciiStrToLower(absl::GetFlag(FLAGS_log_storage));
  cfg.level = ParseLogLevel(absl::GetFlag(FLAGS_log_level));
  manager.SetConfig(*_instance, cfg);

  if (cfg.storage == duckdb::LogConfig::FILE_STORAGE_NAME) {
    duckdb::case_insensitive_map_t<duckdb::Value> storage_config;
    storage_config["path"] = duckdb::Value(absl::GetFlag(FLAGS_log_path));
    manager.UpdateLogStorageConfig(*_instance, storage_config);
  }

  log::SetLogger(&manager.GlobalLogger());
}

void DuckDBEngine::CloseDatabases() {
  if (!_instance) {
    return;
  }
  duckdb::DatabaseManager::Get(*_instance).ResetDatabases();
}

void DuckDBEngine::Shutdown() {
  log::SetLogger(nullptr);
  _instance.reset();
}

duckdb::DatabaseInstance& DuckDBEngine::instance() {
  SDB_ASSERT(_instance);
  return *_instance;
}

duckdb::unique_ptr<duckdb::Connection> DuckDBEngine::CreateConnection() {
  SDB_ASSERT(_instance);
  return duckdb::make_uniq<duckdb::Connection>(*_instance);
}

}  // namespace irs
