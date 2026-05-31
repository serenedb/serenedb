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

#include "query/log_types.h"

#include <duckdb/logging/log_manager.hpp>
#include <duckdb/logging/log_type.hpp>
#include <duckdb/logging/logger.hpp>
#include <string>

#include "basics/logger/logger.h"

namespace sdb::query {
namespace {

// ---- SereneDB-specific log types ---------------------------------------
// Each maps 1:1 to a topic in topic.h. NAME values are matched
// case-sensitively by duckdb's enabled/disabled_log_types sets, so they must
// agree with the PascalCase strings in topic.h.

struct SdbLogType {
  const char* name;
  duckdb::LogLevel level;
};

constexpr SdbLogType kSdbLogTypes[] = {
  {"Startup", duckdb::LogLevel::LOG_INFO},
  {"SSL", duckdb::LogLevel::LOG_WARNING},
  {"Storage", duckdb::LogLevel::LOG_INFO},
  {"Search", duckdb::LogLevel::LOG_INFO},
  {"IResearch", duckdb::LogLevel::LOG_INFO},
};

}  // namespace

void InstallLogManagerSink(duckdb::DatabaseInstance& db) {
  auto& manager = db.GetLogManager();
  // HTTPLogType comes from duckdb's built-in default registration -- don't
  // re-register it (LogManager::RegisterLogType throws on collision).
  // The CRASH topic short-circuits to SignalSafeWrite in logger.cpp and
  // never reaches LogManager, so no CrashLogType registration is needed.
  for (const auto& t : kSdbLogTypes) {
    manager.RegisterLogType(
      duckdb::make_uniq<duckdb::LogType>(t.name, t.level));
  }

  // DatabaseInstance::Configure ignores config.options.log_config and seeds
  // the LogManager with LogConfig() (disabled). Apply our serenedb defaults
  // explicitly here so logging is on out of the box.
  duckdb::LogConfig cfg;
  cfg.enabled = true;
  cfg.level = duckdb::LogLevel::LOG_INFO;
  cfg.mode = duckdb::LogMode::DISABLE_SELECTED;
  cfg.disabled_log_types = {std::string{::sdb::log::HTTP},
                            std::string{::sdb::log::SSL}};
  // In-memory storage so duckdb_logs() (and our sdb_log catalog view that
  // wraps it) can scan log entries from SQL. Stdout storage can't be scanned
  // -- entries fly straight to the terminal and are gone -- which breaks the
  // sdb_log smoke test that queries the startup banner.
  cfg.storage = duckdb::LogConfig::IN_MEMORY_STORAGE_NAME;
  manager.SetConfig(db, cfg);

  // Hand the GlobalLogger over to sdb::log as a raw pointer. The Logger is
  // owned by LogManager (inside DatabaseInstance); DuckDBEngine::Shutdown()
  // runs LAST in RunServer -- after every feature has joined its workers --
  // so by the time UninstallLogManagerSink() clears the slot no thread can
  // be dereferencing the pointer.
  ::sdb::log::SetLogger(&manager.GlobalLogger());
}

void UninstallLogManagerSink() noexcept { ::sdb::log::SetLogger(nullptr); }

}  // namespace sdb::query
