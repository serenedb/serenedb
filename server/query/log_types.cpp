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

#include <atomic>
#include <duckdb/logging/log_manager.hpp>
#include <duckdb/logging/log_type.hpp>
#include <duckdb/logging/logger.hpp>
#include <memory>
#include <shared_mutex>
#include <string>

#include "basics/logger/logger.h"

namespace sdb::query {
namespace {

// Cache the global Logger once at sink-install time and read it from every
// Log() call. The reader grabs a shared_ptr snapshot under a shared lock,
// then releases the lock before calling into duckdb -- so the Logger stays
// alive past _db.reset() until the snapshot itself is dropped. The writer
// (UninstallLogManagerSink) clears the slot under the exclusive lock, which
// drains in-flight readers. We can't use std::atomic<std::shared_ptr<T>>
// directly because libc++ does not implement P0718 yet (the primary
// std::atomic template asserts is_trivially_copyable). DuckDB uses its
// own shared_ptr type (see common/shared_ptr.hpp) which is what
// GlobalLoggerReference() returns.
std::shared_mutex gLoggerMutex;
duckdb::shared_ptr<duckdb::Logger> gLogger;

duckdb::shared_ptr<duckdb::Logger> LoadLogger() noexcept {
  std::shared_lock guard{gLoggerMutex};
  return gLogger;
}

duckdb::LogLevel ToDuckLevel(LogLevel level) noexcept {
  switch (level) {
    case LogLevel::FATAL:
      return duckdb::LogLevel::LOG_FATAL;
    case LogLevel::ERR:
      return duckdb::LogLevel::LOG_ERROR;
    case LogLevel::WARN:
      return duckdb::LogLevel::LOG_WARNING;
    case LogLevel::INFO:
      return duckdb::LogLevel::LOG_INFO;
    case LogLevel::DEB:
      return duckdb::LogLevel::LOG_DEBUG;
    case LogLevel::TRACE:
      return duckdb::LogLevel::LOG_TRACE;
    case LogLevel::DEFAULT:
      return duckdb::LogLevel::LOG_INFO;
  }
  return duckdb::LogLevel::LOG_INFO;
}

bool ShouldLog(LogLevel level, std::string_view topic) noexcept {
  auto logger = LoadLogger();
  if (!logger) {
    return false;
  }
  // Safe because every topic constant in topic.h is initialized from a
  // string literal, so its data() is NUL-terminated.
  return logger->ShouldLog(topic.data(), ToDuckLevel(level));
}

void Write(LogLevel level, std::string_view topic,
           std::string_view message) noexcept {
  auto logger = LoadLogger();
  if (!logger) {
    return;
  }
  try {
    std::string topic_str{topic};
    std::string msg_str{message};
    logger->WriteLog(topic_str.c_str(), ToDuckLevel(level), msg_str.c_str());
  } catch (...) {
    // Logging must never propagate.
  }
}

constexpr ::sdb::log::Sink kSink{
  &ShouldLog,
  &Write,
};

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
    manager.RegisterLogType(duckdb::make_uniq<duckdb::LogType>(t.name, t.level));
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
  cfg.storage = duckdb::LogConfig::STDOUT_STORAGE_NAME;
  manager.SetConfig(db, cfg);

  {
    std::unique_lock guard{gLoggerMutex};
    gLogger = manager.GlobalLoggerReference();
  }
  ::sdb::log::InstallSink(&kSink);
}

void UninstallLogManagerSink() noexcept {
  ::sdb::log::InstallSink(nullptr);
  // Exclusive lock drains in-flight readers; once it's acquired no reader
  // is holding the old shared_ptr from inside the mutex. Each in-flight
  // reader that already extracted its snapshot keeps the Logger alive
  // past _db.reset() until that snapshot itself is dropped.
  std::unique_lock guard{gLoggerMutex};
  gLogger.reset();
}

}  // namespace sdb::query
