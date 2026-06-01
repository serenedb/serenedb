////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
//
// Thin shim over DuckDB's LogManager. SDB_TRACE/DEBUG/INFO/WARN/ERROR/FATAL
// take a topic *identifier* (see topic.h) plus absl::StrCat-style args and
// forward to duckdb::Logger::WriteLog via a single function-pointer hop.
// IsEnabled() honours the runtime `enable_logging` / `logging_level` /
// `enabled_log_types` / `disabled_log_types` SET-options.
//
// Contract: every binary that emits SDB_* MUST call
// DuckDBEngine::Initialize() before the first macro fires, and
// DuckDBEngine::Shutdown() only AFTER the last thread that could emit
// has joined. No atomic guard, no null-check on the hot path -- the
// pointer is set once at Initialize and cleared once at Shutdown.
//
// LogCrash() is the exception: an async-signal-safe entry point that
// goes straight to write(2) on stderr. Reachable from signal handlers
// (crash_handler / signal_handling / SDB_ASSERT) and therefore safe to
// invoke outside the DuckDB window.

#pragma once

#include <absl/strings/str_cat.h>

#include <duckdb/logging/logging.hpp>
#include <string>
#include <string_view>

#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/logger/topic.h"
#include "basics/shared.hpp"

namespace duckdb {

class Logger;

}  // namespace duckdb
namespace sdb::log {

void SetLogger(duckdb::Logger* logger) noexcept;

void Log(duckdb::LogLevel level, std::string_view topic,
         const std::string& message) noexcept;

IRS_NO_INLINE void LogCrash(std::string_view message) noexcept;

bool IsEnabled(duckdb::LogLevel level, std::string_view topic) noexcept;

}  // namespace sdb::log

#define SDB_LOG_INTERNAL(LEVEL, TOPIC, ...)                    \
  do {                                                         \
    constexpr ::duckdb::LogLevel kSdbLevel = (LEVEL);          \
    if (::sdb::log::IsEnabled(kSdbLevel, ::sdb::log::TOPIC)) { \
      ::sdb::log::Log(kSdbLevel, ::sdb::log::TOPIC,            \
                      ::absl::StrCat(__VA_ARGS__));            \
    }                                                          \
  } while (0)

#define SDB_LOG_INTERNAL_IF(LEVEL, TOPIC, COND, ...) \
  do {                                               \
    if ((COND)) {                                    \
      SDB_LOG_INTERNAL(LEVEL, TOPIC, __VA_ARGS__);   \
    }                                                \
  } while (0)

#define SDB_TRACE(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_TRACE, TOPIC, __VA_ARGS__)
#define SDB_DEBUG(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_DEBUG, TOPIC, __VA_ARGS__)
#define SDB_INFO(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_INFO, TOPIC, __VA_ARGS__)
#define SDB_WARN(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_WARNING, TOPIC, __VA_ARGS__)
#define SDB_ERROR(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_ERROR, TOPIC, __VA_ARGS__)

#define SDB_FATAL(TOPIC, ...)                                            \
  do {                                                                   \
    SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_FATAL, TOPIC, __VA_ARGS__); \
    ::sdb::FatalErrorExit();                                             \
  } while (0)

#define SDB_FATAL_EXIT_CODE(TOPIC, CODE, ...)                            \
  do {                                                                   \
    SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_FATAL, TOPIC, __VA_ARGS__); \
    ::sdb::FatalErrorExitCode(CODE);                                     \
  } while (0)

#define SDB_TRACE_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_TRACE, TOPIC, COND, __VA_ARGS__)
#define SDB_DEBUG_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_DEBUG, TOPIC, COND, __VA_ARGS__)
#define SDB_INFO_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_INFO, TOPIC, COND, __VA_ARGS__)
#define SDB_WARN_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_WARNING, TOPIC, COND, __VA_ARGS__)
#define SDB_ERROR_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_ERROR, TOPIC, COND, __VA_ARGS__)
#define SDB_FATAL_IF(TOPIC, COND, ...)                                     \
  do {                                                                     \
    if ((COND)) {                                                          \
      SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_FATAL, TOPIC, __VA_ARGS__); \
      ::sdb::FatalErrorExit();                                             \
    }                                                                      \
  } while (0)

#ifdef SDB_DEV
#define SDB_PRINT_LEVEL ::duckdb::LogLevel::LOG_ERROR
#else
#define SDB_PRINT_LEVEL ::duckdb::LogLevel::LOG_TRACE
#endif

#define SDB_PRINT(...) \
  SDB_LOG_INTERNAL(SDB_PRINT_LEVEL, GENERAL, "###### ", __VA_ARGS__)
#define SDB_PRINT_IF(COND, ...) \
  SDB_LOG_INTERNAL_IF(SDB_PRINT_LEVEL, GENERAL, (COND), "###### ", __VA_ARGS__)
