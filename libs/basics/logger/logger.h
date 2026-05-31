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
// take a topic *identifier* (see topic.h) plus absl::StrCat-style args.
// Once `SetLogger()` has been called (DuckDBEngine::Initialize) records flow
// through duckdb::Logger directly and honour `enable_logging` /
// `logging_level` / `enabled_log_types` / `disabled_log_types` SET-options.
// Before the DuckDB instance is up and after it has been torn down,
// non-CRASH records are silently dropped (apart from a level-only
// IsEnabled() that compares against the initial level so gtest-only
// binaries can gate on --log-level without installing a Logger). The CRASH
// topic always bypasses the Logger: signal-safe write(2) to stderr only.

#pragma once

#include <absl/strings/str_cat.h>

#include <string>
#include <string_view>

#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/logger/log_level.h"
#include "basics/logger/topic.h"

namespace duckdb {

class Logger;
}

namespace sdb::log {

// ---- runtime configuration ------------------------------------------------

// Translate between string spelling and the LogLevel enum. Kept because the
// gtest entry-point still parses a `--log-level <name>` flag.
LogLevel TranslateLogLevel(std::string_view name) noexcept;
std::string_view TranslateLogLevel(LogLevel level) noexcept;

// Process-wide initial level. Used by IsEnabled() BEFORE SetLogger() runs
// (and after it is reverted to nullptr at shutdown). Once a Logger is
// installed, IsEnabled() and Log() defer to duckdb::Logger.
void SetInitialLogLevel(LogLevel level) noexcept;

// ---- duckdb::Logger installation (called by DuckDBEngine) ----------------

// Install the duckdb::Logger that all SDB_* sites will dispatch through.
// `logger` must outlive every thread that can call Log() / IsEnabled();
// callers are expected to clear it back to nullptr BEFORE destroying the
// duckdb::DatabaseInstance that owns the Logger. The contract is upheld in
// RunServer: DuckDBEngine::Shutdown() runs after every feature's stop()
// has joined its workers, so no live thread can dereference the pointer
// once we clear it.
void SetLogger(duckdb::Logger* logger) noexcept;

// ---- entry point ----------------------------------------------------------

void Log(LogLevel level, std::string_view topic, const std::string& message);

// Async-signal-safe variant for the CRASH topic. write(2) to stderr only,
// no heap, no Logger lookup. Intended for signal-handler callers that
// already have a fixed-size stack buffer assembled.
void LogCrash(LogLevel level, std::string_view message) noexcept;

bool IsEnabled(LogLevel level, std::string_view topic) noexcept;

}  // namespace sdb::log

// ---- macros ---------------------------------------------------------------

#define SDB_LOG_INTERNAL(LEVEL_TAG, TOPIC, ...)                       \
  do {                                                                \
    constexpr ::sdb::LogLevel kSdbLevel = ::sdb::LogLevel::LEVEL_TAG; \
    if (::sdb::log::IsEnabled(kSdbLevel, ::sdb::log::TOPIC)) {        \
      ::sdb::log::Log(kSdbLevel, ::sdb::log::TOPIC,                   \
                      ::absl::StrCat(__VA_ARGS__));                   \
    }                                                                 \
  } while (0)

#define SDB_LOG_INTERNAL_IF(LEVEL_TAG, TOPIC, COND, ...) \
  do {                                                   \
    if ((COND)) {                                        \
      SDB_LOG_INTERNAL(LEVEL_TAG, TOPIC, __VA_ARGS__);   \
    }                                                    \
  } while (0)

#define SDB_TRACE(TOPIC, ...) SDB_LOG_INTERNAL(TRACE, TOPIC, __VA_ARGS__)
#define SDB_DEBUG(TOPIC, ...) SDB_LOG_INTERNAL(DEB, TOPIC, __VA_ARGS__)
#define SDB_INFO(TOPIC, ...) SDB_LOG_INTERNAL(INFO, TOPIC, __VA_ARGS__)
#define SDB_WARN(TOPIC, ...) SDB_LOG_INTERNAL(WARN, TOPIC, __VA_ARGS__)
#define SDB_ERROR(TOPIC, ...) SDB_LOG_INTERNAL(ERR, TOPIC, __VA_ARGS__)

#define SDB_FATAL(TOPIC, ...)                    \
  do {                                           \
    SDB_LOG_INTERNAL(FATAL, TOPIC, __VA_ARGS__); \
    ::sdb::FatalErrorExit();                     \
  } while (0)

#define SDB_FATAL_EXIT_CODE(TOPIC, CODE, ...)    \
  do {                                           \
    SDB_LOG_INTERNAL(FATAL, TOPIC, __VA_ARGS__); \
    ::sdb::FatalErrorExitCode(CODE);             \
  } while (0)

// Generic (level supplied by caller). LEVEL_TAG must be one of TRACE/DEB/
// INFO/WARN/ERR/FATAL -- the symbolic part of `LogLevel::LEVEL_TAG`.
#define SDB_LOG(LEVEL_TAG, TOPIC, ...) \
  SDB_LOG_INTERNAL(LEVEL_TAG, TOPIC, __VA_ARGS__)
#define SDB_LOG_IF(LEVEL_TAG, TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(LEVEL_TAG, TOPIC, COND, __VA_ARGS__)

#define SDB_TRACE_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(TRACE, TOPIC, COND, __VA_ARGS__)
#define SDB_DEBUG_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(DEB, TOPIC, COND, __VA_ARGS__)
#define SDB_INFO_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(INFO, TOPIC, COND, __VA_ARGS__)
#define SDB_WARN_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(WARN, TOPIC, COND, __VA_ARGS__)
#define SDB_ERROR_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(ERR, TOPIC, COND, __VA_ARGS__)
#define SDB_FATAL_IF(TOPIC, COND, ...)             \
  do {                                             \
    if ((COND)) {                                  \
      SDB_LOG_INTERNAL(FATAL, TOPIC, __VA_ARGS__); \
      ::sdb::FatalErrorExit();                     \
    }                                              \
  } while (0)

#ifdef SDB_DEV
#define SDB_PRINT_LEVEL ERR
#else
#define SDB_PRINT_LEVEL TRACE
#endif

#define SDB_PRINT(...) \
  SDB_LOG_INTERNAL(SDB_PRINT_LEVEL, GENERAL, "###### ", __VA_ARGS__)
#define SDB_PRINT_IF(COND, ...) \
  SDB_LOG_INTERNAL_IF(SDB_PRINT_LEVEL, GENERAL, (COND), "###### ", __VA_ARGS__)
