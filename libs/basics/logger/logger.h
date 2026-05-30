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
// Before the DuckDB instance is up, and after it has been torn down, log
// lines are written synchronously to stderr. Once `InstallSink()` is called
// (DuckDBEngine::Initialize), records flow through duckdb::LogManager and
// honour `enable_logging` / `logging_level` / `enabled_log_types` /
// `disabled_log_types` SET-options. The CRASH topic always bypasses the
// sink: signal-safe write(2) to stderr only.

#pragma once

// Re-exported by transitive include from the previous (heavier) logger.h.
// Several files lean on these symbols without their own direct include;
// keep them here until each is patched.
#include <absl/algorithm/container.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <string_view>

#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/logger/log_level.h"
#include "basics/logger/topic.h"

namespace sdb::log {

// ---- runtime configuration ------------------------------------------------

// Translate between string spelling and the LogLevel enum. Kept because the
// gtest entry-point still parses a `--log-level <name>` flag.
LogLevel TranslateLogLevel(std::string_view name) noexcept;
std::string_view TranslateLogLevel(LogLevel level) noexcept;

// Process-wide initial level. Used as the ShouldLog comparand BEFORE the
// DuckDB sink is installed (and after it is uninstalled at shutdown). After
// InstallSink() runs, ShouldLog defers to duckdb's MutableLogger.
void SetInitialLogLevel(LogLevel level) noexcept;
LogLevel InitialLogLevel() noexcept;

// Lifecycle. Kept as no-ops for source-compat with the rest of the codebase
// (rest_server/serened.cpp, application-exit.cpp, crash_handler.cpp,
// rocksdb_engine_catalog.cpp). All real work happens via InstallSink / the
// LogManager.
void Initialize() noexcept;
void Shutdown() noexcept;
void Flush() noexcept;
inline bool IsActive() noexcept { return true; }

// Always-on getter; no-one currently flips this off. Kept as a function so
// the HTTP/h2 call sites compile unchanged.
inline bool GetLogRequestParameters() noexcept { return true; }

// ---- sink installation (called by DuckDBEngine) --------------------------

struct Sink {
  // Return true if a record at `level`/`topic` should be emitted.
  bool (*should_log)(LogLevel level, std::string_view topic) noexcept;
  // Write the record. Must be thread-safe.
  void (*write)(LogLevel level, std::string_view topic,
                std::string_view message) noexcept;
};

// `sink` must point to storage with static lifetime; callers typically pass
// a pointer to a static const Sink. Pass nullptr to revert to the stderr
// fallback.
void InstallSink(const Sink* sink) noexcept;

// ---- entry point ----------------------------------------------------------

void Log(LogLevel level, std::string_view topic, std::string_view message);

bool IsEnabled(LogLevel level) noexcept;
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
