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
// Minimal synchronous logger. Writes formatted lines to stderr. The
// SDB_TRACE/DEBUG/INFO/WARN/ERROR/FATAL macros take a topic *identifier*
// (e.g. GENERAL / STARTUP — see topic.h) plus absl::StrCat-style args.
// The macro prepends `::sdb::log::` to the topic name so call sites are
// short.

#pragma once

// Re-exported by transitive include from the previous (heavier) logger.h.
// Several files lean on these symbols without their own direct include;
// keep them here until each is patched.
#include <absl/algorithm/container.h>
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>

#include <string>
#include <string_view>
#include <vector>

#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/logger/log_level.h"
#include "basics/logger/topic.h"

namespace sdb::log {

// ---- runtime configuration ------------------------------------------------

LogLevel GetLogLevel() noexcept;
void SetLogLevel(LogLevel) noexcept;
// Accepts "info" or "topic=info"; "all=info" sets every topic.
void SetLogLevel(std::string_view);
void ResetLogLevels();
// Internal: called by SetLogLevels<>.
void SnapshotTopicDefaults();

// Initial-pass setter: applies one or more level definitions and snapshots
// each as the topic's default level so a later ResetLogLevels() returns
// to *this* set rather than the topic-default.
template<typename C>
void SetLogLevels(const C& defs) {
  for (const auto& def : defs) {
    SetLogLevel(def);
  }
  SnapshotTopicDefaults();
}

// Topic-level helpers.
LogLevel TopicLevel(std::string_view topic) noexcept;
void TopicSetLevel(std::string_view topic, LogLevel) noexcept;
std::vector<std::string_view> KnownTopics() noexcept;
LogLevel TranslateLogLevel(std::string_view name) noexcept;
std::string_view TranslateLogLevel(LogLevel level) noexcept;
std::string LogLevelString();

bool IsActive() noexcept;
void Initialize() noexcept;
void Shutdown() noexcept;
void Flush() noexcept;
void ClearCachedPid() noexcept;  // kept as no-op for source compat

bool GetLogRequestParameters() noexcept;
void SetLogRequestParameters(bool) noexcept;

// SET-setting name used by the SQL surface to mutate the log level at
// runtime. Set by Phase 9 (DuckDB SET registration) in a follow-up.
inline constexpr std::string_view kLogLevelVariable = "sdb_log_level";

// ---- entry point ----------------------------------------------------------

void Log(LogLevel level, std::string_view topic, std::string_view message);

inline bool IsEnabled(LogLevel level) noexcept {
  return level <= GetLogLevel();
}

inline bool IsEnabled(LogLevel level, std::string_view topic) noexcept {
  const auto topic_level = TopicLevel(topic);
  return level <= (topic_level == LogLevel::DEFAULT ? GetLogLevel()
                                                    : topic_level);
}

}  // namespace sdb::log

// ---- macros ---------------------------------------------------------------

#define SDB_LOG_INTERNAL(LEVEL_TAG, TOPIC, ...)                                \
  do {                                                                         \
    constexpr ::sdb::LogLevel kSdbLevel = ::sdb::LogLevel::LEVEL_TAG;          \
    if (::sdb::log::IsEnabled(kSdbLevel, ::sdb::log::TOPIC)) {                 \
      ::sdb::log::Log(kSdbLevel, ::sdb::log::TOPIC,                            \
                      ::absl::StrCat(__VA_ARGS__));                            \
    }                                                                          \
  } while (0)

#define SDB_LOG_INTERNAL_IF(LEVEL_TAG, TOPIC, COND, ...)                       \
  do {                                                                         \
    if ((COND)) {                                                              \
      SDB_LOG_INTERNAL(LEVEL_TAG, TOPIC, __VA_ARGS__);                         \
    }                                                                          \
  } while (0)

#define SDB_TRACE(TOPIC, ...) SDB_LOG_INTERNAL(TRACE, TOPIC, __VA_ARGS__)
#define SDB_DEBUG(TOPIC, ...) SDB_LOG_INTERNAL(DEB,   TOPIC, __VA_ARGS__)
#define SDB_INFO(TOPIC,  ...) SDB_LOG_INTERNAL(INFO,  TOPIC, __VA_ARGS__)
#define SDB_WARN(TOPIC,  ...) SDB_LOG_INTERNAL(WARN,  TOPIC, __VA_ARGS__)
#define SDB_ERROR(TOPIC, ...) SDB_LOG_INTERNAL(ERR,   TOPIC, __VA_ARGS__)

#define SDB_FATAL(TOPIC, ...)                                                  \
  do {                                                                         \
    SDB_LOG_INTERNAL(FATAL, TOPIC, __VA_ARGS__);                               \
    ::sdb::FatalErrorExit();                                                   \
  } while (0)

#define SDB_FATAL_EXIT_CODE(TOPIC, CODE, ...)                                  \
  do {                                                                         \
    SDB_LOG_INTERNAL(FATAL, TOPIC, __VA_ARGS__);                               \
    ::sdb::FatalErrorExitCode(CODE);                                           \
  } while (0)

// Generic (level supplied by caller). LEVEL_TAG must be one of TRACE/DEB/
// INFO/WARN/ERR/FATAL — the symbolic part of `LogLevel::LEVEL_TAG`.
#define SDB_LOG(LEVEL_TAG, TOPIC, ...) \
  SDB_LOG_INTERNAL(LEVEL_TAG, TOPIC, __VA_ARGS__)
#define SDB_LOG_IF(LEVEL_TAG, TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(LEVEL_TAG, TOPIC, COND, __VA_ARGS__)

#define SDB_TRACE_IF(TOPIC, COND, ...) SDB_LOG_INTERNAL_IF(TRACE, TOPIC, COND, __VA_ARGS__)
#define SDB_DEBUG_IF(TOPIC, COND, ...) SDB_LOG_INTERNAL_IF(DEB,   TOPIC, COND, __VA_ARGS__)
#define SDB_INFO_IF(TOPIC,  COND, ...) SDB_LOG_INTERNAL_IF(INFO,  TOPIC, COND, __VA_ARGS__)
#define SDB_WARN_IF(TOPIC,  COND, ...) SDB_LOG_INTERNAL_IF(WARN,  TOPIC, COND, __VA_ARGS__)
#define SDB_ERROR_IF(TOPIC, COND, ...) SDB_LOG_INTERNAL_IF(ERR,   TOPIC, COND, __VA_ARGS__)
#define SDB_FATAL_IF(TOPIC, COND, ...)                                         \
  do {                                                                         \
    if ((COND)) {                                                              \
      SDB_LOG_INTERNAL(FATAL, TOPIC, __VA_ARGS__);                             \
      ::sdb::FatalErrorExit();                                                 \
    }                                                                          \
  } while (0)

#ifdef SDB_DEV
#define SDB_PRINT_LEVEL ERR
#else
#define SDB_PRINT_LEVEL TRACE
#endif

#define SDB_PRINT(...)                                                         \
  SDB_LOG_INTERNAL(SDB_PRINT_LEVEL, GENERAL, "###### ", __VA_ARGS__)
#define SDB_PRINT_IF(COND, ...)                                                \
  SDB_LOG_INTERNAL_IF(SDB_PRINT_LEVEL, GENERAL, (COND), "###### ", __VA_ARGS__)
