////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////
//
/// Redistribution and use in source and binary forms, with or without
/// modification, are permitted provided that the following conditions are
/// met:
//
///     * Redistributions of source code must retain the above copyright
///       notice, this list of conditions and the following disclaimer.
///     * Redistributions in binary form must reproduce the above
///       copyright notice, this list of conditions and the following
///       disclaimer
///       in the documentation and/or other materials provided with the
///       distribution.
///     * Neither the name of Google Inc. nor the names of its
///       contributors may be used to endorse or promote products derived
///       from this software without specific prior written permission.
///
/// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
/// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
/// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
/// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
/// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
/// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
/// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
/// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
/// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
/// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
/// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
///
/// Author: Ray Sidney
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/strings/str_cat.h>

#include <source_location>

#include "basics/application-exit.h"
#include "basics/logger/log_level.h"
#include "basics/logger/log_time_format.h"

namespace sdb {
namespace app {
class AppServer;
}

class LogGroup;

class LogTopic final {
 public:
  // pseudo topic to address all log topics
  static inline constexpr std::string_view kAll = "all";

  explicit LogTopic(std::string_view name,
                    LogLevel level = LogLevel::DEFAULT) noexcept
    : _name{name}, _level{level} {}

  std::string_view GetName() const { return _name; }
  LogLevel GetLevel() const noexcept {
    return _level.load(std::memory_order_relaxed);
  }
  void SetLevel(LogLevel level) noexcept {
    _level.store(level, std::memory_order_relaxed);
  }

 private:
  std::string_view _name;
  std::atomic<LogLevel> _level;
};

struct Logger {
  // NOLINTBEGIN
  static LogTopic AGENCY;
  static LogTopic AGENCYCOMM;
  static LogTopic AGENCYSTORE;
  static LogTopic AQL;
  static LogTopic AUTHENTICATION;
  static LogTopic AUTHORIZATION;
  static LogTopic BACKUP;
  static LogTopic CLUSTER;
  static LogTopic COMMUNICATION;
  static LogTopic CONFIG;
  static LogTopic CRASH;
  static LogTopic DEVEL;
  static LogTopic DUMP;
  static LogTopic ENGINES;
  static LogTopic FIXME;
  static LogTopic FLUSH;
  static LogTopic GRAPHS;
  static LogTopic HEARTBEAT;
  static LogTopic HTTPCLIENT;
  static LogTopic MAINTENANCE;
  static LogTopic MEMORY;
  static LogTopic QUERIES;
  static LogTopic REPLICATION;
  static LogTopic REQUESTS;
  static LogTopic RESTORE;
  static LogTopic ROCKSDB;
  static LogTopic SECURITY;
  static LogTopic SSL;
  static LogTopic STARTUP;
  static LogTopic STATISTICS;
  static LogTopic SUPERVISION;
  static LogTopic SYSCALL;
  static LogTopic THREADS;
  static LogTopic TRANSACTIONS;
  static LogTopic TTL;
  static LogTopic SEARCH;
  static LogTopic IRESEARCH;
  static LogTopic FUERTE;
  // TODO(gnusi) static LogTopic VPACK;
#ifdef USE_V8
  static LogTopic V8;
#endif
  // NOLINTEND

  struct FIXED {
    explicit FIXED(double value, int precision = 6) noexcept
      : value(value), precision(precision) {}
    double value;
    int precision;
  };

  struct LINE {
    explicit LINE(int line) noexcept : line(line) {}
    int line;
  };

  struct FILE {
    explicit FILE(const char* file) noexcept : file(file) {}
    const char* file;
  };

  struct FUNCTION {
    explicit FUNCTION(const char* function) noexcept : function(function) {}
    const char* function;
  };

  struct LOGID {
    explicit LOGID(const char* logid) noexcept : logid(logid) {}
    const char* logid;
  };
};

namespace log {

inline constexpr std::string_view kLogThreadName = "Logging";

struct Message {
  Message(const Message&) = delete;
  Message& operator=(const Message&) = delete;

  Message(LogLevel level, std::string&& message, uint32_t offset,
          const LogTopic& topic, bool shrunk) noexcept
    : message{std::move(message)},
      topic{&topic},
      offset{offset},
      level{level},
      shrunk{shrunk} {}

  /// shrink log message to at most maxLength bytes (plus "..." appended)
  void shrink(size_t max_length);

  std::string message;
  const LogTopic* topic;
  uint32_t offset;  // where actual message starts (i.e. excluding prologue)
  const LogLevel level;
  bool shrunk;  // whether or not the log message was already shrunk
};

LogGroup& GetDefaultLogGroup() noexcept;
LogLevel GetLogLevel() noexcept;
void SetLogLevel(LogLevel) noexcept;
void SetLogLevel(std::string_view);

template<typename C>
void SetLogLevels(const C& levels) {
  for (const auto& level : levels) {
    SetLogLevel(level);
  }
}
void SetRole(char role);
void SetOutputPrefix(std::string_view);
void SetHostname(std::string_view);
void SetShowIds(bool);
void SetShowLineNumber(bool);
void SetShowRole(bool);
void SetShortenFilenames(bool);
void SetShowProcessIdentifier(bool);
void SetShowThreadIdentifier(bool);
void SetShowThreadName(bool);
void SetUseColor(bool);
bool GetUseColor();
void SetUseControlEscaped(bool);
void SetUseUnicodeEscaped(bool);
void SetEscaping();
bool GetUseControlEscaped();
bool GetUseUnicodeEscaped();
bool GetKeepRotate() noexcept;
bool GetUseLocalTime();
void SetTimeFormat(log_time_formats::TimeFormat);
void SetKeepLogrotate(bool);
void SetLogRequestParameters(bool);
bool GetLogRequestParameters();
void SetUseJson(bool);
bool IsActive() noexcept;
void Deactive() noexcept;
log_time_formats::TimeFormat GetTimeFormat();
void ClearCachedPid();  // can be called after fork()

void Log(const char* logid, const char* function, const char* file, int line,
         LogLevel level, const LogTopic& topic, std::string_view message);

inline bool IsEnabled(LogLevel level) noexcept {
  return level <= GetLogLevel();
}

inline bool IsEnabled(LogLevel level, const LogTopic& topic) noexcept {
  const auto topic_level = topic.GetLevel();
  return level <=
         ((topic_level == LogLevel::DEFAULT) ? GetLogLevel() : topic_level);
}

void Initialize();
void InitializeAsync(app::AppServer&, uint32_t max_queued_log_messages);
void Shutdown();
void Flush() noexcept;

std::vector<LogTopic*> GetTopics();
void SetLogLevel(std::string_view name, LogLevel level) noexcept;
LogTopic* FindTopic(std::string_view name) noexcept;

constexpr bool TranslateLogLevel(std::string_view l, bool is_general,
                                 LogLevel& level) noexcept {
  if (l == "fatal") {
    level = LogLevel::FATAL;
  } else if (l == "error" || l == "err") {
    level = LogLevel::ERR;
  } else if (l == "warning" || l == "warn") {
    level = LogLevel::WARN;
  } else if (l == "info") {
    level = LogLevel::INFO;
  } else if (l == "debug") {
    level = LogLevel::DEBUG;
  } else if (l == "trace") {
    level = LogLevel::TRACE;
  } else if (!is_general && (l.empty() || l == "default")) {
    level = LogLevel::DEFAULT;
  } else {
    return false;
  }

  return true;
}

constexpr std::string_view TranslateLogLevel(LogLevel level) noexcept {
  switch (level) {
    case LogLevel::ERR:
      return "ERROR";
    case LogLevel::WARN:
      return "WARNING";
    case LogLevel::INFO:
      return "INFO";
    case LogLevel::DEBUG:
      return "DEBUG";
    case LogLevel::TRACE:
      return "TRACE";
    case LogLevel::FATAL:
      return "FATAL";
    case LogLevel::DEFAULT:
      return "DEFAULT";
  }
}

namespace detail {

template<typename... Args>
void Log(std::source_location location, const char* id, LogLevel level,
         const LogTopic& topic, Args&&... args) {
  log::Log(id, location.function_name(), location.file_name(), location.line(),
           level, topic, absl::StrCat(std::forward<Args>(args)...));
}

}  // namespace detail
}  // namespace log
}  // namespace sdb

#define SDB_LOG_IF(id, level, topic, cond, ...)                           \
  if (::sdb::log::IsEnabled((::sdb::LogLevel::level), (topic)) && (cond)) \
  ::sdb::log::detail::Log(std::source_location::current(), (id),          \
                          (::sdb::LogLevel::level), (topic), __VA_ARGS__)

#define SDB_LOG(id, level, topic, ...) \
  SDB_LOG_IF(id, level, topic, true, __VA_ARGS__)

#define SDB_TRACE(id, topic, ...) SDB_LOG(id, TRACE, topic, __VA_ARGS__)
#define SDB_DEBUG(id, topic, ...) SDB_LOG(id, DEBUG, topic, __VA_ARGS__)
#define SDB_INFO(id, topic, ...) SDB_LOG(id, INFO, topic, __VA_ARGS__)
#define SDB_WARN(id, topic, ...) SDB_LOG(id, WARN, topic, __VA_ARGS__)
#define SDB_ERROR(id, topic, ...) SDB_LOG(id, ERR, topic, __VA_ARGS__)
#define SDB_FATAL(id, topic, ...)         \
  SDB_LOG(id, FATAL, topic, __VA_ARGS__); \
  sdb::FatalErrorExit()
#define SDB_FATAL_EXIT_CODE(id, topic, code, ...) \
  SDB_LOG(id, FATAL, topic, __VA_ARGS__);         \
  sdb::FatalErrorExitCode(code)

#define SDB_TRACE_IF(id, topic, cond, ...) \
  SDB_LOG_IF(id, TRACE, topic, cond, __VA_ARGS__)
#define SDB_DEBUG_IF(id, topic, cond, ...) \
  SDB_LOG_IF(id, DEBUG, topic, cond, __VA_ARGS__)
#define SDB_INFO_IF(id, topic, cond, ...) \
  SDB_LOG_IF(id, INFO, topic, cond, __VA_ARGS__)
#define SDB_WARN_IF(id, topic, cond, ...) \
  SDB_LOG_IF(id, WARN, topic, cond, __VA_ARGS__)
#define SDB_ERROR_IF(id, topic, cond, ...) \
  SDB_LOG_IF(id, ERR, topic, cond, __VA_ARGS__)
#define SDB_FATAL_IF(id, topic, cond, ...) \
  SDB_LOG_IF(id, FATAL, topic, cond, __VA_ARGS__)

#ifdef SDB_DEV
#define SDB_PRINT_LEVEL ERR
#else
#define SDB_PRINT_LEVEL TRACE
#endif

#define SDB_PRINT(...)                                               \
  SDB_LOG("xxxxx", SDB_PRINT_LEVEL, ::sdb::Logger::FIXME, "###### ", \
          __VA_ARGS__)

#define SDB_PRINT_IF(cond, ...)                                      \
  SDB_LOG_IF("xxxxx", SDB_PRINT_LEVEL, ::sdb::Logger::FIXME, (cond), \
             "###### ", __VA_ARGS__)
