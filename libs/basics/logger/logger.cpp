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

#include "basics/logger/logger.h"

#include <absl/strings/ascii.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <duckdb/logging/logger.hpp>
#include <duckdb/logging/logging.hpp>

namespace sdb::log {
namespace {

// Active duckdb::Logger. Set ONCE by SetLogger() from
// DuckDBEngine::Initialize -> InstallLogManagerSink at the very top of
// the process, cleared ONCE by UninstallLogManagerSink() at the very
// bottom. Both writes are single-threaded -- no atomic / acquire-release.
//
// Contract (see logger.h): every binary that uses SDB_* must call
// DuckDBEngine::Initialize() before any SDB_* macro fires and
// DuckDBEngine::Shutdown() only after every thread that could log has
// joined. Within that window gLogger is non-null; outside it the
// dereference in Log() / IsEnabled() is UB -- by design.
duckdb::Logger* gLogger = nullptr;

constexpr std::string_view LevelTag(LogLevel l) noexcept {
  switch (l) {
    case LogLevel::FATAL:
      return "FATAL";
    case LogLevel::ERR:
      return "ERROR";
    case LogLevel::WARN:
      return "WARNING";
    case LogLevel::INFO:
      return "INFO";
    case LogLevel::DEB:
      return "DEBUG";
    case LogLevel::TRACE:
      return "TRACE";
    case LogLevel::DEFAULT:
      return "DEFAULT";
  }
  return "?";
}

constexpr duckdb::LogLevel ToDuckLevel(LogLevel level) noexcept {
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

}  // namespace

LogLevel TranslateLogLevel(std::string_view name) noexcept {
  auto lower = absl::AsciiStrToLower(name);
  if (lower == "trace") {
    return LogLevel::TRACE;
  }
  if (lower == "debug") {
    return LogLevel::DEB;
  }
  if (lower == "info") {
    return LogLevel::INFO;
  }
  if (lower == "warning" || lower == "warn") {
    return LogLevel::WARN;
  }
  if (lower == "error" || lower == "err") {
    return LogLevel::ERR;
  }
  if (lower == "fatal") {
    return LogLevel::FATAL;
  }
  if (lower == "default") {
    return LogLevel::DEFAULT;
  }
  return LogLevel::INFO;
}

std::string_view TranslateLogLevel(LogLevel level) noexcept {
  return LevelTag(level);
}

void SetLogger(duckdb::Logger* logger) noexcept { gLogger = logger; }

bool IsEnabled(LogLevel level, std::string_view topic) noexcept {
  // Every topic constant in topic.h is initialized from a string literal
  // (.data() is NUL-terminated, see invariant in topic.h).
  return gLogger->ShouldLog(topic.data(), ToDuckLevel(level));
}

void Log(LogLevel level, std::string_view topic,
         const std::string& message) noexcept try {
  // .data() is NUL-terminated per the topic.h invariant; .c_str() is
  // free on the rvalue std::string that absl::StrCat() produces.
  gLogger->WriteLog(topic.data(), ToDuckLevel(level), message.c_str());
} catch (...) {
  // duckdb's writer may throw on backing-store errors. Logging itself
  // must never propagate.
}

void LogCrash(LogLevel level, std::string_view message) noexcept {
  // Async-signal-safe path: stack buffer + write(2). No heap, no mutex,
  // no LogManager lookup. Truncates at 4KiB minus the trailing newline.
  constexpr size_t kBufCap = 4096;
  char buf[kBufCap];
  size_t pos = 0;
  auto append = [&](std::string_view s) noexcept {
    // Reserve the last byte for the trailing newline so the append never
    // fills past `kBufCap - 1`.
    size_t n = std::min(s.size(), kBufCap - 1 - pos);
    std::memcpy(buf + pos, s.data(), n);
    pos += n;
  };
  append("[");
  append(LevelTag(level));
  append("] ");
  append(message);
  buf[pos++] = '\n';
  // write(2) is async-signal-safe (POSIX.1-2017 Section 2.4.3).
  ssize_t unused = ::write(STDERR_FILENO, buf, pos);
  (void)unused;
}

}  // namespace sdb::log
