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

#include <atomic>
#include <cstring>
#include <duckdb/logging/logger.hpp>
#include <duckdb/logging/logging.hpp>

#include "basics/assert.h"

namespace sdb::log {
namespace {

// Process-wide initial level. Used as the ShouldLog comparand before
// SetLogger() is called (the gtest entrypoint sets this on the test rig
// before running tests). After SetLogger() runs, IsEnabled() defers to
// duckdb::Logger::ShouldLog.
std::atomic<LogLevel> gInitialLevel{LogLevel::INFO};

// Active duckdb::Logger. Set by SetLogger() (called from
// DuckDBEngine::Initialize -> InstallLogManagerSink) at the very top of
// RunServer, cleared by UninstallLogManagerSink() at the very bottom. The
// pointee is owned by LogManager which lives inside DatabaseInstance; the
// DuckDBEngine::Shutdown() runs after every feature has joined its
// workers, so by the time SetLogger(nullptr) lands no live thread can be
// dereferencing the pointer.
std::atomic<duckdb::Logger*> gLogger{nullptr};

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

// Async-signal-safe stderr write used by the CRASH topic. No heap, no mutex,
// no fwrite buffering -- just write(2). Caps message length to a stack
// buffer; truncates the rest. Output format "[LEVEL] {topic} message\n" so
// crash log lines remain greppable.
void SignalSafeWrite(LogLevel level, std::string_view topic,
                     std::string_view message) noexcept {
  constexpr size_t kBufCap = 4096;
  char buf[kBufCap];
  size_t pos = 0;
  auto append = [&](std::string_view s) {
    // Reserve the last byte for the trailing newline so the append never
    // fills past `kBufCap - 1`.
    size_t n = std::min(s.size(), kBufCap - 1 - pos);
    std::memcpy(buf + pos, s.data(), n);
    pos += n;
  };
  append("[");
  append(LevelTag(level));
  append("] {");
  append(topic.empty() ? std::string_view{"crash"} : topic);
  append("} ");
  append(message);
  buf[pos++] = '\n';
  // write(2) is async-signal-safe (POSIX.1-2017 Section 2.4.3).
  ssize_t unused = ::write(STDERR_FILENO, buf, pos);
  (void)unused;
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

void SetInitialLogLevel(LogLevel level) noexcept {
  gInitialLevel.store(level, std::memory_order_relaxed);
}

void SetLogger(duckdb::Logger* logger) noexcept {
  gLogger.store(logger, std::memory_order_release);
}

bool IsEnabled(LogLevel level, std::string_view topic) noexcept {
  if (auto* logger = gLogger.load(std::memory_order_acquire); logger) {
    // Every topic constant in topic.h is initialized from a string literal
    // (.data() is NUL-terminated, see invariant in topic.h).
    return logger->ShouldLog(topic.data(), ToDuckLevel(level));
  }
  // Pre-Logger window (between main() entry and DuckDBEngine::Initialize()
  // in RunServer) plus gtest entrypoints that don't construct a
  // DuckDBEngine. Compare against the initial level so SDB_FATAL during
  // parseOptions still produces the line; the CRASH topic short-circuits
  // to SignalSafeWrite regardless.
  return level <= gInitialLevel.load(std::memory_order_relaxed);
}

void Log(LogLevel level, std::string_view topic,
         const std::string& message) try {
  // CRASH topic ALWAYS bypasses the Logger. Async-signal-safe path: no
  // heap, no mutex, no LogManager state. Today's CRASH callers
  // (crash_handler.cpp) are invoked from a signal handler.
  if (topic == CRASH) {
    SignalSafeWrite(level, topic, message);
    return;
  }
  if (auto* logger = gLogger.load(std::memory_order_acquire); logger) {
    // .data() is NUL-terminated per the topic.h invariant; .c_str() is
    // free on the rvalue std::string that absl::StrCat() produces.
    logger->WriteLog(topic.data(), ToDuckLevel(level), message.c_str());
  }
} catch (...) {
  // Logging itself must never throw.
}

void LogCrash(LogLevel level, std::string_view message) noexcept {
  SignalSafeWrite(level, CRASH, message);
}

}  // namespace sdb::log
