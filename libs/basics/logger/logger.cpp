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

#include "basics/assert.h"

namespace sdb::log {
namespace {

// Process-wide initial level. Used as the ShouldLog comparand before the
// DuckDB sink is installed (the gtest entrypoint sets this before calling
// log::Initialize() on the test rig). After InstallSink() runs, IsEnabled()
// defers to the sink's should_log.
std::atomic<LogLevel> gInitialLevel{LogLevel::INFO};

// Active sink. Set by InstallSink() (called from DuckDBEngine::Initialize)
// at the very top of RunServer, cleared by UninstallLogManagerSink() at the
// very bottom. The pointed-to Sink must outlive any thread that calls Log()
// through it -- callers are expected to pass a function-local static const
// Sink.
std::atomic<const Sink*> gSink{nullptr};

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

void InstallSink(const Sink* sink) noexcept {
  gSink.store(sink, std::memory_order_release);
}

bool IsEnabled(LogLevel level) noexcept {
  if (const auto* sink = gSink.load(std::memory_order_acquire); sink) {
    return sink->should_log(level, std::string_view{});
  }
  // Pre-sink window (between main() entry and DuckDBEngine::Initialize() in
  // RunServer). Compare against the initial level so SDB_FATAL during
  // parseOptions still produces the line; the CRASH topic short-circuits
  // to SignalSafeWrite regardless.
  return level <= gInitialLevel.load(std::memory_order_relaxed);
}

bool IsEnabled(LogLevel level, std::string_view topic) noexcept {
  if (const auto* sink = gSink.load(std::memory_order_acquire); sink) {
    return sink->should_log(level, topic);
  }
  return level <= gInitialLevel.load(std::memory_order_relaxed);
}

void Log(LogLevel level, std::string_view topic, std::string_view message) try {
  // CRASH topic ALWAYS bypasses the sink. Async-signal-safe path: no heap,
  // no mutex, no LogManager state. Today's CRASH callers (crash_handler.cpp)
  // are invoked from a signal handler.
  if (topic == CRASH) {
    SignalSafeWrite(level, topic, message);
    return;
  }
  // DuckDBEngine::Initialize() installs the sink at the top of RunServer
  // and UninstallLogManagerSink() removes it as the very last step before
  // main returns. Every non-CRASH SDB_* macro from a serened binary runs
  // inside that window. Gtest entry points that don't construct a
  // DuckDBEngine drop log lines silently.
  if (const auto* sink = gSink.load(std::memory_order_acquire); sink) {
    sink->write(level, topic, message);
  }
} catch (...) {
  // Logging itself must never throw.
}

}  // namespace sdb::log
