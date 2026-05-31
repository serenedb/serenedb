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
#include <absl/synchronization/mutex.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <string>

namespace sdb::log {
namespace {

// Process-wide pre-sink level. Lookup in IsEnabled() before InstallSink()
// has run.
std::atomic<LogLevel> gInitialLevel{LogLevel::INFO};

// Active sink (nullptr -> stderr fallback). Set by InstallSink(). The
// pointed-to Sink must outlive any thread that calls Log() through it --
// callers are expected to pass a function-local static const Sink.
std::atomic<const Sink*> gSink{nullptr};

// Serializes stderr writes for the fallback path. NOT touched on the CRASH
// signal-safe path -- there we do a single write(2).
absl::Mutex gStderrMutex;

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

void WriteIsoTimestamp(std::string& out) {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto time_t_s = system_clock::to_time_t(now);
  auto us =
    duration_cast<microseconds>(now.time_since_epoch()).count() % 1'000'000;
  std::tm tm{};
  gmtime_r(&time_t_s, &tm);
  char buf[40];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%06ldZ",
                tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                tm.tm_min, tm.tm_sec, static_cast<long>(us));
  out.append(buf);
}

// Allocates a std::string, formats, mutex-serialized fwrite. Used while no
// sink is installed (boot + post-shutdown windows).
void StderrFallback(LogLevel level, std::string_view topic,
                    std::string_view message) {
  std::string out;
  out.reserve(96 + message.size());
  WriteIsoTimestamp(out);
  out.append(" [");
  out.append(LevelTag(level));
  out.append("] {");
  // GENERAL maps to the empty string; print "general" so unmuted lines stay
  // legible in the fallback writer.
  out.append(topic.empty() ? std::string_view{"general"} : topic);
  out.append("} ");
  // Bound stderr writes.
  constexpr size_t kCap = 64 * 1024;
  if (message.size() > kCap) {
    out.append(message.data(), kCap);
    out.append("... [truncated]");
  } else {
    out.append(message);
  }
  if (out.empty() || out.back() != '\n') {
    out.push_back('\n');
  }
  absl::MutexLock guard{&gStderrMutex};
  ::fwrite(out.data(), 1, out.size(), stderr);
  ::fflush(stderr);
}

// Async-signal-safe stderr write used by the CRASH topic. No heap, no mutex,
// no fwrite buffering -- just write(2). Caps message length to a stack
// buffer; truncates the rest. Output format mirrors the fallback's "[LEVEL]
// {topic} message\n" layout so crash log lines remain greppable.
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

LogLevel InitialLogLevel() noexcept {
  return gInitialLevel.load(std::memory_order_relaxed);
}

void InstallSink(const Sink* sink) noexcept {
  gSink.store(sink, std::memory_order_release);
}

void Initialize() noexcept {}
void Shutdown() noexcept {
  // Detach any installed sink so subsequent log calls hit the stderr
  // fallback. The actual LogManager teardown is owned by DuckDBEngine.
  gSink.store(nullptr, std::memory_order_release);
}
void Flush() noexcept {}

bool IsEnabled(LogLevel level) noexcept {
  if (const auto* sink = gSink.load(std::memory_order_acquire); sink) {
    return sink->should_log(level, std::string_view{});
  }
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
  if (const auto* sink = gSink.load(std::memory_order_acquire); sink) {
    sink->write(level, topic, message);
    return;
  }
  StderrFallback(level, topic, message);
} catch (...) {
  // Logging itself must never throw.
}

}  // namespace sdb::log
