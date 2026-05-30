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

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <ctime>

namespace sdb::log {
namespace {

struct TopicEntry {
  std::string_view name;
  std::atomic<LogLevel> level{LogLevel::DEFAULT};
  std::atomic<LogLevel> default_level{LogLevel::DEFAULT};
};

// Topic table. Levels can be set via SetLogLevel("name=info") or via the
// SQL surface (SET sdb_log_level = 'name=info' -- wired in Phase 9).
// Adding a topic: add it here and to topic.h.
TopicEntry gTopics[]{
  {GENERAL, {LogLevel::DEFAULT}, {LogLevel::DEFAULT}},
  {STARTUP, {LogLevel::DEFAULT}, {LogLevel::DEFAULT}},
  {HTTP, {LogLevel::WARN}, {LogLevel::WARN}},
  {SSL, {LogLevel::WARN}, {LogLevel::WARN}},
  {STORAGE, {LogLevel::DEFAULT}, {LogLevel::DEFAULT}},
  {SEARCH, {LogLevel::DEFAULT}, {LogLevel::DEFAULT}},
  {IRESEARCH, {LogLevel::DEFAULT}, {LogLevel::DEFAULT}},
  {CRASH, {LogLevel::DEFAULT}, {LogLevel::DEFAULT}},
};

TopicEntry* FindTopicEntry(std::string_view name) noexcept {
  for (auto& t : gTopics) {
    if (t.name == name) {
      return &t;
    }
  }
  return nullptr;
}

std::atomic<LogLevel> gGlobalLevel{LogLevel::INFO};
std::atomic<bool> gActive{false};
std::atomic<bool> gLogRequestParameters{true};
absl::Mutex gWriteMutex;  // serializes formatted writes to stderr

const char* LevelTag(LogLevel l) noexcept {
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

}  // namespace

LogLevel GetLogLevel() noexcept {
  return gGlobalLevel.load(std::memory_order_relaxed);
}

void SetLogLevel(LogLevel level) noexcept {
  gGlobalLevel.store(level, std::memory_order_relaxed);
}

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

void SetLogLevel(std::string_view def) {
  // Forms accepted: "info", "topic=info", "all=info".
  auto eq = def.find('=');
  if (eq == std::string_view::npos) {
    SetLogLevel(TranslateLogLevel(def));
    return;
  }
  auto name = absl::AsciiStrToLower(def.substr(0, eq));
  auto level = TranslateLogLevel(def.substr(eq + 1));
  if (name == "all") {
    for (auto& t : gTopics) {
      t.level.store(level, std::memory_order_relaxed);
    }
    SetLogLevel(level);
    return;
  }
  if (auto* topic = FindTopicEntry(name); topic != nullptr) {
    topic->level.store(level, std::memory_order_relaxed);
  }
}

LogLevel TopicLevel(std::string_view topic) noexcept {
  if (auto* t = FindTopicEntry(topic); t != nullptr) {
    return t->level.load(std::memory_order_relaxed);
  }
  return LogLevel::DEFAULT;
}

void TopicSetLevel(std::string_view topic, LogLevel level) noexcept {
  if (auto* t = FindTopicEntry(topic); t != nullptr) {
    t->level.store(level, std::memory_order_relaxed);
  }
}

void SnapshotTopicDefaults() {
  for (auto& t : gTopics) {
    t.default_level.store(t.level.load(std::memory_order_relaxed),
                          std::memory_order_relaxed);
  }
}

void ResetLogLevels() {
  for (auto& t : gTopics) {
    t.level.store(t.default_level.load(std::memory_order_relaxed),
                  std::memory_order_relaxed);
  }
}

std::vector<std::string_view> KnownTopics() noexcept {
  std::vector<std::string_view> result;
  result.reserve(std::size(gTopics));
  for (const auto& t : gTopics) {
    result.push_back(t.name);
  }
  return result;
}

std::string LogLevelString() {
  std::string result;
  for (const auto& t : gTopics) {
    auto level = t.level.load(std::memory_order_relaxed);
    if (level == LogLevel::DEFAULT) {
      continue;
    }
    if (!result.empty()) {
      result.push_back(',');
    }
    result.append(t.name);
    result.push_back('=');
    result.append(LevelTag(level));
  }
  return result;
}

bool IsActive() noexcept { return gActive.load(std::memory_order_acquire); }
void Initialize() noexcept { gActive.store(true, std::memory_order_release); }
void Shutdown() noexcept { gActive.store(false, std::memory_order_release); }
void Flush() noexcept {}           // sync logger; nothing to flush.
void ClearCachedPid() noexcept {}  // PID is fetched per-call; no cache.

bool GetLogRequestParameters() noexcept {
  return gLogRequestParameters.load(std::memory_order_relaxed);
}
void SetLogRequestParameters(bool v) noexcept {
  gLogRequestParameters.store(v, std::memory_order_relaxed);
}

void Log(LogLevel level, std::string_view topic, std::string_view message) try {
  std::string out;
  out.reserve(96 + message.size());
  WriteIsoTimestamp(out);
  out.append(" [");
  out.append(LevelTag(level));
  out.append("] {");
  out.append(topic);
  out.append("} ");
  // Bound stderr writes.
  static constexpr size_t kCap = 64 * 1024;
  if (message.size() > kCap) {
    out.append(message.data(), kCap);
    out.append("... [truncated]");
  } else {
    out.append(message);
  }
  if (out.empty() || out.back() != '\n') {
    out.push_back('\n');
  }
  absl::MutexLock guard{&gWriteMutex};
  ::fwrite(out.data(), 1, out.size(), stderr);
  ::fflush(stderr);
} catch (...) {
  // Logging itself must never throw.
}

}  // namespace sdb::log
