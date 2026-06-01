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

#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <duckdb/logging/logger.hpp>

namespace sdb::log {
namespace {

duckdb::Logger* gLogger = nullptr;

}  // namespace

void SetLogger(duckdb::Logger* logger) noexcept { gLogger = logger; }

bool IsEnabled(duckdb::LogLevel level, std::string_view topic) noexcept {
  // Every topic constant in topic.h is initialized from a string literal
  // (.data() is NUL-terminated, see invariant in topic.h).
  return gLogger->ShouldLog(topic.data(), level);
}

void Log(duckdb::LogLevel level, std::string_view topic,
         const std::string& message) noexcept try {
  gLogger->WriteLog(topic.data(), level, message.c_str());
} catch (...) {
}

void LogCrash(std::string_view message) noexcept {
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
  append("[FATAL] ");
  append(message);
  buf[pos++] = '\n';
  // write(2) is async-signal-safe (POSIX.1-2017 Section 2.4.3).
  [[maybe_unused]] ssize_t unused = ::write(STDERR_FILENO, buf, pos);
}

}  // namespace sdb::log
