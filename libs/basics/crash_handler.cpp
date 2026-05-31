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

#include "basics/crash_handler.h"

#include <absl/debugging/failure_signal_handler.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include "basics/logger/logger.h"

namespace sdb {
namespace {

// Server-state tag set by SetState ("starting"/"running"/"stopping"/...). The
// writerfn dereferences this from a signal handler, so callers must pass a
// pointer that stays alive for the rest of the process.
std::atomic<const char*> gStateString{nullptr};

// Writerfn for absl::InstallFailureSignalHandler. Each abseil-emitted line
// arrives here (plus a final nullptr-as-flush-hint). We route it through the
// signal-safe CRASH topic so it lands in the same stderr stream as our other
// crash records, prefixed with the server-state tag for context.
void CrashWriter(const char* data) noexcept {
  // absl uses a nullptr argument as a "flush" hint; nothing to flush here.
  if (data == nullptr) {
    return;
  }
  // Strip the trailing newline absl adds -- LogCrash re-appends one to match
  // the rest of our log lines.
  std::string_view sv{data};
  while (!sv.empty() && sv.back() == '\n') {
    sv.remove_suffix(1);
  }
  if (const char* state = gStateString.load(std::memory_order_relaxed);
      state != nullptr && state[0] != '\0') {
    // Two-shot write: "[state=<tag>] " then the abseil line. LogCrash is
    // signal-safe (write(2) to stderr, fixed-size stack buffer).
    char prefix[64];
    size_t pos = 0;
    auto append = [&](std::string_view s) noexcept {
      size_t n = std::min(s.size(), sizeof(prefix) - pos);
      std::memcpy(prefix + pos, s.data(), n);
      pos += n;
    };
    append("[state=");
    append(state);
    append("] ");
    // Emit prefix and abseil line as a single CRASH record so they share
    // one "[FATAL] {crash} " header.
    char buf[4096];
    size_t out = 0;
    auto buf_append = [&](std::string_view s) noexcept {
      size_t n = std::min(s.size(), sizeof(buf) - out);
      std::memcpy(buf + out, s.data(), n);
      out += n;
    };
    buf_append(std::string_view{prefix, pos});
    buf_append(sv);
    log::LogCrash(LogLevel::FATAL, std::string_view{buf, out});
  } else {
    log::LogCrash(LogLevel::FATAL, sv);
  }
}

}  // namespace

void CrashHandler::SetState(std::string_view state) {
  gStateString.store(state.data(), std::memory_order_relaxed);
}

[[noreturn]] void CrashHandler::assertionFailure(const char* file, int line,
                                                 const char* func,
                                                 const char* context,
                                                 std::string_view message) {
  // Format on the stack: "assertion failed in <file>:<line> [<func>]: <expr>
  // ; <message>". No heap touch -- this path is reachable from signal-adjacent
  // contexts via SDB_ASSERT, and the message arrives through the CRASH topic
  // which itself takes the async-signal-safe write(2) route.
  char buf[2048];
  size_t pos = 0;
  auto append = [&](std::string_view s) noexcept {
    size_t n = std::min(s.size(), sizeof(buf) - pos);
    std::memcpy(buf + pos, s.data(), n);
    pos += n;
  };
  auto append_int = [&](long v) noexcept {
    char tmp[24];
    int n = std::snprintf(tmp, sizeof(tmp), "%ld", v);
    if (n > 0) {
      append(std::string_view{tmp, static_cast<size_t>(n)});
    }
  };
  append("assertion failed in ");
  append(file != nullptr ? std::string_view{file}
                         : std::string_view{"unknown"});
  append(":");
  append_int(line);
  if (func != nullptr) {
    append(" [");
    append(func);
    append("]");
  }
  append(": ");
  if (context != nullptr) {
    append(context);
  }
  if (!message.empty()) {
    append(" ; ");
    append(message);
  }

  log::LogCrash(LogLevel::FATAL, std::string_view{buf, pos});

  // Raise SIGABRT -- absl::InstallFailureSignalHandler catches it and dumps
  // a symbolized stack trace before terminating the process.
  std::abort();
}

void CrashHandler::installCrashHandler() {
  // absl::InitializeSymbolizer(argv[0]) is invoked from GlobalContext ctor;
  // by the time RunServer reaches us the symbolizer is already up.
  absl::FailureSignalHandlerOptions options;
  options.symbolize_stacktrace = true;
  options.use_alternate_stack = true;
  options.alarm_on_failure_secs = 3;
  options.call_previous_handler = false;
  options.writerfn = &CrashWriter;
  absl::InstallFailureSignalHandler(options);
}

}  // namespace sdb
