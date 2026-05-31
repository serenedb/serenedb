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

#pragma once

#include <source_location>
#include <string_view>

namespace sdb {

// Thin wrapper over absl::InstallFailureSignalHandler. absl owns the
// SIGSEGV/SIGBUS/SIGFPE/SIGILL/SIGABRT/SIGTRAP handlers (alternate stack,
// symbolized stack dump, watchdog alarm). The bits we still own are:
//   - SetState: stamps a short server-state tag that the writerfn callback
//     prepends to every crash line, so logs say "[state=running] ..." rather
//     than just the bare stack.
//   - assertionFailure: SDB_ASSERT/SDB_VERIFY landing pad -- logs the
//     condition + message through the signal-safe CRASH topic and then
//     std::abort()s, which absl catches and dumps.
class CrashHandler {
 public:
  // Set server-state string (e.g. "starting", "running", "stopping"). The
  // pointer must remain valid for the rest of the process lifetime -- the
  // writerfn reads it from a signal handler, so callers must pass a string
  // literal or an object with static storage duration.
  static void SetState(std::string_view state);

  // SDB_ASSERT / SDB_VERIFY landing pad. Logs the assertion through the
  // CRASH topic (signal-safe write to stderr) and then std::abort()s, which
  // raises SIGABRT and trips absl's failure signal handler.
  [[noreturn]] static void assertionFailure(const char* file, int line,
                                            const char* func,
                                            const char* context,
                                            std::string_view message);
  [[noreturn]] static void assertionFailure(std::source_location location,
                                            const char* context,
                                            std::string_view message = {}) {
    assertionFailure(location.file_name(), location.line(),
                     location.function_name(), context, message);
  }

  // Wire absl::InstallFailureSignalHandler. Must be called after
  // absl::InitializeSymbolizer (GlobalContext ctor handles that).
  static void installCrashHandler();
};

}  // namespace sdb
