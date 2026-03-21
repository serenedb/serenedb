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

#pragma once

#include <source_location>
#include <string_view>

namespace sdb {

class CrashHandler {
 public:
  /// log backtrace for current thread to logfile
  static void logBacktrace();

  /// set server state string as context for crash messages.
  /// state string will be advanced whenever the application server
  /// changes its state. state string must be null-terminated!
  static void SetState(std::string_view state);

  /// logs a fatal message and crashes the program
  [[noreturn]] static void crash(std::string_view context);

  /// logs an assertion failure and crashes the program
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

  /// set flag to kill process hard using SIGKILL, in order to circumvent
  /// core file generation etc.
  static void setHardKill();

  /// disable printing of backtraces
  static void disableBacktraces();

  /// installs the crash handler globally
  static void installCrashHandler();
};

}  // namespace sdb
