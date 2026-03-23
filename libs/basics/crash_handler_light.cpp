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

#include <absl/strings/internal/ostringstream.h>

#include <exception>
#include <iostream>
#include <sstream>

#include "crash_handler.h"

// This is a Light/Mock version of SereneDB's crash handler without
// dependencies on the remainder of SereneDB.

namespace sdb {

void CrashHandler::logBacktrace() {}

[[noreturn]] void CrashHandler::crash(std::string_view context) {
  std::cerr << "[LightCrashHandler] " << context << std::endl;
  std::flush(std::cerr);
  std::terminate();
};

[[noreturn]] void CrashHandler::assertionFailure(const char* file, int line,
                                                 const char* func,
                                                 const char* context,
                                                 const char* message) {
  std::string msg_str;
  absl::strings_internal::OStringStream msg{&msg_str};

  msg << "Assertion failed in file " << file << ":" << line << ", expression "
      << context << " with message ";
  if (message) {
    msg << message;
  }
  msg << std::endl;
  CrashHandler::crash(msg_str);
}

void CrashHandler::setHardKill() {
  CrashHandler::crash("setHardKill is not implemented");
}

void CrashHandler::disableBacktraces() {
  CrashHandler::crash("disabledBacktraces is not implemented.");
}

void CrashHandler::installCrashHandler() {
  CrashHandler::crash("installCrashHandler is not implemented.");
}

}  // namespace sdb
