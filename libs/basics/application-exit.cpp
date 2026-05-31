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

#include "basics/application-exit.h"

#include "basics/cleanup_functions.h"
#include "basics/common.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

namespace sdb {

static void DefaultExitFunction(int exit_code, void* /*data*/) {
  sdb::basics::CleanupFunctions::run(exit_code, nullptr);
  _exit(exit_code);
}

ExitFunction gExitFunction = DefaultExitFunction;

void ApplicationExitSetExit(ExitFunction exit_function) {
  if (exit_function != nullptr) {
    gExitFunction = exit_function;
  } else {
    gExitFunction = DefaultExitFunction;
  }
}

[[noreturn]] void FatalErrorExitCode(int code) noexcept {
  try {
    sdb::basics::CleanupFunctions::run(code, nullptr);
    gExitFunction(code, nullptr);
  } catch (...) {
  }
  exit(code);
}

[[noreturn]] void FatalErrorExit() noexcept {
  FatalErrorExitCode(EXIT_FAILURE);
}

[[noreturn]] void FatalErrorAbort() noexcept {
  try {
    sdb::basics::CleanupFunctions::run(500, nullptr);
  } catch (...) {
  }
  std::abort();
}

}  // namespace sdb
