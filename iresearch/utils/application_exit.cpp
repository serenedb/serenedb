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

#include "iresearch/utils/application_exit.hpp"

#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#include "iresearch/utils/log.hpp"
#include "iresearch/utils/system_compiler.hpp"

#ifdef SDB_COVERAGE
extern "C" int __llvm_profile_write_file(void);
#endif

namespace irs {
namespace {

void FlushCoverage() noexcept {
#ifdef SDB_COVERAGE
  __llvm_profile_write_file();
#endif
}

}  // namespace

[[noreturn]] void FatalErrorExit() noexcept {
  log::Flush();
  std::fflush(nullptr);
  FlushCoverage();
  std::_Exit(EXIT_FAILURE);
}

[[noreturn]] void PlannedCrashExit() noexcept {
  FlushCoverage();
  std::_Exit(kPlannedCrashExitCode);
}

}  // namespace irs
