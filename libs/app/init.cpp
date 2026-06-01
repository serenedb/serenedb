////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "init.h"

#include <absl/debugging/symbolize.h>
#include <sys/resource.h>
#include <vpack/vpack_helper.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <string_view>
#include <yaclib/log.hpp>

#include "basics/crash_handler.h"
#include "basics/logger/logger.h"
#include "basics/random/random_generator.h"
#include "rest/version.h"
#define ZLIB_COMPAT
#include <functable.h>

namespace sdb::app {
namespace {

// Raise the soft NOFILE limit up to min(kTarget, hard). Hard-limit bumps
// require CAP_SYS_RESOURCE on Linux, so we don't try; if the operator wants
// more, they raise the hard limit in systemd / ulimit before invoking us.
void RaiseFdLimit() {
  constexpr rlim_t kTarget = 65535;
  rlimit lim{};
  if (getrlimit(RLIMIT_NOFILE, &lim) != 0) {
    SDB_DEBUG(GENERAL, "getrlimit(RLIMIT_NOFILE) failed: ", strerror(errno));
    return;
  }
  const rlim_t target = std::min(kTarget, lim.rlim_max);
  if (lim.rlim_cur >= target) {
    return;
  }
  rlimit next = lim;
  next.rlim_cur = target;
  if (setrlimit(RLIMIT_NOFILE, &next) != 0) {
    SDB_DEBUG(GENERAL, "setrlimit(RLIMIT_NOFILE, ", target,
              ") failed: ", strerror(errno));
  }
}

}  // namespace

void InitProcess(const char* argv0) {
  // Order matters:
  //   * RaiseFdLimit              soft NOFILE -> 65535 (or hard, if lower)
  //   * random::Reset             seeds the PRNGs the basics layer holds
  //   * Version::initialize       fills the rest::Version table
  //   * VPackHelper::initialize   wires the velocypack <-> sdb glue
  //   * FUNCTABLE_INIT            picks the zlib-ng dispatch (SIMD)
  //   * InitializeSymbolizer      lets the absl crash handler symbolize
  //   * YACLIB_INIT_DEBUG         routes yaclib's debug-asserts through us
  RaiseFdLimit();
  random::Reset();
  rest::Version::initialize();
  basics::VPackHelper::initialize();
  FUNCTABLE_INIT;
  absl::InitializeSymbolizer(argv0);
  YACLIB_INIT_DEBUG([](std::string_view file, std::size_t line,
                       std::string_view func, std::string_view condition,
                       std::string_view message) noexcept {
    CrashHandler::assertionFailure(file.data(), static_cast<int>(line),
                                   func.data(), condition.data(),
                                   message.data());
  });
}

void ShutdownGlobals() { random::Reset(); }

}  // namespace sdb::app
