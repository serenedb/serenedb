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

#include "app_server.h"

#include <absl/flags/parse.h>

#include <chrono>
#include <thread>

#include "basics/lifecycle.h"
#include "basics/logger/logger.h"

namespace sdb::app {

AppServer::AppServer() {
  SDB_ASSERT(gInstance == nullptr, "AppServer is a singleton");
  gInstance = this;
}

AppServer::~AppServer() { gInstance = nullptr; }

void AppServer::parseOptions(int argc, char* argv[]) {
  // All CLI knobs are ABSL_FLAGs declared in their owning .cpp files
  // and read via absl::GetFlag during validateOptions/prepare. Run
  // absl's parser; positional args land in lifecycle:: for features
  // (DatabasePathFeature) to pick up.
  auto positionals = absl::ParseCommandLine(argc, argv);
  lifecycle::SetPositionalArgs(positionals);
}

void AppServer::wait() {
  // The signal handler is async-signal-safe and flips
  // lifecycle::IsStopping() via BeginShutdown(); we poll the flag every
  // 100ms here. A condvar+mutex would shave the worst-case wakeup
  // latency at the cost of a non-AS-safe notify path inside the
  // handler, which we're not willing to pay -- shutdown latency of
  // <100ms is plenty.
  while (!lifecycle::IsStopping()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  // Log from non-handler context (the handler can't allocate / lock).
  SDB_INFO(GENERAL,
           "received shutdown signal, beginning shut down "
           "sequence");
}

}  // namespace sdb::app
