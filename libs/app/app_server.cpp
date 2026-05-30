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

#include "basics/lifecycle.h"

namespace sdb::app {

AppServer::AppServer() {
  SDB_ASSERT(gInstance == nullptr, "AppServer is a singleton");
  gInstance = this;
}

AppServer::~AppServer() { gInstance = nullptr; }

bool AppServer::isStopping() const noexcept {
  auto s = state();
  return s == State::InStop || s == State::Stopped || s == State::Aborted;
}

void AppServer::beginShutdown() noexcept {
  // Idempotent: lifecycle::BeginShutdown / the condvar notify are safe
  // to call repeatedly. State transitions are owned by RunServer; we
  // only nudge the wait loop here.
  lifecycle::BeginShutdown();

  absl::MutexLock guard{&_shutdown_condition.mutex};
  _abort_waiting = true;
  _shutdown_condition.cv.notify_one();
}

void AppServer::parseOptions(int argc, char* argv[]) {
  // All CLI knobs are ABSL_FLAGs declared in their owning .cpp files
  // and read via absl::GetFlag during validateOptions/prepare. Run
  // absl's parser; positional args land in lifecycle:: for features
  // (DatabasePathFeature) to pick up.
  auto positionals = absl::ParseCommandLine(argc, argv);
  lifecycle::SetPositionalArgs(positionals);
}

void AppServer::wait() {
  while (true) {
    if (lifecycle::gCtrlC.load()) {
      beginShutdown();
    }
    absl::MutexLock guard{&_shutdown_condition.mutex};
    if (_abort_waiting) {
      break;
    }
    _shutdown_condition.cv.WaitWithTimeout(&_shutdown_condition.mutex,
                                           absl::Milliseconds(100));
  }
}

}  // namespace sdb::app
