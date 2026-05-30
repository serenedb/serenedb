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

#include "app_server.h"

#include <absl/cleanup/cleanup.h>
#include <absl/flags/parse.h>
#include <absl/strings/str_join.h>
#include <vpack/options.h>
#include <vpack/slice.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <new>
#include <ranges>
#include <utility>

#include "app/app_feature.h"
#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/lifecycle.h"
#include "basics/logger/logger.h"
#include "basics/result.h"
#include "basics/string_utils.h"

namespace sdb::app {

AppServer::AppServer(const char* binary_path,
                     std::span<std::unique_ptr<AppFeature>> features)
  : _all_features{features}, _binary_path{binary_path} {
  addReporter(
    ProgressHandler{[](AppServer::State state) {},
                    [](AppServer::State state, std::string_view name) {}});
}

bool AppServer::isStopping() const {
  auto tmp = state();
  return isStoppingState(tmp);
}

bool AppServer::isStoppingState(State state) const {
  return state == State::InShutdown || state == State::InStop ||
         state == State::InUnprepare || state == State::Stopped ||
         state == State::Aborted;
}

// signal the server to shut down
void AppServer::beginShutdown() {
  // fetch the old state, check if somebody already called shutdown, and only
  // proceed if not.
  State old = state();
  do {
    if (isStoppingState(old)) {
      // beginShutdown already called, nothing to do now
      return;
    }
    // try to enter the new state, but make sure nobody changed it in between
  } while (!_state.compare_exchange_weak(old, State::InShutdown,
                                         std::memory_order_release,
                                         std::memory_order_acquire));

  lifecycle::BeginShutdown();

  // make sure that we advance the state when we get out of here
  absl::Cleanup wait_aborter = [this] noexcept {
    absl::MutexLock guard{&_shutdown_condition.mutex};
    _abort_waiting = true;
    _shutdown_condition.cv.notify_one();
  };

  // now we can execute the actual shutdown sequence

  // fowards the begin shutdown signal to all features
  for (auto& feature : EnabledFeaturesReverse()) {
    auto r = basics::SafeCall([&] { feature.beginShutdown(); });

    if (!r.ok()) {
      SDB_ERROR(STARTUP,
                "caught exception during beginShutdown of feature '",
                feature.name(), "': ", r.errorMessage());
    }
  }
}

void AppServer::shutdownFatalError() { reportServerProgress(State::Aborted); }

void AppServer::parseOptions(int argc, char* argv[]) {
  // All CLI knobs are declared as ABSL_FLAGs in their owning .cpp files
  // and read via absl::GetFlag during validateOptions/prepare. Run absl's
  // parser and stash positional args (e.g. positional data-dir) for
  // features to pick up.
  auto positionals = absl::ParseCommandLine(argc, argv);
  lifecycle::SetPositionalArgs(positionals);
  _positional_args.clear();
  _positional_args.reserve(positionals.size());
  for (auto* p : positionals) {
    _positional_args.emplace_back(p);
  }
}

void AppServer::wait() {
  // wait here until beginShutdown has been called and finished
  while (true) {
    if (gCtrlC.load()) {
      beginShutdown();
    }

    // wait until somebody calls beginShutdown and it finishes
    absl::MutexLock guard{&_shutdown_condition.mutex};
    if (_abort_waiting) {
      break;
    }
    _shutdown_condition.cv.WaitWithTimeout(&_shutdown_condition.mutex,
                                           absl::Milliseconds(100));
  }
}

void AppServer::reportServerProgress(State state) {
  for (auto& reporter : _progress_reports) {
    if (reporter.state) {
      reporter.state(state);
    }
  }
}

void AppServer::reportFeatureProgress(State state, std::string_view name) {
  for (auto& reporter : _progress_reports) {
    if (reporter.feature) {
      reporter.feature(state, name);
    }
  }
}

}  // namespace sdb::app
