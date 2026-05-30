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

// this method will initialize and validate options
// of all feature, start them and wait for a shutdown
// signal. after that, it will shutdown all features
void AppServer::run(int argc, char* argv[]) {
  // dependency setup
  SetupFeatures();

  // parse the command line via absl::flags
  parseOptions(argc, argv);

  // validate options pulled from absl::GetFlag by each feature
  _state.store(State::InValidateOptions, std::memory_order_release);
  reportServerProgress(State::InValidateOptions);
  validateOptions();

  // now the features will actually do some preparation work
  // in the preparation phase, the features must not start any threads
  // furthermore, they must not write any files under elevated privileges
  // if they want other features to access them, or if they want to access
  // these files with dropped privileges
  _state.store(State::InPrepare, std::memory_order_release);
  reportServerProgress(State::InPrepare);
  prepare();

  // start features. now features are allowed to start threads, write files
  // etc.
  _state.store(State::InStart, std::memory_order_release);
  reportServerProgress(State::InStart);
  start();

  // wait until we get signaled the shutdown request
  _state.store(State::InWait, std::memory_order_release);
  reportServerProgress(State::InWait);
  wait();

  // beginShutdown is called asynchronously ----------

  // stop all features
  _state.store(State::InStop, std::memory_order_release);
  reportServerProgress(State::InStop);
  stop();

  // unprepare all features
  _state.store(State::InUnprepare, std::memory_order_release);
  reportServerProgress(State::InUnprepare);
  unprepare();

  // stopped
  _state.store(State::Stopped, std::memory_order_release);
  reportServerProgress(State::Stopped);
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
  _positional_args.clear();
  _positional_args.reserve(positionals.size());
  for (auto* p : positionals) {
    _positional_args.emplace_back(p);
  }
}

void AppServer::validateOptions() {
  for (auto& feature : EnabledFeatures()) {
    reportFeatureProgress(_state.load(std::memory_order_relaxed),
                          feature.name());
    feature.validateOptions();
    feature.state(AppFeature::State::Validated);
  }
}

void AppServer::SetupFeatures() {
  for (auto& feature : EnabledFeatures()) {
    feature.state(AppFeature::State::Initialized);
  }
}

void AppServer::prepare() {
  for (auto& feature : EnabledFeatures()) {
    reportFeatureProgress(_state.load(std::memory_order_relaxed),
                          feature.name());

    auto r = basics::SafeCall([&] { feature.prepare(); });

    if (!r.ok()) {
      SDB_ERROR(STARTUP,
                "caught exception during prepare of feature '", feature.name(),
                "': ", r.errorMessage());
    }

    feature.state(AppFeature::State::Prepared);
  }
}

void AppServer::start() {
  for (auto& feature : EnabledFeatures()) {
    auto r = basics::SafeCall(
      [&] {
        reportFeatureProgress(_state.load(std::memory_order_relaxed),
                              feature.name());
        feature.start();
        feature.state(AppFeature::State::Started);
      },
      [&](ErrorCode code, std::string_view what) {
        return Result{
          code, "startup aborted: caught exception during start of feature '",
          feature.name(), "': ", what.empty() ? "unspecified error" : what};
      });

    if (!r.ok()) {
      SDB_ERROR(STARTUP, r.errorMessage(), ". shutting down");

      auto started_features =
        EnabledFeaturesReverse() | std::views::filter([](const auto& feature) {
          return feature.state() == AppFeature::State::Started;
        });

      // try to stop all feature that we just started
      for (auto& feature : started_features) {
        auto r = basics::SafeCall([&] { feature.beginShutdown(); });

        if (!r.ok()) {
          // ignore errors on shutdown
          SDB_ERROR(STARTUP,
                    "caught exception while stopping feature '", feature.name(),
                    "', error: ", r.errorMessage());
        }
      }

      // try to stop all feature that we just started
      for (auto& feature : started_features) {
        auto r = basics::SafeCall([&] { feature.stop(); });

        if (!r.ok()) {
          // if something goes wrong, we simply rethrow to abort!
          SDB_FATAL(STARTUP,
                    "caught exception while stopping feature '", feature.name(),
                    "', error: ", r.errorMessage());
          SDB_THROW(std::move(r));
        }

        feature.state(AppFeature::State::Stopped);
      }

      auto stopped_features =
        EnabledFeaturesReverse() | std::views::filter([](const auto& feature) {
          const auto state = feature.state();
          return state == AppFeature::State::Stopped ||
                 state == AppFeature::State::Prepared;
        });

      // try to unprepare all feature that we just started
      for (auto& feature : stopped_features) {
        auto r = basics::SafeCall([&] { feature.unprepare(); });

        if (!r.ok()) {
          // if something goes wrong, we simply rethrow to abort!
          SDB_FATAL(STARTUP,
                    "caught exception while unpreparing feature '",
                    feature.name(), "', error: ", r.errorMessage());
          SDB_THROW(std::move(r));
        }

        feature.state(AppFeature::State::Unprepared);
      }

      shutdownFatalError();
      SDB_THROW(std::move(r));  // throw exception so the startup aborts
    }
  }
}

void AppServer::stop() {
  for (auto& feature : EnabledFeaturesReverse()) {
    reportFeatureProgress(_state.load(std::memory_order_relaxed),
                          feature.name());

    auto r = basics::SafeCall([&] { feature.stop(); });

    if (!r.ok()) {
      SDB_ERROR(STARTUP,
                "caught exception during stop of feature '", feature.name(),
                "': ", r.errorMessage());
    }

    feature.state(AppFeature::State::Stopped);
  }
}

void AppServer::unprepare() {
  for (auto& feature : EnabledFeaturesReverse()) {
    reportFeatureProgress(_state.load(std::memory_order_relaxed),
                          feature.name());

    auto r = basics::SafeCall([&] { feature.unprepare(); });

    if (!r.ok()) {
      SDB_ERROR(STARTUP,
                "caught exception during unprepare of feature '",
                feature.name(), "': ", r.errorMessage());
    }

    feature.state(AppFeature::State::Unprepared);
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
