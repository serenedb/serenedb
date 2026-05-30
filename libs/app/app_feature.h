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

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "basics/common.h"
#include "basics/down_cast.h"
#include "basics/exceptions.h"

namespace sdb {
namespace options {

class ProgramOptions;
}
namespace app {

class AppServer;

class AppFeature {
 public:
  virtual ~AppFeature();

  enum class State {
    Uninitialized = 0,
    Initialized,
    Validated,
    Prepared,
    Started,
    Stopped,
    Unprepared,
  };

  auto& server() const { return _server; }

  std::string_view name() const noexcept { return _name; }

  State state() const { return _state.load(std::memory_order_acquire); }

  // Always true now; the disable / setOptional surface is gone.
  bool isEnabled() const { return true; }

  // validate options pulled from absl::GetFlag during init.
  virtual void validateOptions() {}

  // preparation phase for feature in the preparation phase, the features must
  // not start any threads. furthermore, they must not write any files under
  // elevated privileges if they want other features to access them, or if they
  // want to access these files with dropped privileges
  virtual void prepare() {}

  // start the feature
  virtual void start() {}

  // notify the feature about a shutdown request
  virtual void beginShutdown() {}

  // stop the feature
  virtual void stop() {}

  // shut down the feature
  virtual void unprepare() {}

 protected:
  AppFeature(AppServer& server, std::string_view name)
    : _server{server}, _name{name} {}

  // No-op shim so existing setOptional(true)/(false) call sites compile.
  void setOptional(bool /*value*/) {}

 private:
  friend class AppServer;

  // set a feature's state. this method should be called by the
  // application server only
  void state(State state) { _state.store(state, std::memory_order_release); }

  AppServer& _server;

  std::string_view _name;

  std::atomic<State> _state{State::Uninitialized};
};

template<typename ServerT>
class AppFeatureImpl : public AppFeature {
 public:
  using Server = ServerT;

  Server& server() const noexcept {
    return basics::downCast<Server>(AppFeature::server());
  }

 protected:
  using AppFeature::AppFeature;
};

}  // namespace app
}  // namespace sdb
