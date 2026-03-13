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

#include "app/app_feature.h"
#include "rest_server/serened.h"

namespace sdb {

namespace rest {

class RestHandlerFactory;
class AsyncJobManager;

}  // namespace rest

class ServerFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Server"; }

  ServerFeature(Server& server, int* result);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void start() final;
  void beginShutdown() final;
  bool isStopping() const {
    return _is_stopping.load(std::memory_order_acquire);
  }

 private:
  void waitForHeartbeat();

  bool _rest_server = true;
  bool _validate_utf8_strings = false;
  std::atomic_bool _is_stopping = false;
  int* _result;
};

}  // namespace sdb
