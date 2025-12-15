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

#include "basics/down_cast.h"
#include "rest_server/serened.h"

namespace sdb {

class StorageEngine;

class EngineSelectorFeature : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "EngineSelector"; }

  explicit EngineSelectorFeature(Server& server);

  void start() final;
  void stop() final;
  void prepare() override;
  void unprepare() override;

  StorageEngine& engine();
  bool started() const { return _started.load(std::memory_order_relaxed); }

  bool isRocksDB();

 protected:
  StorageEngine* _engine = nullptr;
  std::atomic_bool _started = false;
};

StorageEngine& GetServerEngine();

template<typename T>
T& GetServerEngineAs() {
  return basics::downCast<T>(GetServerEngine());
}

}  // namespace sdb
