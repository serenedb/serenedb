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

#pragma once

#include "basics/down_cast.h"
#include "rest_server/serened.h"

namespace sdb {

class RocksDBEngineCatalog;

class EngineFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Engine"; }

  explicit EngineFeature(Server& server);

  void start() final;
  void stop() final;
  void prepare() final;
  void unprepare() final;
  void beginShutdown() final;

  RocksDBEngineCatalog& engine() { return *_engine; }
  bool started() const { return _started.load(std::memory_order_relaxed); }

 protected:
  std::shared_ptr<RocksDBEngineCatalog> _engine;
  std::atomic_bool _started = false;
};

RocksDBEngineCatalog& GetServerEngine();

}  // namespace sdb
