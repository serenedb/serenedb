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

class EngineFeature final {
 public:
  inline static EngineFeature* gInstance = nullptr;
  static EngineFeature& instance() noexcept { return *gInstance; }

  explicit EngineFeature(SerenedServer& server);
  ~EngineFeature();

  void start();
  void stop();

  RocksDBEngineCatalog& engine() { return *_engine; }

 protected:
  std::shared_ptr<RocksDBEngineCatalog> _engine;
};

RocksDBEngineCatalog& GetServerEngine();

}  // namespace sdb
