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

#include "engine_feature.h"

#include <memory>

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"

namespace sdb {

EngineFeature::EngineFeature(Server& server)
  : SerenedFeature{server, name()},
    _engine{std::make_shared<RocksDBEngineCatalog>(server)} {
  setOptional(false);
}

RocksDBEngineCatalog& GetServerEngine() {
  return SerenedServer::Instance().getFeature<EngineFeature>().engine();
}

void EngineFeature::start() {
  _engine->start();
  _started.store(true);
}

void EngineFeature::stop() {
  _engine->cleanupReplicationContexts();
  _engine->stop();
}

void EngineFeature::prepare() { _engine->prepare(); }

void EngineFeature::unprepare() { _engine->unprepare(); }

void EngineFeature::beginShutdown() { _engine->beginShutdown(); }

}  // namespace sdb
