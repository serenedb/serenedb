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

#include "engine_selector_feature.h"

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "replication/replication_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"

namespace sdb {

EngineSelectorFeature::EngineSelectorFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(false);
}

StorageEngine& EngineSelectorFeature::engine() {
  SDB_ASSERT(_engine);
  return *_engine;
}

bool EngineSelectorFeature::isRocksDB() {
  SDB_ASSERT(_engine);
  return _engine->typeName() == RocksDBEngineCatalog::kEngineName;
}

StorageEngine& GetServerEngine() {
  return SerenedServer::Instance().getFeature<EngineSelectorFeature>().engine();
}

void EngineSelectorFeature::start() {
  auto r = server().getFeature<catalog::CatalogFeature>().Open();
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  _started.store(true);
}

void EngineSelectorFeature::stop() {
#ifdef SDB_CLUSTER
  if (auto replication = server().TryGetFeature<ReplicationFeature>();
      replication && !ServerState::instance()->IsCoordinator()) {
    for (auto [_, applier] : GetAllReplicationAppliers()) {
      replication->stopApplier(applier);
    }
  }
#endif
  _engine->cleanupReplicationContexts();
}

#ifndef SDB_CLUSTER
void EngineSelectorFeature::prepare() {
  _engine = &SerenedServer::Instance().getFeature<RocksDBEngineCatalog>();
  SDB_ASSERT(_engine);
}

void EngineSelectorFeature::unprepare() {
  SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Cleanup();
  _engine = nullptr;
}
#endif

}  // namespace sdb
