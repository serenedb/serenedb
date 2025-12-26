#include "engine_feature.h"

#include <memory>

#include "app/app_server.h"
#include "catalog/catalog.h"

#ifdef SDB_CLUSTER
#include "rocksdb_engine/rocksdb_engine.h"
#include "replication/replication_feature.h"
#else
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#endif

namespace sdb {

EngineFeature::EngineFeature(Server& server)
  : SerenedFeature{server, name()},
    _engine{std::make_shared<EngineType>(server)} {
  setOptional(false);
}

EngineFeature::EngineType& GetServerEngine() {
  return SerenedServer::Instance().getFeature<EngineFeature>().engine();
}

void EngineFeature::start() {
  _engine->start();

  auto r = server().getFeature<catalog::CatalogFeature>().Open();
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  _started.store(true);
}

void EngineFeature::stop() {
#ifdef SDB_CLUSTER
  if (auto replication = server().TryGetFeature<ReplicationFeature>();
      replication && !ServerState::instance()->IsCoordinator()) {
    for (auto [_, applier] : replication->GetAllReplicationAppliers()) {
      replication->stopApplier(applier);
    }
  }
#endif
  _engine->cleanupReplicationContexts();
  _engine->stop();
}

void EngineFeature::prepare() { _engine->prepare(); }

void EngineFeature::unprepare() { _engine->unprepare(); }

void EngineFeature::beginShutdown() { _engine->beginShutdown(); }

}  // namespace sdb
