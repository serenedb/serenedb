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

#include "pg_feature.h"

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/random/random_generator.h"
#include "connector/serenedb_connector.hpp"
#include "pg/system_catalog.h"
#include "pg_functions_registration.hpp"
#include "query/types.h"
#include "rest_server/endpoint_feature.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::pg {

PostgresFeature::PostgresFeature(SerenedServer& server)
  : SerenedFeature{server, name()} {
  setOptional(true);
}

void PostgresFeature::CancelTaskPacket(uint64_t key) {
  auto task = [&] {
    std::lock_guard lock{_mutex};
    auto it = _tasks.find(key);
    return it != _tasks.end() ? it->second.lock() : nullptr;
  }();
  if (task) {
    basics::downCast<PgSQLCommTaskBase>(*task).CancelPacket();
  }
}

uint64_t PostgresFeature::RegisterTask(PgSQLCommTaskBase& task) {
  auto weak = task.weak_from_this();
  std::lock_guard lock{_mutex};
  while (true) {
    const auto key = random::RandU64();
    if (key != 0 && _tasks.try_emplace(key, std::move(weak)).second) {
      return key;
    }
  }
}

void PostgresFeature::validateOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  const auto& endpoint_list =
    server().getFeature<HttpEndpointProvider, EndpointFeature>().endpointList();
  const bool needs_disable = std::ranges::none_of(
    endpoint_list | std::views::values, [](const auto& endpoint) {
      return endpoint->transport() == Endpoint::TransportType::PGSQL;
    });
  if (needs_disable) {
    disable();
  }
}

void PostgresFeature::UnregisterTask(uint64_t key) {
  std::lock_guard lock{_mutex};
  [[maybe_unused]] auto count = _tasks.erase(key);
  SDB_ASSERT(count == 1);
}

void PostgresFeature::prepare() {
  folly::SingletonVault::singleton()->registrationComplete();

  velox::memory::MemoryManager::initialize(
    velox::memory::MemoryManager::Options{});

  RegisterVeloxFunctionsAndTypes();
}

void PostgresFeature::start() {
  pg::RegisterSystemViews();
  if (ServerState::instance()->IsDBServer() ||
      ServerState::instance()->IsSingle()) {
    auto& engine = GetServerEngine();
    auto* cf =
      RocksDBColumnFamilyManager::get(RocksDBColumnFamilyManager::Family::Default);
    SDB_ASSERT(cf);

    auto connector = std::make_shared<connector::SereneDBConnector>(
      StaticStrings::kSereneDBConnector, nullptr, *engine.db(), *cf);
    velox::connector::registerConnector(std::move(connector));
    auto connector_metadata =
      std::make_shared<connector::SereneDBConnectorMetadata>();
    axiom::connector::ConnectorMetadata::registerMetadata(
      StaticStrings::kSereneDBConnector, connector_metadata);
  }
}

void PostgresFeature::unprepare() {
  folly::SingletonVault::singleton()->destroyInstancesFinal();
}

}  // namespace sdb::pg
