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
#include "pg/system_catalog.h"
#include "query/duckdb_engine.h"
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

void PostgresFeature::validateOptions() {
  const auto& endpoint_list =
    server().getFeature<EndpointFeature>().endpointList();
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
  query::DuckDBEngine::Instance().Initialize();
}

void PostgresFeature::start() {
  if (ServerState::instance()->IsDBServer() ||
      ServerState::instance()->IsSingle()) {
    auto* cf = RocksDBColumnFamilyManager::get(
      RocksDBColumnFamilyManager::Family::Default);
    SDB_ASSERT(cf);
  }
}

void PostgresFeature::unprepare() {
  query::DuckDBEngine::Instance().Shutdown();
}

}  // namespace sdb::pg
