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

#include <axiom/optimizer/FunctionRegistry.h>
#include <velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <velox/functions/prestosql/registration/RegistrationFunctions.h>
#include <velox/functions/prestosql/types/HyperLogLogRegistration.h>
#include <velox/functions/prestosql/types/IPAddressRegistration.h>
#include <velox/functions/prestosql/types/IPPrefixRegistration.h>
#include <velox/functions/prestosql/types/JsonRegistration.h>
#include <velox/functions/prestosql/types/QDigestRegistration.h>
#include <velox/functions/prestosql/types/SfmSketchRegistration.h>
#include <velox/functions/prestosql/types/TDigestRegistration.h>
#include <velox/functions/prestosql/types/TimeWithTimezoneRegistration.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneRegistration.h>
#include <velox/functions/prestosql/types/UuidRegistration.h>
#include <velox/functions/prestosql/window/WindowFunctionsRegistration.h>
#include <velox/functions/sparksql/aggregates/Register.h>
#include <velox/functions/sparksql/registration/Register.h>
#include <velox/functions/sparksql/window/WindowFunctionsRegistration.h>
#include <velox/type/Cost.h>
#include <velox/type/TypeCoercer.h>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/random/random_generator.h"
#include "connector/serenedb_connector.hpp"
#include "pg/functions.h"
#include "pg/system_catalog.h"
#include "query/types.h"
#include "rest_server/endpoint_feature.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_selector_feature.h"

namespace sdb::pg {

static constexpr std::string kConnectorId = "serenedb";

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

velox::AllowedCoercions AllowedCoercions() {
  velox::AllowedCoercions coercions;

  auto add = [&](const velox::TypePtr& from,
                 const std::vector<velox::TypePtr>& to) {
    velox::Cost cost = velox::kMinCoercionCost;
    for (const auto& to_type : to) {
      coercions.emplace(
        std::make_pair<std::string, std::string>(from->name(), to_type->name()),
        velox::Coercion{.type = to_type, .cost = ++cost});
    }
  };

  add(velox::TINYINT(), {velox::SMALLINT(), velox::INTEGER(), velox::BIGINT(),
                         velox::HUGEINT(), velox::REAL(), velox::DOUBLE()});
  add(velox::SMALLINT(), {velox::INTEGER(), velox::BIGINT(), velox::HUGEINT(),
                          velox::REAL(), velox::DOUBLE()});
  add(velox::INTEGER(),
      {velox::BIGINT(), velox::HUGEINT(), velox::REAL(), velox::DOUBLE()});
  add(velox::BIGINT(), {velox::HUGEINT(), velox::REAL(), velox::DOUBLE()});
  add(velox::REAL(), {velox::DOUBLE()});
  add(velox::DATE(), {velox::TIMESTAMP()});

  return coercions;
}

void PostgresFeature::prepare() {
  folly::SingletonVault::singleton()->registrationComplete();

  velox::memory::MemoryManager::initialize(
    velox::memory::MemoryManager::Options{});

  velox::Type::registerSerDe();

  // TODO(mbkkt) velox::registerGeometryType();
  velox::registerHyperLogLogType();
  velox::registerIPAddressType();
  velox::registerIPPrefixType();
  velox::registerJsonType();
  velox::registerQDigestType();
  velox::registerSfmSketchType();
  velox::registerTDigestType();
  velox::registerTimeWithTimezoneType();
  velox::registerTimestampWithTimeZoneType();
  velox::registerUuidType();

  velox::functions::sparksql::registerFunctions("spark_");
  velox::functions::aggregate::sparksql::registerAggregateFunctions("spark_");
  velox::functions::window::sparksql::registerWindowFunctions("spark_");

  // Make Presto functions override Spark functions if both are registered
  // as Presto is more SQL compliant.
  velox::functions::prestosql::registerAllScalarFunctions("presto_");
  velox::aggregate::prestosql::registerAllAggregateFunctions("presto_");
  velox::window::prestosql::registerAllWindowFunctions("presto_");
  axiom::optimizer::FunctionRegistry::registerPrestoFunctions("presto_");

  pg::RegisterTypes();
  pg::functions::registerFunctions("pg_");
  velox::TypeCoercer::registerCoercions(AllowedCoercions());
}

void PostgresFeature::start() {
  pg::RegisterSystemViews();
  auto& selector = server().getFeature<EngineSelectorFeature>();
  if (selector.isRocksDB() && (ServerState::instance()->IsDBServer() ||
                               ServerState::instance()->IsSingle())) {
    auto& engine = GetServerEngineAs<RocksDBEngineCatalog>();
    auto* cf =
      RocksDBColumnFamilyManager::get(RocksDBColumnFamilyManager::Family::Data);
    SDB_ASSERT(cf);

    auto connector = std::make_shared<connector::SereneDBConnector>(
      kConnectorId, nullptr, *engine.db(), *cf);
    velox::connector::registerConnector(std::move(connector));
    auto connector_metadata =
      std::make_shared<connector::SereneDBConnectorMetadata>();
    axiom::connector::ConnectorMetadata::registerMetadata(kConnectorId,
                                                          connector_metadata);
  }
}

void PostgresFeature::unprepare() {
  folly::SingletonVault::singleton()->destroyInstancesFinal();
}

}  // namespace sdb::pg
