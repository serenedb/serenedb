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

#include "table.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/slice.h>
#include <vpack/utf8_helper.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/misc.hpp"
#include "basics/static_strings.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/key_generator.h"
#include "catalog/sharding_strategy.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "catalog/validators.h"
#include "general_server/server_options_feature.h"
#include "general_server/state.h"
#include "storage_engine/engine_selector_feature.h"
#include "vpack/serializer.h"

#ifdef SDB_CLUSTER
#include "cluster/cluster_feature.h"
#include "cluster/cluster_info.h"
#endif

namespace sdb::catalog {

Table::Table(const catalog::Table& other, NewOptions options)
  : SchemaObject{other.GetOwnerId(), other.GetDatabaseId(), other.GetSchemaId(),
                 other.GetId(),      options.name,          ObjectType::Table},
    _type{other.GetTableType()},
    _wait_for_sync{options.wait_for_sync},
    _shard_keys{other.shardKeys()},
    _columns{other._columns},
    _pk_columns{other._pk_columns},
    _pk_type{other._pk_type},
    _row_type{other._row_type},
    _plan_id{other.planId()},
    _plan_db{other.planDb()},
    _distribute_shards_like{other.distributeShardsLike()},
    _from{other.from()},
    _to{other.to()},
    _key_generator{other._key_generator},
    _sharding_strategy{other._sharding_strategy},
    _schema{std::move(options.schema)},
    _shard_ids{other._shard_ids},
    _number_of_shards{options.number_of_shards},
    _replication_factor{options.replication_factor},
    _write_concern{options.write_concern} {}

velox::RowTypePtr BuildPkType(const std::vector<Column>& columns,
                              const std::vector<Column::Id>& pk_columns) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(pk_columns.size());
  types.reserve(pk_columns.size());

  for (auto pk_col_id : pk_columns) {
    auto it = std::find_if(
      columns.begin(), columns.end(),
      [pk_col_id](const Column& col) { return col.id == pk_col_id; });
    SDB_ASSERT(it != columns.end());
    names.push_back(it->name);
    types.push_back(it->type);
  }

  return velox::ROW(std::move(names), std::move(types));
}

velox::RowTypePtr BuildRowType(const std::vector<Column>& columns) {
  std::vector<std::string> names;
  std::vector<velox::TypePtr> types;
  names.reserve(columns.size());
  types.reserve(columns.size());
  for (const auto& col : columns) {
    names.push_back(col.name);
    types.push_back(col.type);
  }

  return velox::ROW(std::move(names), std::move(types));
}

Table::Table(TableOptions&& options, ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 options.id,
                 std::move(options.name),
                 ObjectType::Table},
    _type{static_cast<TableType>(options.type)},
    _wait_for_sync{options.waitForSync},
    _shard_keys{std::move(options.shardKeys)},
    _columns{std::move(options.columns)},
    _pk_columns{std::move(options.pkColumns)},
    _pk_type{BuildPkType(_columns, _pk_columns)},
    _row_type{BuildRowType(_columns)},
    _plan_id{[&] {
      auto plan_id = options.planId.value_or(Identifier{});
      return plan_id.isSet() ? plan_id : GetId();
    }()},
    _plan_db{[&] {
      auto plan_db = options.planDb.value_or(ObjectId{});
      return plan_db.isSet() ? plan_db : database_id;
    }()},
    _distribute_shards_like{options.distributeShardsLike.value_or(ObjectId{})},
    _from{options.from},
    _to{options.to},
    _key_generator{std::move(options.keyOptions)},
    _schema{std::move(options.schema)},
    _shard_ids{std::move(options.shards)},
    _number_of_shards{options.numberOfShards},
    _replication_factor{options.replicationFactor},
    _write_concern{options.writeConcern} {
  SDB_ASSERT(_shard_ids);

  _sharding_strategy = [&] -> std::unique_ptr<ShardingStrategy> {
#ifdef SDB_CLUSTER
#ifdef SDB_GTEST
    if (!ServerState::instance()->IsClusterNode() &&
        SerenedServer::Instance()
            .getFeature<EngineSelectorFeature>()
            .engine()
            .typeName() == "Mock") {
      return std::make_unique<ShardingStrategyNone>();
    }
#endif
#endif
    return ShardingStrategy::make(
      options.shardingStrategy,
      {.shard_keys = _shard_keys, .object_id = options.id});
  }();
  SDB_ASSERT(_sharding_strategy);
}

TableOptions Table::MakeTableOptions() const {
  return {
    .shardKeys = _shard_keys,
    .columns = _columns,
    .pkColumns = _pk_columns,
    .shardingStrategy = std::string{_sharding_strategy->name()},
    .name = std::string{GetName()},
    .schema = _schema,
    .keyOptions = _key_generator,
    .shards = _shard_ids,
    .id = Identifier{GetId().id()},
    .distributeShardsLike = _distribute_shards_like,
    .planId = Identifier{_plan_id.id()},
    .planDb = _plan_db,
    .from = ForeignId{_from.id()},
    .to = ForeignId{_to.id()},
    .numberOfShards = _number_of_shards,
    .replicationFactor = _replication_factor,
    .writeConcern = _write_concern,
    .type = std::to_underlying(_type),
    .waitForSync = _wait_for_sync,
  };
}

void catalog::Table::WriteProperties(vpack::Builder& build) const {
  SDB_ASSERT(build.isOpenObject());
  vpack::WriteObject(build, vpack::Embedded{MakeTableOptions()},
                     ObjectProperties{});
}

void catalog::Table::WriteInternal(vpack::Builder& build) const {
  // TODO(gnusi) writeTuple?
  SDB_ASSERT(build.isOpenObject());
  vpack::WriteObject(build, vpack::Embedded{MakeTableOptions()},
                     ObjectInternal{_database_id});
}

Result ChangeTableHelper(const catalog::Table& old_collection,
                         vpack::Slice props,
                         std::shared_ptr<catalog::Table>& new_collection) {
  ModifyCollection request{
    .numberOfShards = old_collection.numberOfShards(),
    .replicationFactor = old_collection.replicationFactor(),
    .writeConcern = old_collection.writeConcern(),
    .waitForSync = old_collection.waitForSync(),
  };
  auto r = vpack::ReadObjectNothrow(props, request,
                                    {.skip_unknown = true, .strict = false});

  if (!r.ok()) {
    return r;
  }

  if (ServerState::instance()->IsClientNode()) {
    if (request.replicationFactor == 0) {
      if (old_collection.replicationFactor() != 0) {
        return {ERROR_BAD_PARAMETER,
                "cannot change collection to be redundant"};
      }
      if (request.writeConcern != old_collection.writeConcern()) {
        return {ERROR_FORBIDDEN,
                "cannot change writeConcern for redundant collection"};
      }

      SDB_ASSERT(request.writeConcern == 0);
    } else if (ServerState::instance()->IsCoordinator()) {
#ifdef SDB_CLUSTER
      auto& cluster = SerenedServer::Instance().getFeature<ClusterFeature>();

      // replication checks
      if (const auto nodes_count =
            cluster.clusterInfo().getCurrentDBServers().size();
          request.replicationFactor > nodes_count ||
          request.writeConcern > nodes_count) {
        return {ERROR_CLUSTER_INSUFFICIENT_DBSERVERS};
      }

      const auto& server_options = GetServerOptions();

      if (request.replicationFactor <
            server_options.cluster_min_replication_factor ||
          request.replicationFactor >
            server_options.cluster_max_replication_factor) {
        return {
          ERROR_BAD_PARAMETER,
          "bad value for replicationFactor. replicationFactor must be between ",
          server_options.cluster_min_replication_factor, " and ",
          server_options.cluster_max_replication_factor};
      }

      if (request.replicationFactor != old_collection.replicationFactor()) {
        if (old_collection.distributeShardsLike().isSet()) {
          return {ERROR_FORBIDDEN,
                  "cannot change replicationFactor for a collection using "
                  "'distributeShardsLike'"};
        } else if (old_collection.replicationFactor() == 0) {
          return {ERROR_FORBIDDEN,
                  "cannot change replicationFactor of a redundant collection"};
        }
      }

      if (request.writeConcern > request.replicationFactor) {
        SDB_THROW(ERROR_BAD_PARAMETER,
                  "replicationFactor cannot be smaller than writeConcern (",
                  request.replicationFactor, " < ", request.writeConcern, ")");
      }

      if (auto r = catalog::ValidateShardsAndReplicationFactor(props, false);
          !r.ok()) {
        return r;
      }
#endif
    }
  }

  NewOptions options{
    .name = old_collection.GetName(),
    .schema = old_collection.GetSchema(),
    .number_of_shards = request.numberOfShards,
    .replication_factor = request.replicationFactor,
    .write_concern = request.writeConcern,
    .wait_for_sync = request.waitForSync,
  };

  if (auto schema = request.schema; !schema.isNone()) {
    if (schema.isNull()) {
      schema = vpack::Slice::emptyObjectSlice();
    }
    if (!schema.isObject()) {
      return {ERROR_VALIDATION_BAD_PARAMETER,
              "Schema description is not an object."};
    }

    auto r = ValidatorJsonSchema::buildInstance(schema);

    if (!r) {
      return std::move(r).error();
    }

    options.schema = std::move(*r);
  }

  new_collection =
    std::make_shared<catalog::Table>(old_collection, std::move(options));
  return {};
}

Result ValidateShardsAndReplicationFactor(vpack::Slice slice,
                                          bool enforce_replication_factor) {
  if (slice.isObject()) {
    const auto& server_options = GetServerOptions();

    auto number_of_shards_slice = slice.get(StaticStrings::kNumberOfShards);
    if (number_of_shards_slice.isNumber()) {
      const uint32_t max_number_of_shards =
        server_options.cluster_max_number_of_shards;
      uint32_t number_of_shards = number_of_shards_slice.getNumber<uint32_t>();
      if (max_number_of_shards > 0 && number_of_shards > max_number_of_shards) {
        return {ERROR_CLUSTER_TOO_MANY_SHARDS,
                "too many shards. maximum number of shards is ",
                max_number_of_shards};
      }
    }

    auto write_concern_slice = slice.get(StaticStrings::kWriteConcern);
    if (enforce_replication_factor) {
      auto enforce_slice = slice.get("enforceReplicationFactor");
      if (!enforce_slice.isBool() || enforce_slice.getBool()) {
        auto replication_factor_slice =
          slice.get(StaticStrings::kReplicationFactor);
        if (replication_factor_slice.isNumber()) {
          int64_t replication_factor_probe =
            replication_factor_slice.getNumber<int64_t>();
          if (replication_factor_probe == 0) {
            return {};
          }
          if (replication_factor_probe < 0) {
            return {ERROR_BAD_PARAMETER, "invalid value for replicationFactor"};
          }

          const uint32_t min_replication_factor =
            server_options.cluster_min_replication_factor;
          const uint32_t max_replication_factor =
            server_options.cluster_max_replication_factor;
          uint32_t replication_factor =
            replication_factor_slice.getNumber<uint32_t>();

          // make sure the replicationFactor value is between the configured min
          // and max values
          if (replication_factor > max_replication_factor &&
              max_replication_factor > 0) {
            return {ERROR_BAD_PARAMETER,
                    absl::StrCat("replicationFactor must not be higher than "
                                 "maximum allowed replicationFactor (",
                                 max_replication_factor, ")")};
          } else if (replication_factor < min_replication_factor &&
                     min_replication_factor > 0) {
            return {ERROR_BAD_PARAMETER,
                    absl::StrCat("replicationFactor must not be lower than "
                                 "minimum allowed replicationFactor (",
                                 min_replication_factor, ")")};
          }
#ifdef SDB_CLUSTER
          // make sure we have enough servers available for the replication
          // factor
          auto& cl = SerenedServer::Instance().getFeature<ClusterFeature>();
          if (ServerState::instance()->IsCoordinator() &&
              replication_factor >
                cl.clusterInfo().getCurrentDBServers().size()) {
            return Result(ERROR_CLUSTER_INSUFFICIENT_DBSERVERS);
          }
#endif
        }

        if (!replication_factor_slice.isString()) {
          if (write_concern_slice.isNumber()) {
            int64_t write_concern = write_concern_slice.getNumber<int64_t>();
            if (write_concern <= 0) {
              return {ERROR_BAD_PARAMETER, "invalid value for writeConcern"};
            }
#ifdef SDB_CLUSTER
            auto& cl = SerenedServer::Instance().getFeature<ClusterFeature>();
            if (ServerState::instance()->IsCoordinator() &&
                static_cast<size_t>(write_concern) >
                  cl.clusterInfo().getCurrentDBServers().size()) {
              return {ERROR_CLUSTER_INSUFFICIENT_DBSERVERS};
            }
#endif

            if (replication_factor_slice.isNumber() &&
                write_concern > replication_factor_slice.getNumber<int64_t>()) {
              auto replication_factor =
                replication_factor_slice.getNumber<int64_t>();

              return {ERROR_BAD_PARAMETER,
                      "replicationFactor cannot be smaller than writeConcern (",
                      replication_factor,
                      " < ",
                      write_concern,
                      ")"};
            }
          }
        }
      }
    }
  }

  return {};
}

}  // namespace sdb::catalog
