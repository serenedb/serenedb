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
#include <vpack/serializer.h>
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
#include "storage_engine/engine_feature.h"

namespace sdb::catalog {

Table::Table(const catalog::Table& other, NewOptions options)
  : SchemaObject{other.GetOwnerId(), other.GetDatabaseId(), other.GetSchemaId(),
                 other.GetId(),      options.name,          ObjectType::Table},
    _type{other.GetTableType()},
    _wait_for_sync{options.wait_for_sync},
    _shard_keys{other.shardKeys()},
    _columns{other._columns},
    _pk_columns{other._pk_columns},
    _check_constraints{other._check_constraints},
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
    auto it = absl::c_find_if(
      columns, [&](const Column& col) { return col.id == pk_col_id; });
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
    _check_constraints{std::move(options.checkConstraints)},
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
    _write_concern{options.writeConcern},
    _file_info{std::move(options.file_info)} {
  SDB_ASSERT(_shard_ids);

  _sharding_strategy = [&] -> std::unique_ptr<ShardingStrategy> {
    return ShardingStrategy::make(
      options.shardingStrategy,
      {.shard_keys = _shard_keys, .object_id = options.id});
  }();
  SDB_ASSERT(_sharding_strategy);
}

// NOLINTBEGIN
// Just a TableOptions but with views to light-weight serialize
struct Table::TableOutput {
  std::span<const std::string> shardKeys;
  std::span<const Column> columns;
  std::span<const Column::Id> pkColumns;
  std::span<const CheckConstraint> checkConstraints;
  std::string_view shardingStrategy;
  std::string_view name;
  // TODO make them just pointers if catalog::Table became immutable
  vpack::Nullable<std::shared_ptr<ValidatorBase>> schema;
  const KeyGenerator* keyOptions;
  std::shared_ptr<ShardMap> shards;
  Identifier id;
  ForeignId distributeShardsLike;
  Identifier planId;
  ObjectId planDb;
  ForeignId from;
  ForeignId to;
  uint32_t numberOfShards;
  uint32_t replicationFactor;
  uint32_t writeConcern;
  int type;
  bool waitForSync;
  FileInfo file_info;
};
// NOLINTEND

Table::TableOutput Table::MakeTableOptions() const {
  return {
    .shardKeys = _shard_keys,
    .columns = _columns,
    .pkColumns = _pk_columns,
    .checkConstraints = _check_constraints,
    .shardingStrategy = _sharding_strategy->name(),
    .name = GetName(),
    .schema = _schema,
    .keyOptions = _key_generator.get(),
    .shards = _shard_ids,
    .id = Identifier{GetId().id()},
    .distributeShardsLike = ForeignId{_distribute_shards_like.id()},
    .planId = Identifier{_plan_id.id()},
    .planDb = _plan_db,
    .from = ForeignId{_from.id()},
    .to = ForeignId{_to.id()},
    .numberOfShards = _number_of_shards,
    .replicationFactor = _replication_factor,
    .writeConcern = _write_concern,
    .type = std::to_underlying(_type),
    .waitForSync = _wait_for_sync,
    .file_info = _file_info,
  };
}

void catalog::Table::WriteProperties(vpack::Builder& build) const {
  SDB_ASSERT(build.isOpenObject());
  vpack::WriteObject(build, vpack::Embedded{MakeTableOptions()},
                     ObjectProperties{});
}

void catalog::Table::WriteInternal(vpack::Builder& build) const {
  SDB_ASSERT(build.isOpenObject());
  vpack::WriteObject(build, vpack::Embedded{MakeTableOptions()},
                     ObjectInternal{_database_id});
}

}  // namespace sdb::catalog
