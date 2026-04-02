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

#pragma once

#include <velox/type/Type.h>
#include <vpack/slice.h>

#include "basics/fwd.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "catalog/validators.h"
#include "general_server/state.h"

namespace sdb {

// Read from storage engine if unknown
static constexpr auto kRead = std::numeric_limits<uint64_t>::max();

}  // namespace sdb
namespace sdb::catalog {

struct NewOptions {
  std::string_view name;
  std::shared_ptr<ValidatorBase> schema;
  uint32_t number_of_shards = 1;
  uint32_t replication_factor = 1;
  uint32_t write_concern = 1;
  bool wait_for_sync = false;
};

NewOptions ParseTableChange(vpack::Slice slice);

class Table : public SchemaObject {
 public:
  Table(TableOptions&& options, ObjectId database_id);
  Table(const catalog::Table& other, NewOptions options);

  void WriteProperties(vpack::Builder& build) const final;

  void WriteInternal(vpack::Builder& build) const final;

  const auto& Columns() const noexcept { return _columns; }
  const auto& PKColumns() const noexcept { return _pk_columns; }
  const auto& CheckConstraints() const noexcept { return _check_constraints; }
  auto GetTableType() const noexcept { return _type; }
  auto& GetSchema() const noexcept { return _schema; }
  auto& sharding(this auto& self) noexcept { return self._sharding; }
  bool waitForSync() const noexcept { return _wait_for_sync; }
  auto& keyGenerator() const noexcept {
    SDB_ASSERT(_key_generator);
    return *_key_generator;
  }
  auto from() const noexcept { return _from; }
  auto to() const noexcept { return _to; }
  auto planId() const noexcept { return _plan_id; }
  auto planDb() const noexcept { return _plan_db; }
  auto numberOfShards() const noexcept { return _number_of_shards; }
  auto replicationFactor() const noexcept { return _replication_factor; }
  auto writeConcern() const noexcept { return _write_concern; }
  auto& shardKeys() const noexcept { return _shard_keys; }
  auto& distributeShardsLike() const noexcept {
    return _distribute_shards_like;
  }
  auto& shardIds() const noexcept { return _shard_ids; }
  auto& shardingStrategy() const noexcept {
    SDB_ASSERT(_sharding_strategy);
    return *_sharding_strategy;
  }
  const auto& GetFileInfo() const noexcept { return _file_info; }
#ifdef SDB_GTEST
  // TODO(gnusi): remove
  void setShardMap(std::shared_ptr<ShardMap> map) {
    SDB_ASSERT(map);
    _shard_ids = std::move(map);
    _number_of_shards = _shard_ids->size();
  }
#endif

  const auto& PKType() const noexcept { return _lookup_cache.pk_type; }
  const auto& RowType() const noexcept { return _lookup_cache.row_type; }

  using NameToColumnMap =
    containers::FlatHashMap<std::string_view, const catalog::Column*>;

  const auto& NameToColumn() const noexcept {
    return _lookup_cache.name2column;
  }
  using IdToColumnMap =
    containers::FlatHashMap<catalog::Column::Id, const catalog::Column*>;

  const auto& IdToColumn() const noexcept { return _lookup_cache.id2column; }

 private:
  struct TableOutput;
  TableOutput MakeTableOptions() const;

  struct LookupCache {
    LookupCache(std::span<const catalog::Column> columns,
                std::span<const catalog::Column::Id> pk_columns);

    NameToColumnMap name2column;
    IdToColumnMap id2column;
    velox::RowTypePtr pk_type;
    velox::RowTypePtr row_type;
  };

  const TableType _type = TableType::Unknown;
  bool _wait_for_sync = false;
  std::vector<std::string> _shard_keys;
  std::vector<Column> _columns;
  std::vector<Column::Id> _pk_columns;
  std::vector<CheckConstraint> _check_constraints;
  const ObjectId _plan_id;
  const ObjectId _plan_db;
  ObjectId _distribute_shards_like;
  ObjectId _from;
  ObjectId _to;
  std::shared_ptr<KeyGenerator> _key_generator;
  std::shared_ptr<ShardingStrategy> _sharding_strategy;
  std::shared_ptr<ValidatorBase> _schema;
  // name of other table this table's shards should be distributed like
  std::shared_ptr<ShardMap> _shard_ids = std::make_shared<ShardMap>();
  uint32_t _number_of_shards = 1;
  uint32_t _replication_factor = 1;
  // writes will be disallowed if we know we cannot fulfill it.
  // _write_concern <= _replication_factor
  uint32_t _write_concern = 1;
  FileInfo _file_info;

  LookupCache _lookup_cache;
};

}  // namespace sdb::catalog
