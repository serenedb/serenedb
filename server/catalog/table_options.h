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

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <velox/type/Type.h>
#include <vpack/builder.h>
#include <vpack/slice.h>

#include <cstdint>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/fwd.h"
#include "basics/reboot_id.h"
#include "basics/static_strings.h"
#include "catalog/cluster_types.h"
#include "catalog/column_expr.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/identifier.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/key_generator.h"
#include "catalog/types.h"
#include "catalog/validators.h"
#include "pg/sql_collector.h"
#include "pg/sql_utils.h"
#include "utils/velox_vpack.h"

namespace sdb::catalog {

struct ObjectInternal {
  ObjectId database_id;
};

struct ObjectProperties {};

struct ForeignId : ObjectId {
  using ObjectId::ObjectId;
};

std::shared_ptr<catalog::Table> GetVertexByName(ObjectId database,
                                                std::string_view name);

void WriteTableName(vpack::Builder& b, ObjectId id);

bool VPackWriteHook(auto ctx, auto&&, ForeignId value) { return value.isSet(); }

template<typename Context>
void VPackWrite(Context ctx, ForeignId value) {
  static constexpr bool kIsInternal =
    std::is_same_v<typename Context::Arg, ObjectInternal>;

  auto& vpack = ctx.vpack();
  if constexpr (kIsInternal) {
    vpack.add(value.id());
  } else {
    WriteTableName(vpack, value);
  }
}

template<typename Context>
void VPackRead(Context ctx, ForeignId& value) {
  static constexpr bool kIsInternal =
    std::is_same_v<typename Context::Arg, ObjectInternal>;

  auto vpack = ctx.vpack();

  if (vpack.isNumber()) {
    // Cluster inventory
    // Cluster action
    // User action on single server
    // Open existing database cluster
    value = ForeignId{vpack.template getNumber<uint64_t>()};
  } else {
    if constexpr (!kIsInternal) {
      // Open existing database single
      // Tailing sync
      // Initial sync
      // Restore
      auto c = GetVertexByName(ctx.arg().database, vpack.stringView());
      if (!c) {
        SDB_THROW(ERROR_SERVER_DATA_SOURCE_NOT_FOUND,
                  "Object not found: ", vpack.stringView());
      }
      value = ForeignId{c->planId().id()};

    } else {
      SDB_ENSURE(false, ERROR_INTERNAL);
    }
  }
}

static constexpr std::string_view kDefaultSharding =
#ifdef SDB_CLUSTER
  "hash"
#else
  "none"
#endif
  ;

// NOLINTBEGIN
struct IndexProperties {
  std::string_view id;  // TODO(gnusi): must be ObjectId
  std::string_view type;
  std::string_view name;
  std::vector<std::string_view> fields;
  bool sparse = false;
  bool unique = true;
};

struct AgencyIsBuildingFlags {
  std::string coordinatorId;
  RebootId coordinatorRebootId = RebootId{0};
  bool isBuilding = true;
};

struct Column {
  enum GeneratedType : uint8_t { kNone = 0, kStored = 1, kVirtual = 2 };

  bool IsGenerated() const noexcept {
    return generated_type != GeneratedType::kNone;
  }

  using Id = uint64_t;

  Id id;
  velox::TypePtr type;
  std::string name;
  // if generated type is not kNone, default_value = generated expression
  ColumnExpr default_value;
  GeneratedType generated_type = GeneratedType::kNone;
};

struct CreateTableRequest {
  std::vector<std::string> shardKeys{std::string{StaticStrings::kKeyString}};
  std::vector<Column> columns;
  std::vector<Column::Id> pkColumns;
  // TOOD(gnusi): we don't need it to be a part of collection slice
  std::vector<std::string> avoidServers;
  std::optional<std::string> distributeShardsLike;
  std::optional<std::string> shardingStrategy;
  std::string_view name;
  std::string_view from;
  std::string_view to;
  std::optional<uint32_t> numberOfShards;
  std::optional<uint32_t> replicationFactor;
  std::optional<uint32_t> writeConcern;
  vpack::Slice planId;  // TODO(gnusi): remove?
  vpack::Slice planDb;  // TODO(gnusi): remove?
  vpack::Slice shards;  // TODO(gnusi): remove?
  vpack::Slice schema = vpack::Slice::nullSlice();
  vpack::Slice keyOptions = vpack::Slice::emptyObjectSlice();
  vpack::Slice indexes;
  std::string_view id;  // TODO(gnusi): make ObjectId
  int type = std::to_underlying(TableType::Document);
  bool waitForSync = false;
  bool deleted = false;  // TODO(gnusi): really needed?
};

struct TableOptions {
  std::vector<std::string> shardKeys{std::string{StaticStrings::kKeyString}};
  std::vector<Column> columns;
  std::vector<Column::Id> pkColumns;
  std::string shardingStrategy = std::string{kDefaultSharding};
  std::string name;
  std::shared_ptr<ValidatorBase> schema;
  std::shared_ptr<KeyGenerator> keyOptions;
  std::shared_ptr<ShardMap> shards = std::make_shared<ShardMap>();
  Identifier id;
  std::optional<ObjectId> distributeShardsLike;
  std::optional<Identifier> planId;  // TODO(gnusi): remove
  std::optional<ObjectId> planDb;    // TODO(gnusi): remove
  ForeignId from;
  ForeignId to;
  vpack::Slice indexes = vpack::Slice::emptyArraySlice();
  uint32_t numberOfShards = 1;
  uint32_t replicationFactor = 1;
  uint32_t writeConcern = 1;
  int type = std::to_underlying(TableType::Document);
  bool waitForSync = false;
};

struct CreateTableOptions : TableOptions {
  std::vector<std::string> avoidServers;
};

struct TableMeta {
  ObjectId database;
  ObjectId schema;
  ObjectId id;
  ObjectId plan_id;
  ObjectId plan_db;
  ObjectId from;
  ObjectId to;
  std::string name;  // TODO(gnusi): remove

  auto GetTarget(EdgeDirection dir) const noexcept {
    SDB_ASSERT(dir == EdgeDirection::Out || dir == EdgeDirection::In);
    return dir == EdgeDirection::Out ? to : from;
  }
  auto GetSource(EdgeDirection dir) const noexcept {
    SDB_ASSERT(dir == EdgeDirection::Out || dir == EdgeDirection::In);
    return dir == EdgeDirection::Out ? from : to;
  }
};

struct ModifyCollection {
  vpack::Slice schema;
  uint32_t numberOfShards = 1;
  uint32_t replicationFactor = 1;
  uint32_t writeConcern = 1;
  bool waitForSync = false;
};
// NOLINTEND

Result MakeTableOptions(CreateTableRequest&& request, ObjectId database_id,
                        CreateTableOptions& options,
                        uint32_t replication_factor, uint32_t write_concern,
                        bool enforce_replication_factor);

}  // namespace sdb::catalog
