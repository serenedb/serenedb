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

#include "pg/functions/size.h"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/type/SimpleFunctionApi.h>

#include <string>

#include "basics/down_cast.h"
#include "basics/fwd.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "pg/connection_context.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception_macro.h"
#include "query/types.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

int64_t GetRelationForkSize(const catalog::Snapshot& snapshot, uint64_t oid,
                            std::string_view fork) {
  auto rel = snapshot.GetObject(ObjectId{oid});
  if (!rel) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation with OID ", oid, " does not exist"));
  }
  if (fork != "main") {
    return 0;
  }
  switch (rel->GetType()) {
    case catalog::ObjectType::Table:
      return GetServerEngine().GetTableSize(rel->GetId());
    case catalog::ObjectType::Index: {
      auto shard = snapshot.GetIndexShard(rel->GetId());
      SDB_ASSERT(shard);
      SDB_ASSERT(shard->GetType() == IndexType::Inverted);
      return static_cast<int64_t>(
        basics::downCast<search::InvertedIndexShard>(shard.get())
          ->GetStats()
          .indexSize);
    }
    default:
      SDB_UNREACHABLE();
  }
}

template<typename T>
struct PgDatabaseSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>& /*input*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    snapshot = conn->EnsureCatalogSnapshot();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input) {
    std::string_view database_name = input;
    auto database = snapshot->GetDatabase(database_name);
    if (!database) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_NAME),
        ERR_MSG("database \"", database_name, "\" does not exist"));
    }
    result = GetServerEngine().GetDatabaseSize(*snapshot, database->GetId());
  }

  std::shared_ptr<const catalog::Snapshot> snapshot;
};

template<typename T>
struct PgTableSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>* /*input*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    db_id = conn->GetDatabaseId();
    current_schema = conn->GetCurrentSchema();
    snapshot = conn->EnsureCatalogSnapshot();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input) {
    auto object_name = ParseObjectName(input, current_schema);
    auto table =
      snapshot->GetTable(db_id, object_name.schema, object_name.relation);
    if (!table) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_NAME),
        ERR_MSG("relation \"", object_name.relation, "\" does not exist"));
    }
    result = GetServerEngine().GetTableSize(table->GetId());
  }

  ObjectId db_id;
  std::string current_schema;
  std::shared_ptr<const catalog::Snapshot> snapshot;
};

template<typename T>
struct PgSchemaSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>* /*input*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    db_id = conn->GetDatabaseId();
    snapshot = conn->EnsureCatalogSnapshot();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input) {
    std::string_view schema_name = input;
    if (!snapshot->GetSchema(db_id, schema_name)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                      ERR_MSG("schema \"", schema_name, "\" does not exist"));
    }
    result = GetServerEngine().GetSchemaSize(*snapshot, db_id, schema_name);
  }

  ObjectId db_id;
  std::shared_ptr<const catalog::Snapshot> snapshot;
};

template<typename T>
struct PgRelationSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    snapshot = conn->EnsureCatalogSnapshot();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input) {
    result =
      GetRelationForkSize(*snapshot, static_cast<uint64_t>(input), "main");
  }

  std::shared_ptr<const catalog::Snapshot> snapshot;
};

template<typename T>
struct PgRelationSizeWithFork {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*,
                                      const arg_type<velox::Varchar>*) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    snapshot = conn->EnsureCatalogSnapshot();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input,
                                const arg_type<velox::Varchar>& fork) {
    result = GetRelationForkSize(*snapshot, static_cast<uint64_t>(input),
                                 std::string_view{fork.data(), fork.size()});
  }

  std::shared_ptr<const catalog::Snapshot> snapshot;
};

}  // namespace

void registerSizeFunctions(const std::string& prefix) {
  velox::registerFunction<PgDatabaseSize, int64_t, velox::Varchar>(
    {prefix + "database_size"});
  velox::registerFunction<PgSchemaSize, int64_t, velox::Varchar>(
    {prefix + "schema_size"});
  velox::registerFunction<PgTableSize, int64_t, velox::Varchar>(
    {prefix + "table_size"});
  velox::registerFunction<PgRelationSize, int64_t, pg::RegclassCustomType>(
    {prefix + "relation_size"});
  velox::registerFunction<PgRelationSizeWithFork, int64_t,
                          pg::RegclassCustomType, velox::Varchar>(
    {prefix + "relation_size"});
}

}  // namespace sdb::pg::functions
