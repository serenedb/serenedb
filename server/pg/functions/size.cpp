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

#include "basics/fwd.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/sql_exception_macro.h"
#include "query/types.h"
#include "storage_engine/engine_feature.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

int64_t GetRelationForkSize(const catalog::Snapshot& snapshot, uint64_t oid,
                            std::string_view fork, bool table_only = false) {
  auto rel = snapshot.GetObject(ObjectId{oid});
  if (!rel) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation with OID ", oid, " does not exist"));
  }
  if (table_only && rel->GetType() != catalog::ObjectType::Table) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", rel->GetName(), "\" is not a table"));
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

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input) {
    std::string_view database_name = input;
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    auto database = snapshot->GetDatabase(database_name);
    if (!database) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_NAME),
        ERR_MSG("database \"", database_name, "\" does not exist"));
    }
    result = GetServerEngine().GetDatabaseSize(*snapshot, database->GetId());
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgDatabaseSizeOid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input) {
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    auto database = snapshot->GetDatabase(static_cast<ObjectId>(input));
    if (!database) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                      ERR_MSG("database with OID ", input, " does not exist"));
    }
    result = GetServerEngine().GetDatabaseSize(*snapshot, database->GetId());
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgTableSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input) {
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    result = GetRelationForkSize(*snapshot, static_cast<uint64_t>(input),
                                 "main", true);
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgSchemaSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input) {
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    auto schema = snapshot->GetObject<catalog::Schema>(
      ObjectId{static_cast<uint64_t>(input)});
    if (!schema) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                      ERR_MSG("schema with OID ", input, " does not exist"));
    }
    result = GetServerEngine().GetSchemaSize(*snapshot, schema->GetDatabaseId(),
                                             schema->GetName());
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgRelationSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input) {
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    result =
      GetRelationForkSize(*snapshot, static_cast<uint64_t>(input), "main");
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgRelationSizeWithFork {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*,
                                      const arg_type<velox::Varchar>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input,
                                const arg_type<velox::Varchar>& fork) {
    std::string_view fork_name = fork;
    if (fork_name != "main" && fork_name != "fsm" && fork_name != "vm" &&
        fork_name != "init") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("invalid fork name"));
    }
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    result =
      GetRelationForkSize(*snapshot, static_cast<uint64_t>(input), fork_name);
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgTotalRelationSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>& input) {
    auto snapshot = _ctx->EnsureCatalogSnapshot();
    result =
      GetRelationForkSize(*snapshot, static_cast<uint64_t>(input), "main");
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgIndexesSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<int64_t>&) {
    // In RocksDB, indexes are embedded in the LSM tree alongside table data,
    // so we cannot separate index size from table size.
    result = 0;
  }
};

template<typename T>
struct PgTablespaceSizeOid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&, const arg_type<int64_t>&) {
    return false;  // NULL: SereneDB has no tablespaces
  }
};

template<typename T>
struct PgTablespaceSizeName {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // NULL: SereneDB has no tablespaces
  }
};

template<typename T>
struct PgColumnSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int32_t>&,
                                const arg_type<velox::Any>&) {
    return false;  // NULL: requires runtime type inspection
  }
};

template<typename T>
struct PgColumnCompression {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<velox::Any>&) {
    return false;  // NULL: SereneDB does not use TOAST compression
  }
};

template<typename T>
struct PgColumnToastChunkId {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&,
                                const arg_type<velox::Any>&) {
    return false;  // NULL: SereneDB does not use TOAST
  }
};

template<typename T>
struct PgSizeBytes {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // NULL: not yet implemented
  }
};

template<typename T>
struct PgSizePretty {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&) {
    return false;  // NULL: not yet implemented
  }
};

}  // namespace

void registerSizeFunctions(const std::string& prefix) {
  velox::registerFunction<PgColumnSize, int32_t, velox::Any>(
    {prefix + "column_size"});
  velox::registerFunction<PgColumnCompression, velox::Varchar, velox::Any>(
    {prefix + "column_compression"});
  velox::registerFunction<PgColumnToastChunkId, OidCustomType, velox::Any>(
    {prefix + "column_toast_chunk_id"});
  velox::registerFunction<PgDatabaseSize, int64_t, NameCustomType>(
    {prefix + "database_size"});
  velox::registerFunction<PgDatabaseSizeOid, int64_t, OidCustomType>(
    {prefix + "database_size"});
  velox::registerFunction<PgIndexesSize, int64_t, RegclassCustomType>(
    {prefix + "indexes_size"});
  velox::registerFunction<PgRelationSize, int64_t, RegclassCustomType>(
    {prefix + "relation_size"});
  velox::registerFunction<PgRelationSizeWithFork, int64_t, RegclassCustomType,
                          velox::Varchar>({prefix + "relation_size"});
  velox::registerFunction<PgSizeBytes, int64_t, velox::Varchar>(
    {prefix + "size_bytes"});
  velox::registerFunction<PgSizePretty, velox::Varchar, int64_t>(
    {prefix + "size_pretty"});
  // TODO: pg_size_pretty(numeric) -> text
  velox::registerFunction<PgTableSize, int64_t, RegclassCustomType>(
    {prefix + "table_size"});
  velox::registerFunction<PgTablespaceSizeName, int64_t, NameCustomType>(
    {prefix + "tablespace_size"});
  velox::registerFunction<PgTablespaceSizeOid, int64_t, OidCustomType>(
    {prefix + "tablespace_size"});
  velox::registerFunction<PgTotalRelationSize, int64_t, RegclassCustomType>(
    {prefix + "total_relation_size"});

  // pg_schema_size is not a standard PostgreSQL function, but we include it for
  // convenience. It returns the total size of all objects in the schema, which
  // is a common use case that would otherwise require summing table_size and
  // index sizes for all tables in the schema.
  velox::registerFunction<PgSchemaSize, int64_t, RegnamespaceCustomType>(
    {prefix + "schema_size"});
}

}  // namespace sdb::pg::functions
