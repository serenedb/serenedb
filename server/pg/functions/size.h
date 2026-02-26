#pragma once

#include <velox/functions/Macros.h>
#include <velox/functions/prestosql/json/JsonStringUtil.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/type/SimpleFunctionApi.h>

#include <string>

#include "basics/fwd.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "pg/connection_context.h"
#include "pg/functions/json.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::pg {

template<typename T>
struct PgDatabaseSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<int64_t>& result, const arg_type<velox::Varchar>& input) {
    std::string_view database_name = input;
    auto snapshot = catalog::GetCatalog().GetSnapshot();
    auto database = snapshot->GetDatabase(database_name);
    if (!database) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_NAME),
        ERR_MSG("database \"", database_name, "\" does not exist"));
    }
    result = GetServerEngine().GetDatabaseSize(*snapshot, database->GetId());
  }
};

template<typename T>
struct PgTableSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(  // NOLINT
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>* /*input*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    db_id = conn->GetDatabaseId();
    current_schema = conn->GetCurrentSchema();
  }

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<int64_t>& result, const arg_type<velox::Varchar>& input) {
    std::string_view name = input;
    auto pos = name.find('.');
    std::string_view schema_name =
      pos == std::string_view::npos ? current_schema : name.substr(0, pos);
    std::string_view table_name =
      pos == std::string_view::npos ? name : name.substr(pos + 1);
    auto snapshot = catalog::GetCatalog().GetSnapshot();
    auto table = snapshot->GetTable(db_id, schema_name, table_name);
    if (!table) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                      ERR_MSG("relation \"", table_name, "\" does not exist"));
    }
    result = GetServerEngine().GetTableSize(table->GetId());
  }
  ObjectId db_id;
  std::string current_schema;
};

template<typename T>
struct PgSchemaSize {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void initialize(  // NOLINT
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>* /*input*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    db_id = conn->GetDatabaseId();
  }

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<int64_t>& result, const arg_type<velox::Varchar>& input) {
    std::string_view schema_name = input;
    auto snapshot = catalog::GetCatalog().GetSnapshot();
    if (!snapshot->GetSchema(db_id, schema_name)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_NAME),
                      ERR_MSG("schema \"", schema_name, "\" does not exist"));
    }
    result = GetServerEngine().GetSchemaSize(*snapshot, db_id, schema_name);
  }
  ObjectId db_id;
};

}  // namespace sdb::pg
