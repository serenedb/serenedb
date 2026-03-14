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
#include "pg/sql_collector.h"
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
    auto object_name = ParseObjectName(input, current_schema);
    auto snapshot = catalog::GetCatalog().GetSnapshot();
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
