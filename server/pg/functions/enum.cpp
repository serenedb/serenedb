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

#include "pg/functions/enum.h"

#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>

#include <string_view>

#include "catalog/catalog.h"
#include "catalog/enum_type.h"
#include "pg/connection_context.h"
#include "pg/sql_exception_macro.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {

// pg_enum_in(label VARCHAR, enum_name VARCHAR) -> DOUBLE
// Looks up the enum definition from the catalog in initialize(),
// then converts labels to ordinals in call().
template<typename T>
struct PgEnumIn {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void initialize(const std::vector<velox::TypePtr>&,
                  const velox::core::QueryConfig& config,
                  const arg_type<velox::Varchar>*,
                  const arg_type<velox::Varchar>* enum_name_ptr) {
    auto* ctx =
      basics::downCast<const ConnectionContext>(config.config().get());
    auto snapshot = ctx->EnsureCatalogSnapshot();
    auto db_id = ctx->GetDatabaseId();
    auto current_schema = ctx->GetCurrentSchema();
    std::string_view name{enum_name_ptr->data(), enum_name_ptr->size()};
    _enum_type = snapshot->GetEnumType(db_id, current_schema, name);
    SDB_ASSERT(_enum_type);
  }

  FOLLY_ALWAYS_INLINE bool call(double& result,
                                const arg_type<velox::Varchar>& label,
                                const arg_type<velox::Varchar>&) {
    std::string_view val{label.data(), label.size()};
    auto ordinal = EnumLabelToOrdinal(_enum_type->GetLabels(), val);
    if (!ordinal) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                      ERR_MSG("invalid input value for enum ",
                              _enum_type->GetName(), ": \"", val, "\""));
    }
    result = *ordinal;
    return true;
  }

 private:
  std::shared_ptr<catalog::EnumType> _enum_type;
};

// pg_enum_out(ordinal DOUBLE, enum_name VARCHAR) -> VARCHAR
// Looks up the enum definition from the catalog in initialize(),
// then converts ordinals to labels in call().
template<typename T>
struct PgEnumOut {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void initialize(const std::vector<velox::TypePtr>&,
                  const velox::core::QueryConfig& config,
                  const double*,
                  const arg_type<velox::Varchar>* enum_name_ptr) {
    auto* ctx =
      basics::downCast<const ConnectionContext>(config.config().get());
    auto snapshot = ctx->EnsureCatalogSnapshot();
    auto db_id = ctx->GetDatabaseId();
    auto current_schema = ctx->GetCurrentSchema();
    std::string_view name{enum_name_ptr->data(), enum_name_ptr->size()};
    _enum_type = snapshot->GetEnumType(db_id, current_schema, name);
    SDB_ASSERT(_enum_type);
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>& result,
                                double ordinal,
                                const arg_type<velox::Varchar>&) {
    auto label = EnumOrdinalToLabel(_enum_type->GetLabels(), ordinal);
    if (!label) {
      return false;
    }
    result.resize(label->size());
    std::memcpy(result.data(), label->data(), label->size());
    return true;
  }

 private:
  std::shared_ptr<catalog::EnumType> _enum_type;
};

void registerEnumFunctions(const std::string& prefix) {
  velox::registerFunction<PgEnumIn, double, velox::Varchar, velox::Varchar>(
    {prefix + "enum_in"});
  velox::registerFunction<PgEnumOut, velox::Varchar, double, velox::Varchar>(
    {prefix + "enum_out"});
}

}  // namespace sdb::pg::functions
