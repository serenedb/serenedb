////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "pg/functions/system.h"

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <velox/expression/DecodedArgs.h>
#include <velox/expression/FunctionMetadata.h>
#include <velox/expression/VectorFunction.h>
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/vector/ComplexVector.h>

#include "basics/down_cast.h"
#include "basics/fwd.h"
#include "basics/static_strings.h"
#include "pg/connection_context.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/types.h"
#include "rest/version.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

template<typename T>
struct GetUserByIdFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    if (input == id::kRootUser) {
      result = StaticStrings::kDefaultUser;
    } else {
      result = absl::StrCat("unknown (OID=", input, ")");
    }
  }
};

template<typename T>
struct CurrentSchemaFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config) {
    auto conn_ctx = basics::downCast<const ConnectionContext>(config.config());
    SDB_ASSERT(conn_ctx);
    _schema_name = conn_ctx->GetCurrentSchema();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) {
    out = _schema_name;
  }

 private:
  std::string _schema_name;
};

template<typename T>
struct CurrentSchemasFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<bool>&) {
    auto conn_ctx = basics::downCast<const ConnectionContext>(config.config());
    SDB_ASSERT(conn_ctx);
    auto database_id = conn_ctx->GetDatabaseId();
    auto search_path = conn_ctx->Get<VariableType::PgSearchPath>("search_path");
    auto catalog = conn_ctx->EnsureCatalogSnapshot();
    auto filter = [&](const std::string_view schema_name) {
      return catalog->GetSchema(database_id, schema_name) != nullptr;
    };
    _schema_names = std::move(search_path) | std::views::filter(filter) |
                    std::ranges::to<std::vector>();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>& out,
                                const arg_type<bool>& include_implicit) {
    if (include_implicit) {
      out.add_item().copy_from("pg_catalog");
    }
    for (const auto& schema_name : _schema_names) {
      out.add_item().copy_from(schema_name);
    }
  }

 private:
  std::vector<std::string> _schema_names;
};

template<typename T>
struct CurrentUserFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config) {
    const auto& ctx =
      basics::downCast<const ConnectionContext>(config.config());
    SDB_ASSERT(ctx);
    _user = ctx->user();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) { out = _user; }

 private:
  std::string _user;
};

template<typename T>
struct CurrentDatabaseFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config) {
    auto conn_ctx = basics::downCast<const ConnectionContext>(config.config());
    SDB_ASSERT(conn_ctx);
    _database = conn_ctx->GetDatabase();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) {
    out = _database;
  }

 private:
  std::string _database;
};

template<typename T>
struct CurrentSettingFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*) {
    _cfg = basics::downCast<const Config>(config.config());
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>& out,
                                const arg_type<velox::Varchar>& name) {
    std::string_view key{name};
    auto val = _cfg->GetSetting(key);
    if (!val) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized configuration parameter \"", key, "\""));
    }
    out = *val;
    return true;
  }

 private:
  std::shared_ptr<const Config> _cfg;
};

template<typename T>
struct CurrentSettingMissingOkFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const bool*) {
    _cfg = basics::downCast<const Config>(config.config());
  }

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>& out,
                                const arg_type<velox::Varchar>& name,
                                const bool& missing_ok) {
    std::string_view key{name};
    auto val = _cfg->GetSetting(key);
    if (!val) {
      if (missing_ok) {
        return false;
      }
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized configuration parameter \"", key, "\""));
      return false;
    }
    out = *val;
    return true;
  }

 private:
  std::shared_ptr<const Config> _cfg;
};

// Sets the parameter and returns the new value.
template<typename T>
struct SetConfigFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>*,
                                      const bool*) {
    _cfg = basics::downCast<const Config>(config.config());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out,
                                const arg_type<velox::Varchar>& name,
                                const arg_type<velox::Varchar>& value,
                                const bool& is_local) {
    std::string val{value};
    _cfg->SetSetting(name, val, is_local);
    out = val;
  }

 private:
  std::shared_ptr<const Config> _cfg;
};

template<typename T>
struct VersionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(mbkkt) Don't use hard-coded version
  // PG version should be from libpg_query,
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) {
    out = absl::StrCat("PostgreSQL 18.3 (SereneDB ", SERENEDB_VERSION, ")");
  }
};

template<typename T>
struct PgBackendPidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& out) {
    out = static_cast<int32_t>(getpid());
  }
};

template<typename T>
struct CurrentQueryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(mbkkt) Save current query in query config and get it here
  // Also important to check multistatement query behavior
  // (possible only in simple protocol)
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) { out = ""; }
};

template<typename T>
struct PgCharToEncodingFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result,
                                const arg_type<velox::Varchar>&) {
    result = 6;  // UTF8
  }
};

template<typename T>
struct PgEncodingToCharFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int32_t>&) {
    result = "UTF8";
  }
};

template<typename T>
struct UnicodeVersionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) {
    out = "15.1.0";
  }
};

template<typename T>
struct IcuUnicodeVersionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) { out = "75.1"; }
};

template<typename T>
struct GetDatabaseEncodingFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result) {
    result = "UTF8";
  }
};

template<typename T>
struct PgEncodingMaxLength {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Returns max bytes per character for the given encoding (always 4 for UTF-8)
  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result,
                                const arg_type<int32_t>&) {
    result = 4;
  }
};

template<typename T>
struct PgRelationIsUpdatable {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Returns bitmask: INSERT=8, UPDATE=4, DELETE=16, all=28
  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    result = 28;
  }
};

template<typename T>
struct NameConcatOidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& name,
                                const arg_type<int64_t>& oid) {
    // TODO(mbkkt) It doesn't look very good, but it's best what we can for
    // Velox. It's also doesn't really matter.
    result.reserve(name.size() + 1 + 22);
    result.append(name);
    result.append("_");
    result.append(absl::StrCat(static_cast<int64_t>(oid)));
  }
};

template<typename T>
struct NowFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(mbkkt) should use TimestampWithTimeZoneType
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result) {
    result = velox::Timestamp::now();
  }
};

template<typename T>
struct FormatTypeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(Pasha) Account typmod?
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& type_oid,
                                const arg_type<int64_t>&) {
    result = RegtypeOut(type_oid);
  }
};

template<typename T>
struct PgTypeofFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig& config, const arg_type<velox::Any>* input) {
    _type_oid = Type2Oid(inputTypes[0]);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(out_type<int64_t>& result,
                                        const arg_type<velox::Any>* input) {
    result = _type_oid;
    return true;
  }

 private:
  int32_t _type_oid;
};

}  // namespace

void registerSystemFunctions(const std::string& prefix) {
  // 9.27.1 Session Information Functions

  velox::registerFunction<CurrentSchemaFunction, NameCustomType>(
    {prefix + "current_schema"});
  velox::registerFunction<CurrentSchemasFunction, velox::Array<velox::Varchar>,
                          bool>({prefix + "current_schemas"});
  velox::registerFunction<CurrentUserFunction, NameCustomType>(
    {prefix + "current_user"});
  velox::registerFunction<CurrentDatabaseFunction, NameCustomType>(
    {prefix + "current_database"});
  velox::registerFunction<CurrentSettingFunction, velox::Varchar,
                          velox::Varchar>({prefix + "current_setting"});
  velox::registerFunction<CurrentSettingMissingOkFunction, velox::Varchar,
                          velox::Varchar, bool>({prefix + "current_setting"});
  velox::registerFunction<SetConfigFunction, velox::Varchar, velox::Varchar,
                          velox::Varchar, bool>({prefix + "set_config"});
  velox::registerFunction<CurrentQueryFunction, velox::Varchar>(
    {prefix + "current_query"});
  velox::registerFunction<PgBackendPidFunction, int32_t>(
    {prefix + "backend_pid"});
  velox::registerFunction<PgEncodingMaxLength, int32_t, int32_t>(
    {prefix + "encoding_max_length"});
  velox::registerFunction<PgRelationIsUpdatable, int32_t, RegclassCustomType,
                          bool>({prefix + "relation_is_updatable"});

  // 9.27.4 System Catalog Information Functions

  velox::registerFunction<NameConcatOidFunction, NameCustomType, NameCustomType,
                          OidCustomType>({prefix + "nameconcatoid"});
  velox::registerFunction<PgCharToEncodingFunction, int32_t, NameCustomType>(
    {prefix + "char_to_encoding"});
  velox::registerFunction<PgEncodingToCharFunction, NameCustomType, int32_t>(
    {prefix + "encoding_to_char"});
  velox::registerFunction<GetUserByIdFunction, NameCustomType, OidCustomType>(
    {prefix + "get_userbyid"});

  // 9.27.11 Version Information Functions

  velox::registerFunction<VersionFunction, velox::Varchar>(
    {prefix + "version"});
  velox::registerFunction<UnicodeVersionFunction, velox::Varchar>(
    {prefix + "unicode_version"});
  velox::registerFunction<IcuUnicodeVersionFunction, velox::Varchar>(
    {prefix + "icu_unicode_version"});

  velox::registerFunction<GetDatabaseEncodingFunction, velox::Varchar>(
    {prefix + "getdatabaseencoding"});

  velox::registerFunction<NowFunction, velox::Timestamp>(
    {prefix + "conf_load_time"});
  velox::registerFunction<NowFunction, velox::Timestamp>(
    {prefix + "postmaster_start_time"});

  velox::registerFunction<PgTypeofFunction, RegtypeCustomType, velox::Any>(
    {prefix + "typeof"});

  velox::registerFunction<FormatTypeFunction, velox::Varchar, OidCustomType,
                          int32_t>({prefix + "format_type"});
}

}  // namespace sdb::pg::functions
