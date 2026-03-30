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

#include "pg/functions.h"

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
#include <velox/vector/RangeVector.h>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "pg/connection_context.h"
#include "pg/functions/array_extra.h"
#include "pg/functions/datetime_extra.h"
#include "pg/functions/extract.h"
#include "pg/functions/interval.h"
#include "pg/functions/json.h"
#include "pg/functions/lexize.h"
#include "pg/functions/math_extra.h"
#include "pg/functions/regexp.h"
#include "pg/functions/size.h"
#include "pg/functions/string_extra.h"
#include "pg/pg_types.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config.h"
#include "query/types.h"
#include "rest/version.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "common/keywords.h"
#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

template<typename T>
struct GetUserByIdFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    if (input == 10) {
      result = StaticStrings::kDefaultUser;
    } else {
      result = absl::StrCat("unknown (OID=", input, ")");
    }
  }
};

template<typename T>
struct GetViewDef {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    result = "";
  }
};

template<typename T>
struct GetRuleDef {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    result = "";
  }
};

template<typename T>
struct ByteaOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varbinary>* /*input*/) {
    auto cfg = basics::downCast<const Config>(config.config());
    auto bytea_output = cfg->Get<VariableType::PgByteaOutput>("bytea_output");

    _bytea_output = std::move(bytea_output);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varchar>& input) {
    auto value = std::string_view{input.begin(), input.end()};
    if (_bytea_output == ByteaOutput::Hex) {
      const auto required_size = 2 + 2 * input.size();
      result.resize(required_size);
      ByteaOutHex<false>(result.data(), value);
    } else {
      SDB_ASSERT(_bytea_output == ByteaOutput::Escape);
      const auto required_size = ByteaOutEscapeLength<false>(value);
      result.resize(required_size);
      ByteaOutEscape<false>(result.data(), value);
    }
  }

 private:
  ByteaOutput _bytea_output;
};

template<typename T>
struct ByteaInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varchar>& input) {
    if (std::string_view{input}.starts_with("\\x")) {
      std::string_view payload{input.begin() + 2, input.end()};
      result.resize(payload.size() / 2);

      char* out = result.data();

      for (size_t i = 0; i < payload.size();) {
        char c = payload[i];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
          ++i;
          continue;
        }
        const char h1 = absl::kHexValueStrict[c & 0xFF];
        if (h1 == -1) [[unlikely]] {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("invalid hexadecimal digit: \"",
                                  std::string_view{&c, 1}, "\""));
        }
        if (i + 1 >= payload.size()) [[unlikely]] {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("invalid hexadecimal data: odd number of digits"));
          return false;
        }

        const char h2 = absl::kHexValueStrict[payload[i + 1] & 0xFF];
        if (h2 == -1) [[unlikely]] {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("invalid hexadecimal digit: \"",
                                  std::string_view{&payload[i + 1], 1}, "\""));
        }
        *out++ = (h1 << 4) + h2;
        i += 2;
      }
      const size_t new_size = out - result.data();
      result.resize(new_size);
    } else {
      // Unescape \\ and octal numbers
      result.resize(input.size());

      std::string_view payload{input.begin(), input.end()};
      char* out = result.data();
      for (size_t i = 0; i < payload.size();) {
        char c = payload[i];
        if (c != '\\') [[likely]] {
          ++i;
          *out++ = c;
          continue;
        }
        if (i + 1 >= payload.size()) [[unlikely]] {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                          ERR_MSG("invalid input syntax for type bytea"));
        }
        if (payload[i + 1] == '\\') {
          *out++ = '\\';
          i += 2;
          continue;
        }
        if (i + 3 >= payload.size()) [[unlikely]] {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                          ERR_MSG("invalid input syntax for type bytea"));
        }
        // Octal number
        if ((payload[i + 1] >= '0' && payload[i + 1] <= '3') &&
            (payload[i + 2] >= '0' && payload[i + 2] <= '7') &&
            (payload[i + 3] >= '0' && payload[i + 3] <= '7')) {
          unsigned char val = ((payload[i + 1] - '0') << 6) +
                              ((payload[i + 2] - '0') << 3) +
                              (payload[i + 3] - '0');
          *out++ = val;
          i += 4;
        } else {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                          ERR_MSG("invalid input syntax for type bytea"));
        }
      }
      const size_t new_size = out - result.data();
      result.resize(new_size);
    }
    return true;
  }
};

template<typename T>
struct CurrentSchemaFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
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

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<bool>& /*include_implicit*/) {
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

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
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

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
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
    std::string_view key(name.data(), name.size());
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
    std::string_view key(name.data(), name.size());
    auto val = _cfg->GetSetting(key);
    if (!val) {
      if (missing_ok) {
        return false;  // NULL
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
    std::string_view key(name.data(), name.size());
    std::string val(value.data(), value.size());
    _cfg->SetSetting(key, val, is_local);
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

// num_nonnulls(VARIADIC "any") -> integer
template<typename T>
struct NumNonNullsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  static constexpr bool is_default_null_behavior = false;

  FOLLY_ALWAYS_INLINE void callNullable(
    int32_t& result, const arg_type<velox::Variadic<velox::Any>>* args) {
    int32_t count = 0;
    if (args) {
      for (auto i = 0; i < args->size(); ++i) {
        if ((*args)[i].has_value()) {
          ++count;
        }
      }
    }
    result = count;
  }
};

// num_nulls(VARIADIC "any") -> integer
template<typename T>
struct NumNullsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  static constexpr bool is_default_null_behavior = false;

  FOLLY_ALWAYS_INLINE void callNullable(
    int32_t& result, const arg_type<velox::Variadic<velox::Any>>* args) {
    int32_t count = 0;
    if (args) {
      for (auto i = 0; i < args->size(); ++i) {
        if (!(*args)[i].has_value()) {
          ++count;
        }
      }
    }
    result = count;
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
struct PgTriggerDepthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& out) { out = 0; }
};

template<typename T>
struct PgJitAvailableFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& out) { out = false; }
};

template<typename T>
struct PgMyTempSchemaFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& out) { out = 0; }
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
struct PgBlockingPidsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<int32_t>>&,
                                const arg_type<int32_t>&) {}
};

template<typename T>
struct PgConfLoadTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(mbkkt) should use TimestampWithTimeZoneType
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result) {
    result = velox::Timestamp::now();
  }
};

template<typename T>
struct PgPostmasterStartTimeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(mbkkt) should use TimestampWithTimeZoneType
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result) {
    result = velox::Timestamp::now();
  }
};

// pg_current_logfile([text]) → text
// Returns NULL (no log collector)
template<typename T>
struct PgCurrentLogfileFunction0 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&) {
    return false;  // NULL
  }
};

template<typename T>
struct PgCurrentLogfileFunction1 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // NULL
  }
};

template<typename T>
struct PgNumaAvailableFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& out) { out = false; }
};

template<typename T>
struct PgNotificationQueueUsageFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<double>& out) { out = 0.0; }
};

template<typename T>
struct EmptyStringOidBool {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    result = "";
  }
};

template<typename T>
struct EmptyStringOidIntBool {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    result = "";
  }
};

template<typename T>
struct EmptyStringOidInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    result = "";
  }
};

template<typename T>
struct EmptyStringText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>&) {
    result = "";
  }
};

template<typename T>
struct EmptyStringTextBool {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<bool>&) {
    result = "";
  }
};

template<typename T>
struct EmptyStringTextOid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&) {
    result = "";
  }
};

template<typename T>
struct EmptyStringTextOidBool {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<bool>&) {
    result = "";
  }
};

template<typename T>
struct NullVarcharTextText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // NULL
  }
};

template<typename T>
struct NullVarcharText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // NULL
  }
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
struct AlwaysFalseFunction1Int {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&) {
    result = false;
  }
};

template<typename T>
struct AlwaysFalseIntText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = false;
  }
};

template<typename T>
struct AlwaysFalseIntIntText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = false;
  }
};

template<typename T>
struct EmptyTextArrayFromText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>& result,
                                const arg_type<velox::Varchar>&) {
    // Return empty array
  }
};

template<typename T>
struct EmptyStringOidOidInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    result = "";
  }
};

template<typename T>
struct PgInputIsValidFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    result = true;  // optimistic stub
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

#define DEFINE_NOT_SUPPORTED_FUNC(Name, ...)                             \
  template<typename T>                                                   \
  struct Name {                                                          \
    VELOX_DEFINE_FUNCTION_TYPES(T);                                      \
                                                                         \
    FOLLY_ALWAYS_INLINE void call(__VA_ARGS__) {                         \
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),           \
                      ERR_MSG("Function is not supported in SereneDB")); \
    }                                                                    \
  };

DEFINE_NOT_SUPPORTED_FUNC(NotSupported0Text, out_type<velox::Varchar>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported0Int, out_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported1Int, out_type<velox::Varchar>&,
                          const arg_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported1Text, out_type<velox::Varchar>&,
                          const arg_type<velox::Varchar>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported2TextText, out_type<velox::Varchar>&,
                          const arg_type<velox::Varchar>&,
                          const arg_type<velox::Varchar>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported3OidOidInt, out_type<velox::Varchar>&,
                          const arg_type<int64_t>&, const arg_type<int64_t>&,
                          const arg_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported1IntTimestamp,
                          out_type<velox::Timestamp>&, const arg_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupportedBool2IntInt, out_type<bool>&,
                          const arg_type<int64_t>&, const arg_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported4OidOidTextBool,
                          out_type<velox::Varchar>&, const arg_type<int64_t>&,
                          const arg_type<int64_t>&,
                          const arg_type<velox::Varchar>&,
                          const arg_type<bool>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupportedInt1Int, out_type<int32_t>&,
                          const arg_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupportedInt1Text, out_type<int32_t>&,
                          const arg_type<velox::Varchar>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported2CharInt,
                          out_type<velox::Array<velox::Varchar>>&,
                          const arg_type<int8_t>&, const arg_type<int64_t>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported1TextArray, out_type<velox::Varchar>&,
                          const arg_type<velox::Array<velox::Varchar>>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported1IntArray, out_type<velox::Varchar>&,
                          const arg_type<velox::Array<int64_t>>&)
DEFINE_NOT_SUPPORTED_FUNC(NotSupported3TextTextArrayTextArray,
                          out_type<velox::Varchar>&,
                          const arg_type<velox::Varchar>&,
                          const arg_type<velox::Array<velox::Varchar>>&,
                          const arg_type<velox::Array<velox::Varchar>>&)

#undef DEFINE_NOT_SUPPORTED_FUNC

// Stub functions returning 0 or NULL, used by pg_stat_* views

template<typename T>
struct ZeroBigintFunction0 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& out) { out = 0; }
};

template<typename T>
struct ZeroBigintFunction1Oid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& out,
                                const arg_type<int64_t>&) {
    out = 0;
  }
};

template<typename T>
struct ZeroDoubleFunction1Oid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<double>& out,
                                const arg_type<int64_t>&) {
    out = 0.0;
  }
};

template<typename T>
struct ZeroDoubleFunction0 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<double>& out) { out = 0.0; }
};

template<typename T>
struct NullTimestampFunction1Oid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Timestamp>&,
                                const arg_type<int64_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct NullTimestampFunction0 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Timestamp>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct NullBigintFunction1Oid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int64_t>&, const arg_type<int64_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct NullVarcharFunction1OidInt {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct NullTextArrayFunction1Oid {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Array<velox::Varchar>>&,
                                const arg_type<int64_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct GetDatabaseEncodingFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result) {
    result = "UTF8";
  }
};

template<typename T>
struct AlwaysTrueFunction1Text {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction1Int {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction2Text {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
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
struct PgColumnIsUpdatable {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int16_t>&,
                                const arg_type<bool>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction2IntText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction3Text {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction3TextIntText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction4Text {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction4TextIntTextText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction4TextIntIntText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct AlwaysTrueFunction3IntSmallintText {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>&,
                                const arg_type<int16_t>&,
                                const arg_type<velox::Varchar>&) {
    result = true;
  }
};

template<typename T>
struct ColDescriptionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<int64_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct ObjDescriptionFunction1 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct ObjDescriptionFunction2 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct ShObjDescriptionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&,
                                const arg_type<int64_t>&,
                                const arg_type<velox::Varchar>&) {
    return false;  // returns NULL
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
struct NullVarcharFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varchar>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct NullIntFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<int32_t>&) {
    return false;  // returns NULL
  }
};

template<typename T>
struct PgTableIsVisible {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<bool>& result,
                                const arg_type<int64_t>& relid) {
    // TODO(codeworse): implement proper schema resolution
    result = true;
  }
};

template<typename T>
struct IntervalInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<Interval>& result,
                                const arg_type<velox::Varchar>& input,
                                const arg_type<int32_t>& range,
                                const arg_type<int32_t>& precision) {
    std::string_view input_view{input.begin(), input.end()};
    result = IntervalIn(input_view, range, precision);
  }
};

template<typename T>
struct ConcatWsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_null_behavior = false;

  FOLLY_ALWAYS_INLINE bool callNullable(
    out_type<velox::Varchar>& result, const arg_type<velox::Varchar>* separator,
    const arg_type<velox::Variadic<velox::Varchar>>* args) {
    if (!separator) {
      return false;
    }
    if (args) {
      std::string_view s{*separator};
      bool first = true;
      for (size_t i = 0; i < args->size(); ++i) {
        if (const auto& v = (*args)[i]) {
          if (!first) {
            result.append(s);
          }
          first = false;
          result.append(*v);
        }
      }
    }
    return true;
  }
};

// octet_length(text) -> integer
// Returns the number of bytes in the string (not characters).
template<typename T>
struct PgOctetLength {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(int64_t& result,
                                const arg_type<velox::Varchar>& input) {
    result = static_cast<int64_t>(input.size());
  }
};

template<typename T>
struct QuoteIdentFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& input) {
    auto str = std::string_view{input.data(), input.size()};
    bool needs_quoting = str.empty() || (str[0] >= '0' && str[0] <= '9');
    if (!needs_quoting) {
      for (auto c : str) {
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) {
          needs_quoting = true;
          break;
        }
      }
    }
    // Check if the identifier is a reserved keyword in PostgreSQL.
    if (!needs_quoting) {
      // ScanKeywordLookup expects a null-terminated lowercase string.
      // Our str is already lowercase (passed the char check above).
      std::string null_terminated{str};
      int kwnum = ScanKeywordLookup(null_terminated.c_str(), &ScanKeywords);
      if (kwnum >= 0 && ScanKeywordCategories[kwnum] != UNRESERVED_KEYWORD) {
        needs_quoting = true;
      }
    }
    if (!needs_quoting) {
      result = str;
      return;
    }
    result.reserve(str.size() + 2);
    result.append("\"");
    for (auto c : str) {
      if (c == '"') {
        result.append("\"\"");
      } else {
        result.append(std::string_view{&c, 1});
      }
    }
    result.append("\"");
  }
};

template<typename T>
struct QuoteLiteralFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // STRICT: returns NULL on NULL input (the default velox behavior for
  // non-default-null functions)
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& input) {
    auto str = std::string_view{input.data(), input.size()};
    bool has_backslash = str.find('\\') != std::string_view::npos;
    result.reserve(str.size() + 2);
    if (has_backslash) {
      result.append("E'");
    } else {
      result.append("'");
    }
    for (auto c : str) {
      if (c == '\'') {
        result.append("''");
      } else if (c == '\\') {
        result.append("\\\\");
      } else {
        result.append(std::string_view{&c, 1});
      }
    }
    result.append("'");
  }
};

template<typename T>
struct QuoteNullableFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_null_behavior = false;

  FOLLY_ALWAYS_INLINE bool callNullable(out_type<velox::Varchar>& result,
                                        const arg_type<velox::Varchar>* input) {
    if (!input) {
      result = "NULL";
      return true;
    }
    QuoteLiteralFunction<T> literal;
    literal.call(result, *input);
    return true;
  }
};

template<typename T>
struct PgSimilarToEscape {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& pattern) {
    result =
      EscapePattern(std::string_view{pattern.data(), pattern.size()}, '\\');
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& pattern,
                                const arg_type<velox::Varchar>& escape) {
    std::string_view escape_sv{escape.data(), escape.size()};

    if (escape_sv.size() != 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                      ERR_MSG("invalid escape string: must be one character"));
    }

    result = EscapePattern({pattern.data(), pattern.size()}, escape_sv[0]);
  }

 private:
  std::string EscapePattern(std::string_view pattern, char escape_char) {
    // Postgres implementation from
    // https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/adt/regexp.c
    std::string result;
    result.reserve(pattern.size() + 6);  // at least

    bool afterescape = false;
    int nquotes = 0;
    int bracket_depth = 0;
    int charclass_pos = 0;

    result += "^(?:";

    for (size_t i = 0; i < pattern.size(); ++i) {
      char pchar = pattern[i];

      if (afterescape) {
        if (pchar == '"' && bracket_depth < 1) {
          if (nquotes == 0) {
            result += "){1,1}?(";
          } else if (nquotes == 1) {
            result += "){1,1}(?:";
          } else {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_INVALID_USE_OF_ESCAPE_CHARACTER),
              ERR_MSG("SQL regular expression may not contain more than "
                      "two escape-double-quote separators"));
          }
          nquotes++;
        } else {
          result += '\\';
          result += pchar;
          charclass_pos = 3;
        }

        afterescape = false;
      } else if (pchar == escape_char) {
        afterescape = true;
      } else if (bracket_depth > 0) {
        if (pchar == '\\') {
          result += '\\';
        }
        result += pchar;

        if (pchar == ']' && charclass_pos > 2) {
          bracket_depth--;
        } else if (pchar == '[') {
          bracket_depth++;
          charclass_pos = 3;
        } else if (pchar == '^') {
          charclass_pos++;
        } else {
          charclass_pos = 3;
        }
      } else if (pchar == '[') {
        result += pchar;
        bracket_depth = 1;
        charclass_pos = 1;
      } else if (pchar == '%') {
        result += ".*";
      } else if (pchar == '_') {
        result += '.';
      } else if (pchar == '(') {
        result += "(?:";
      } else if (pchar == '\\' || pchar == '.' || pchar == '^' ||
                 pchar == '$') {
        result += '\\';
        result += pchar;
      } else {
        result += pchar;
      }
    }

    result += ")$";
    return result;
  }
};

template<typename T>
struct IntervalOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<Interval>& interval_packed) {
    result = IntervalOut(interval_packed);
  }
};

velox::Timestamp AddIntervalToTimestamp(const velox::Timestamp& timestamp,
                                        const UnpackedInterval& interval) {
  velox::Timestamp result = velox::functions::addToTimestamp(
    timestamp, velox::functions::DateTimeUnit::kMonth, interval.month);
  result = velox::functions::addToTimestamp(
    result, velox::functions::DateTimeUnit::kDay, interval.day);
  result = velox::functions::addToTimestamp(
    result, velox::functions::DateTimeUnit::kMicrosecond, interval.time);
  return result;
}

template<typename T>
struct TimestampPlusIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<velox::Timestamp>& timestamp,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval);
    result = AddIntervalToTimestamp(timestamp, interval);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<Interval>& interval,
                                const arg_type<velox::Timestamp>& timestamp) {
    call(result, timestamp, interval);
  }
};

template<typename T>
struct PgLikeEscape {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& pattern,
                                const arg_type<velox::Varchar>& escape) {
    std::string_view escape_sv{escape.data(), escape.size()};

    if (escape_sv.size() != 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                      ERR_MSG("invalid escape string: must be one character"));
    }

    result = EscapePattern({pattern.data(), pattern.size()}, escape_sv[0]);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& pattern) {
    result =
      EscapePattern(std::string_view{pattern.data(), pattern.size()}, '\0');
  }

 private:
  std::string EscapePattern(std::string_view pattern, char escape_char) {
    // Postgres implementation
    std::string result;
    result.reserve(pattern.size());  // at least
    if (escape_char == '\\') {
      result = pattern;
      return result;
    }
    bool afterescape = false;
    for (char c : pattern) {
      if (c == escape_char && !afterescape) {
        result += '\\';
        afterescape = true;
        continue;
      } else if (c == '\\' && !afterescape) {
        result += '\\';
      }
      result += c;
      afterescape = false;
    }
    return result;
  }
};

template<typename T>
struct TimestampMinusIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Timestamp>& result,
                                const arg_type<velox::Timestamp>& timestamp,
                                const arg_type<Interval>& packed_interval) {
    auto interval = UnpackInterval(packed_interval).Negate();
    result = AddIntervalToTimestamp(timestamp, interval);
  }
};

// Process escape pattern for LIKE
// to make pattern presto compatible for escape character '\'
// Example: escape '\' in 'ab\ac' => 'abac'
template<typename T>
struct ProcessEscapePattern {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<velox::Varchar>& pattern) {
    char escape_char = '\\';
    std::string res;
    res.reserve(pattern.size());
    bool afterescape = false;
    for (char c : pattern) {
      if (c == escape_char && !afterescape) {
        afterescape = true;
        continue;
      }
      if (afterescape && (c == '%' || c == '_' || c == '\\')) {
        res += '\\';
      }
      res += c;
      afterescape = false;
    }
    if (afterescape) {
      // For some reason postgres allows escape character at the end of pattern
      res += escape_char;
      res += escape_char;
    }
    result = std::move(res);
  }
};

template<typename T>
struct RegtypeInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input,
                                const arg_type<int32_t>& location) {
    std::string_view name{input};
    auto oid = RegtypeIn(name);
    if (oid != kInvalidOid) {
      result = oid;
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT), CURSOR_POS(location),
                    ERR_MSG("type \"", name, "\" does not exist"));
  }
};

template<typename T>
struct RegtypeOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    result = RegtypeOut(input);
  }
};

template<typename T>
struct RegclassInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<int32_t>*) {
    _ctx = basics::downCast<const ConnectionContext>(config.config()).get();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input,
                                const arg_type<int32_t>& location) {
    result = RegclassIn(*_ctx, input);
    if (result == kInvalidOid) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_TABLE), CURSOR_POS(location),
        ERR_MSG("relation \"", std::string_view{input}, "\" does not exist"));
    }
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct RegclassOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = &basics::downCast<const ConnectionContext>(*config.config());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    result = RegclassOut(*_ctx->EnsureCatalogSnapshot(), input);
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct RegnamespaceInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<int32_t>*) {
    _ctx = &basics::downCast<const ConnectionContext>(*config.config());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int64_t>& result,
                                const arg_type<velox::Varchar>& input,
                                const arg_type<int32_t>& location) {
    result = RegnamespaceIn(*_ctx, input);
    if (result == kInvalidOid) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_SCHEMA), CURSOR_POS(location),
        ERR_MSG("schema \"", std::string_view{input}, "\" does not exist"));
    }
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct RegnamespaceOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<int64_t>*) {
    _ctx = &basics::downCast<const ConnectionContext>(*config.config());
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& input) {
    result = RegnamespaceOut(*_ctx->EnsureCatalogSnapshot(), input);
  }

 private:
  const ConnectionContext* _ctx;
};

template<typename T>
struct PgTypeofFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig& config, const arg_type<velox::Any>* input) {
    _type_oid = GetTypeOID(inputTypes[0]);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(out_type<int64_t>& result,
                                        const arg_type<velox::Any>* input) {
    result = _type_oid;
    return true;
  }

 private:
  int32_t _type_oid;
};

template<typename T>
struct PgErrorFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  [[noreturn]] FOLLY_ALWAYS_INLINE void call(
    out_type<velox::UnknownValue>&, const arg_type<velox::Varchar>& errmsg) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_RAISE_EXCEPTION),
                    ERR_MSG(std::string_view{errmsg}));
  }

  [[noreturn]] FOLLY_ALWAYS_INLINE void call(
    out_type<velox::UnknownValue>&, const arg_type<int32_t>& errcode,
    const arg_type<int32_t>& cursorpos,
    const arg_type<velox::Varchar>& errmsg) {
    THROW_SQL_ERROR(ERR_CODE(errcode), CURSOR_POS(cursorpos),
                    ERR_MSG(std::string_view{errmsg}));
  }

  [[noreturn]] FOLLY_ALWAYS_INLINE void call(
    out_type<velox::UnknownValue>&, const arg_type<int32_t>& errcode,
    const arg_type<int32_t>& cursorpos, const arg_type<velox::Varchar>& errmsg,
    const arg_type<velox::Varchar>& detail) {
    THROW_SQL_ERROR(ERR_CODE(errcode), CURSOR_POS(cursorpos),
                    ERR_MSG(std::string_view{errmsg}),
                    ERR_DETAIL(std::string_view{detail}));
  }

  [[noreturn]] FOLLY_ALWAYS_INLINE void call(
    out_type<velox::UnknownValue>&, const arg_type<int32_t>& errcode,
    const arg_type<int32_t>& cursorpos, const arg_type<velox::Varchar>& errmsg,
    const arg_type<velox::Varchar>& detail,
    const arg_type<velox::Varchar>& hint) {
    THROW_SQL_ERROR(ERR_CODE(errcode), CURSOR_POS(cursorpos),
                    ERR_MSG(std::string_view{errmsg}),
                    ERR_DETAIL(std::string_view{detail}),
                    ERR_HINT(std::string_view{hint}));
  }
};

class GenerateSeriesFunction : public velox::exec::VectorFunction {
 public:
  void apply(const velox::SelectivityVector& rows,
             std::vector<velox::VectorPtr>& args,
             const velox::TypePtr& output_type, velox::exec::EvalCtx& context,
             velox::VectorPtr& result) const final {
    velox::exec::DecodedArgs decoded_args{rows, args, context};
    const auto* start_vector = decoded_args.at(0);
    const auto* stop_vector = decoded_args.at(1);
    velox::DecodedVector* step_vector = nullptr;
    if (args.size() == 3) {
      step_vector = decoded_args.at(2);
    }

    const auto num_rows = rows.end();
    auto* pool = context.pool();

    auto sizes = velox::allocateSizes(num_rows, pool);
    auto offsets = velox::allocateOffsets(num_rows, pool);
    auto* raw_sizes = sizes->asMutable<velox::vector_size_t>();
    auto* raw_offsets = offsets->asMutable<velox::vector_size_t>();

    std::vector<velox::RangeVector::RowMeta> metas(num_rows);
    velox::vector_size_t total_elements = 0;

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto start = start_vector->valueAt<int64_t>(row);
      auto stop = stop_vector->valueAt<int64_t>(row);
      int64_t step = step_vector ? step_vector->valueAt<int64_t>(row) : 1;

      if (step == 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("step size cannot equal zero"));
      }

      int64_t count = 0;
      if ((step > 0 && stop >= start) || (step < 0 && stop <= start)) {
        count = (stop - start) / step + 1;
      }

      raw_offsets[row] = total_elements;
      raw_sizes[row] = static_cast<velox::vector_size_t>(count);
      metas[row] = {start, step};
      total_elements += raw_sizes[row];
    });

    auto range_vector = std::make_shared<velox::RangeVector>(
      pool, output_type, nullptr, num_rows, std::move(offsets),
      std::move(sizes), std::move(metas));

    context.moveOrCopyResult(std::move(range_vector), rows, result);
  }
};

class GenerateSubscriptsFunction : public velox::exec::VectorFunction {
 public:
  void apply(const velox::SelectivityVector& rows,
             std::vector<velox::VectorPtr>& args,
             const velox::TypePtr& output_type, velox::exec::EvalCtx& context,
             velox::VectorPtr& result) const override {
    velox::exec::DecodedArgs decoded_args{rows, args, context};
    auto* array_vector = decoded_args.at(0);
    auto* dim_vector = decoded_args.at(1);
    velox::DecodedVector* reverse_vector = nullptr;
    if (args.size() == 3) {
      reverse_vector = decoded_args.at(2);
    }

    const auto num_rows = rows.end();
    auto* pool = context.pool();

    auto sizes = velox::allocateSizes(num_rows, pool);
    auto offsets = velox::allocateOffsets(num_rows, pool);
    auto* raw_sizes = sizes->asMutable<velox::vector_size_t>();
    auto* raw_offsets = offsets->asMutable<velox::vector_size_t>();

    std::vector<velox::RangeVector::RowMeta> metas(num_rows);
    velox::vector_size_t total_elements = 0;

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      auto dim = dim_vector->valueAt<int64_t>(row);
      bool reverse = reverse_vector && reverse_vector->valueAt<bool>(row);

      int64_t count = 0;
      if (dim >= 1 && !array_vector->isNullAt(row)) {
        auto& base_array =
          basics::downCast<velox::ArrayVector>(*array_vector->base());
        auto* cur = &base_array;
        auto idx = array_vector->index(row);
        bool valid = true;

        for (int64_t d = 1; d < dim; ++d) {
          if (cur->sizeAt(idx) == 0) {
            valid = false;
            break;
          }
          auto* inner =
            dynamic_cast<velox::ArrayVector*>(cur->elements().get());
          if (!inner) {
            valid = false;
            break;
          }
          idx = cur->offsetAt(idx);
          cur = inner;
        }

        if (valid) {
          count = cur->sizeAt(idx);
        }
      }

      raw_offsets[row] = total_elements;
      raw_sizes[row] = static_cast<velox::vector_size_t>(count);
      if (reverse) {
        metas[row] = {count, -1};
      } else {
        metas[row] = {1, 1};
      }
      total_elements += raw_sizes[row];
    });

    auto range_vector = std::make_shared<velox::RangeVector>(
      pool, output_type, nullptr, num_rows, std::move(offsets),
      std::move(sizes), std::move(metas));

    context.moveOrCopyResult(std::move(range_vector), rows, result);
  }
};

using facebook::velox::T1;

// array_length(anyarray, integer) → integer
// Returns the length of the requested array dimension.
// Produces NULL for empty arrays or missing dimensions.
template<typename T>
struct ArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig& /*config*/,
    const arg_type<velox::Array<velox::Generic<T1>>>* /*input*/,
    const arg_type<int32_t>* /*dim*/) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input,
    const arg_type<int32_t>& dim) {
    if (dim < 1 || dim > _depth || input.size() == 0) {
      return false;
    }
    if (dim == 1) {
      out = static_cast<int32_t>(input.size());
      return true;
    }
    // Walk into nested arrays via first element at each level
    auto first = input.at(0);
    if (!first.has_value()) {
      return false;
    }
    const auto* base = first->base();
    auto idx = first->decodedIndex();
    for (int32_t d = 2; d <= dim; ++d) {
      const auto* arr = base->template as<velox::ArrayVector>();
      auto size = arr->sizeAt(idx);
      if (d == dim) {
        if (size == 0) {
          return false;
        }
        out = static_cast<int32_t>(size);
        return true;
      }
      if (size == 0) {
        return false;
      }
      idx = arr->offsetAt(idx);
      base = arr->elements().get();
    }
    return false;
  }

 private:
  int32_t _depth = 0;
};

// array_ndims(anyarray) -> integer
// Returns the number of dimensions of the array.
template<typename T>
struct ArrayNdimsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input) {
    if (input.size() == 0) {
      return false;  // NULL for empty arrays
    }
    out = _depth;
    return true;
  }

 private:
  int32_t _depth = 0;
};

// Returns the lower bound of the requested dimension (always 1 in PG).
template<typename T>
struct ArrayLowerFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*,
    const arg_type<int32_t>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input,
    const arg_type<int32_t>& dim) {
    if (dim < 1 || dim > _depth || input.size() == 0) {
      return false;
    }
    out = 1;  // PG arrays are always 1-based
    return true;
  }

 private:
  int32_t _depth = 0;
};

// array_upper(anyarray, integer) -> integer
// Returns the upper bound (= length) of the requested dimension.
template<typename T>
struct ArrayUpperFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*,
    const arg_type<int32_t>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<int32_t>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input,
    const arg_type<int32_t>& dim) {
    if (dim < 1 || dim > _depth || input.size() == 0) {
      return false;
    }
    if (dim == 1) {
      out = static_cast<int32_t>(input.size());
      return true;
    }
    // Same logic as ArrayLengthFunction for deeper dims.
    auto first = input.at(0);
    if (!first.has_value()) {
      return false;
    }
    const auto* base = first->base();
    auto idx = first->decodedIndex();
    for (int32_t d = 2; d <= dim; ++d) {
      const auto* arr = base->template as<velox::ArrayVector>();
      auto size = arr->sizeAt(idx);
      if (d == dim) {
        if (size == 0) {
          return false;
        }
        out = static_cast<int32_t>(size);
        return true;
      }
      if (size == 0) {
        return false;
      }
      idx = arr->offsetAt(idx);
      base = arr->elements().get();
    }
    return false;
  }

 private:
  int32_t _depth = 0;
};

// array_dims(anyarray) -> text
// Returns text representation of array dimensions, e.g. "[1:3][1:2]".
template<typename T>
struct ArrayDimsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig&,
    const arg_type<velox::Array<velox::Generic<T1>>>*) {
    SDB_ASSERT(_depth == 0, "initialize should be called only once");
    auto type = inputTypes[0];
    while (type->isArray()) {
      ++_depth;
      type = type->childAt(0);
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
    out_type<velox::Varchar>& out,
    const arg_type<velox::Array<velox::Generic<T1>>>& input) {
    if (input.size() == 0) {
      return false;
    }

    std::string result;
    // First dimension.
    result += "[1:" + std::to_string(input.size()) + "]";

    if (_depth > 1 && input.size() > 0) {
      auto first = input.at(0);
      if (first.has_value()) {
        const auto* base = first->base();
        auto idx = first->decodedIndex();
        for (int32_t d = 2; d <= _depth; ++d) {
          const auto* arr = base->template as<velox::ArrayVector>();
          auto size = arr->sizeAt(idx);
          result += "[1:" + std::to_string(size) + "]";
          if (d < _depth && size > 0) {
            idx = arr->offsetAt(idx);
            base = arr->elements().get();
          }
        }
      }
    }

    out.resize(result.size());
    std::memcpy(out.data(), result.data(), result.size());
    return true;
  }

 private:
  int32_t _depth = 0;
};

}  // namespace

void registerFunctions(const std::string& prefix) {
  // Internal type I/O and operator functions (not in 9.27)

  velox::registerFunction<ByteaInFunction, velox::Varbinary, velox::Varchar>(
    {prefix + "byteain"});
  velox::registerFunction<ByteaOutFunction, velox::Varchar, velox::Varbinary>(
    {prefix + "byteaout"});

  velox::registerFunction<IntervalInFunction, Interval, velox::Varchar, int32_t,
                          int32_t>({prefix + "intervalin"});
  velox::registerFunction<IntervalOutFunction, velox::Varchar, Interval>(
    {prefix + "intervalout"});

  velox::registerFunction<TimestampPlusIntervalFunction, velox::Timestamp,
                          velox::Timestamp, Interval>({prefix + "time_plus"});
  velox::registerFunction<TimestampPlusIntervalFunction, velox::Timestamp,
                          Interval, velox::Timestamp>({prefix + "time_plus"});
  velox::registerFunction<TimestampMinusIntervalFunction, velox::Timestamp,
                          velox::Timestamp, Interval>({prefix + "time_minus"});

  velox::registerFunction<PgOctetLength, int64_t, velox::Varchar>(
    {prefix + "octet_length"});

  velox::registerFunction<QuoteIdentFunction, velox::Varchar, velox::Varchar>(
    {prefix + "quote_ident"});
  velox::registerFunction<QuoteLiteralFunction, velox::Varchar, velox::Varchar>(
    {prefix + "quote_literal"});
  velox::registerFunction<QuoteNullableFunction, velox::Varchar,
                          velox::Varchar>({prefix + "quote_nullable"});

  velox::registerFunction<PgSimilarToEscape, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "similar_to_escape"});
  velox::registerFunction<PgSimilarToEscape, velox::Varchar, velox::Varchar>(
    {prefix + "similar_to_escape"});
  velox::registerFunction<PgLikeEscape, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "like_escape"});
  velox::registerFunction<ProcessEscapePattern, velox::Varchar, velox::Varchar>(
    {prefix + "process_escape_pattern"});

  velox::registerFunction<PgJsonExtractIndex, velox::Json, velox::Json,
                          int64_t>({prefix + "json_extract_index"});
  velox::registerFunction<PgJsonExtractIndexText, velox::Varchar, velox::Json,
                          int64_t>({prefix + "json_extract_index_text"});
  velox::registerFunction<PgJsonExtractField, velox::Json, velox::Json,
                          velox::Varchar>({prefix + "json_extract_field"});
  velox::registerFunction<PgJsonExtractFieldText, velox::Varchar, velox::Json,
                          velox::Varchar>({prefix + "json_extract_field_text"});
  velox::registerFunction<PgJsonExtractPath, velox::Json, velox::Json,
                          velox::Array<velox::Varchar>>(
    {prefix + "json_extract_path"});
  velox::registerFunction<PgJsonExtractPath, velox::Json, velox::Json,
                          velox::Variadic<velox::Varchar>>(
    {prefix + "json_extract_path"});
  velox::registerFunction<PgJsonExtractPathText, velox::Varchar, velox::Json,
                          velox::Array<velox::Varchar>>(
    {prefix + "json_extract_path_text"});
  velox::registerFunction<PgJsonExtractPathText, velox::Varchar, velox::Json,
                          velox::Variadic<velox::Varchar>>(
    {prefix + "json_extract_path_text"});
  velox::registerFunction<PgJsonInFunction, velox::Json, velox::Varchar>(
    {prefix + "jsonin"});
  velox::registerFunction<PgJsonOutFunction, velox::Varchar, velox::Json>(
    {prefix + "jsonout"});
  velox::registerFunction<PgJsonTypeof, velox::Varchar, velox::Json>(
    {prefix + "json_typeof"});
  velox::registerFunction<PgJsonStripNulls, velox::Json, velox::Json>(
    {prefix + "json_strip_nulls"});
  // pg_error(message)
  velox::registerFunction<PgErrorFunction, velox::UnknownValue, velox::Varchar>(
    {prefix + "error"});
  // pg_error(errcode, cursorpos, message)
  velox::registerFunction<PgErrorFunction, velox::UnknownValue, int32_t,
                          int32_t, velox::Varchar>({prefix + "error"});
  // pg_error(errcode, cursorpos, message, detail)
  velox::registerFunction<PgErrorFunction, velox::UnknownValue, int32_t,
                          int32_t, velox::Varchar, velox::Varchar>(
    {prefix + "error"});
  // pg_error(errcode, cursorpos, message, detail, hint)
  velox::registerFunction<PgErrorFunction, velox::UnknownValue, int32_t,
                          int32_t, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "error"});

  velox::registerFunction<PgDatabaseSize, int64_t, velox::Varchar>(
    {prefix + "database_size"});
  velox::registerFunction<PgSchemaSize, int64_t, velox::Varchar>(
    {prefix + "schema_size"});
  velox::registerFunction<PgTableSize, int64_t, velox::Varchar>(
    {prefix + "table_size"});

  velox::registerFunction<PgTsLexize, velox::Array<velox::Varchar>,
                          velox::Varchar, velox::Varchar>(
    {prefix + "ts_lexize"});

  registerRegexpFunctions(prefix);
  registerStringExtraFunctions(prefix);
  registerMathExtraFunctions(prefix);
  registerDatetimeExtraFunctions(prefix);
  registerArrayExtraFunctions(prefix);

  velox::registerFunction<RegtypeInFunction, RegtypeCustomType, velox::Varchar,
                          int32_t>({prefix + "regtypein"});
  velox::registerFunction<RegtypeOutFunction, velox::Varchar,
                          RegtypeCustomType>({prefix + "regtypeout"});
  velox::registerFunction<RegclassInFunction, RegclassCustomType,
                          velox::Varchar, int32_t>({prefix + "regclassin"});
  velox::registerFunction<RegclassOutFunction, velox::Varchar,
                          RegclassCustomType>({prefix + "regclassout"});

  velox::registerFunction<RegnamespaceInFunction, RegnamespaceCustomType,
                          velox::Varchar, int32_t>({prefix + "regnamespacein"});
  velox::registerFunction<RegnamespaceOutFunction, velox::Varchar,
                          RegnamespaceCustomType>({prefix + "regnamespaceout"});

  // 9.27.1 Session Information Functions

  velox::registerFunction<CurrentSchemaFunction, velox::Varchar>(
    {prefix + "current_schema"});
  velox::registerFunction<CurrentSchemasFunction, velox::Array<velox::Varchar>,
                          bool>({prefix + "current_schemas"});
  velox::registerFunction<CurrentUserFunction, velox::Varchar>(
    {prefix + "current_user"});
  velox::registerFunction<CurrentDatabaseFunction, velox::Varchar>(
    {prefix + "current_database"});
  velox::registerFunction<CurrentSettingFunction, velox::Varchar,
                          velox::Varchar>({prefix + "current_setting"});
  velox::registerFunction<CurrentSettingMissingOkFunction, velox::Varchar,
                          velox::Varchar, bool>({prefix + "current_setting"});
  velox::registerFunction<SetConfigFunction, velox::Varchar, velox::Varchar,
                          velox::Varchar, bool>({prefix + "set_config"});
  velox::registerFunction<ConcatWsFunction, velox::Varchar, velox::Varchar,
                          velox::Variadic<velox::Varchar>>(
    {prefix + "concat_ws"});
  velox::registerFunction<NumNonNullsFunction, int32_t,
                          velox::Variadic<velox::Any>>(
    {prefix + "num_nonnulls"});
  velox::registerFunction<NumNullsFunction, int32_t,
                          velox::Variadic<velox::Any>>({prefix + "num_nulls"});
  velox::registerFunction<CurrentQueryFunction, velox::Varchar>(
    {prefix + "current_query"});
  velox::registerFunction<PgBackendPidFunction, int32_t>(
    {prefix + "backend_pid"});
  velox::registerFunction<PgBlockingPidsFunction, velox::Array<int32_t>,
                          int32_t>({prefix + "blocking_pids"});
  velox::registerFunction<PgConfLoadTimeFunction, velox::Timestamp>(
    {prefix + "conf_load_time"});
  velox::registerFunction<PgCurrentLogfileFunction0, velox::Varchar>(
    {prefix + "current_logfile"});
  velox::registerFunction<PgCurrentLogfileFunction1, velox::Varchar,
                          velox::Varchar>({prefix + "current_logfile"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "get_loaded_modules"});
  velox::registerFunction<PgMyTempSchemaFunction, int64_t>(
    {prefix + "my_temp_schema"});
  velox::registerFunction<AlwaysFalseFunction1Int, bool, int64_t>(
    {prefix + "is_other_temp_schema"});
  velox::registerFunction<PgJitAvailableFunction, bool>(
    {prefix + "jit_available"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "listening_channels"});
  velox::registerFunction<PgNotificationQueueUsageFunction, double>(
    {prefix + "notification_queue_usage"});
  velox::registerFunction<PgNumaAvailableFunction, bool>(
    {prefix + "numa_available"});
  velox::registerFunction<PgPostmasterStartTimeFunction, velox::Timestamp>(
    {prefix + "postmaster_start_time"});
  velox::registerFunction<PgBlockingPidsFunction, velox::Array<int32_t>,
                          int32_t>({prefix + "safe_snapshot_blocking_pids"});
  velox::registerFunction<PgTriggerDepthFunction, int32_t>(
    {prefix + "trigger_depth"});
  velox::registerFunction<NullVarcharFunction, velox::Varchar>(
    {prefix + "inet_client_addr"});
  velox::registerFunction<NullIntFunction, int32_t>(
    {prefix + "inet_client_port"});
  velox::registerFunction<NullVarcharFunction, velox::Varchar>(
    {prefix + "inet_server_addr"});
  velox::registerFunction<NullIntFunction, int32_t>(
    {prefix + "inet_server_port"});
  // system_user: not supported (parser lacks SVFOP_SYSTEM_USER)

  // 9.27.2 Access Privilege Inquiry Functions

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>(
    {prefix + "has_any_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_any_column_privilege"});

  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction4Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction4TextIntTextText, bool,
                          velox::Varchar, int64_t, velox::Varchar,
                          velox::Varchar>({prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction4TextIntIntText, bool,
                          velox::Varchar, int64_t, int64_t, velox::Varchar>(
    {prefix + "has_column_privilege"});
  velox::registerFunction<AlwaysTrueFunction3IntSmallintText, bool, int64_t,
                          int16_t, velox::Varchar>(
    {prefix + "has_column_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_database_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_database_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_foreign_data_wrapper_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_function_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_function_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_language_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_language_privilege"});

  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>(
    {prefix + "has_largeobject_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_largeobject_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_parameter_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_parameter_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_schema_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_schema_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_sequence_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_sequence_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_server_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_server_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_table_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_table_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>(
    {prefix + "has_tablespace_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_tablespace_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_type_privilege"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>(
    {prefix + "has_type_privilege"});

  velox::registerFunction<AlwaysTrueFunction2Text, bool, velox::Varchar,
                          velox::Varchar>({prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction3Text, bool, velox::Varchar,
                          velox::Varchar, velox::Varchar>(
    {prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction2IntText, bool, int64_t,
                          velox::Varchar>({prefix + "has_role"});
  velox::registerFunction<AlwaysTrueFunction3TextIntText, bool, velox::Varchar,
                          int64_t, velox::Varchar>({prefix + "has_role"});

  velox::registerFunction<AlwaysTrueFunction1Text, bool, velox::Varchar>(
    {prefix + "row_security_active"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "row_security_active"});

  velox::registerFunction<NotSupported2CharInt, velox::Array<velox::Varchar>,
                          int8_t, int64_t>({prefix + "acldefault"});
  velox::registerFunction<NotSupported1IntArray, velox::Varchar,
                          velox::Array<int64_t>>({prefix + "aclexplode"});
  velox::registerFunction<NotSupported1TextArray, velox::Varchar,
                          velox::Array<velox::Varchar>>(
    {prefix + "aclexplode"});
  velox::registerFunction<NotSupported4OidOidTextBool, velox::Varchar, int64_t,
                          int64_t, velox::Varchar, bool>(
    {prefix + "makeaclitem"});

  // 9.27.3 Schema Visibility Inquiry Functions

  velox::registerFunction<PgTableIsVisible, bool, int64_t>(
    {prefix + "table_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "function_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "type_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "operator_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "opclass_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "opfamily_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "collation_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "conversion_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "statistics_obj_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "ts_config_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "ts_dict_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "ts_parser_is_visible"});
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
    {prefix + "ts_template_is_visible"});

  velox::registerFunction<PgEncodingMaxLength, int32_t, int32_t>(
    {prefix + "encoding_max_length"});
  velox::registerFunction<PgRelationIsUpdatable, int32_t, int64_t, bool>(
    {prefix + "relation_is_updatable"});
  velox::registerFunction<PgColumnIsUpdatable, bool, int64_t, int16_t, bool>(
    {prefix + "column_is_updatable"});

  // 9.27.4 System Catalog Information Functions

  velox::registerFunction<FormatTypeFunction, velox::Varchar, int64_t, int64_t>(
    {prefix + "format_type"});
  velox::registerFunction<NameConcatOidFunction, velox::Varchar, velox::Varchar,
                          int64_t>({prefix + "nameconcatoid"});
  velox::registerFunction<ArrayLengthFunction, int32_t,
                          velox::Array<velox::Generic<T1>>, int32_t>(
    {prefix + "array_length"});
  velox::registerFunction<ArrayNdimsFunction, int32_t,
                          velox::Array<velox::Generic<T1>>>(
    {prefix + "array_ndims"});
  velox::registerFunction<ArrayLowerFunction, int32_t,
                          velox::Array<velox::Generic<T1>>, int32_t>(
    {prefix + "array_lower"});
  velox::registerFunction<ArrayUpperFunction, int32_t,
                          velox::Array<velox::Generic<T1>>, int32_t>(
    {prefix + "array_upper"});
  velox::registerFunction<ArrayDimsFunction, velox::Varchar,
                          velox::Array<velox::Generic<T1>>>(
    {prefix + "array_dims"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "basetype"});
  velox::registerFunction<PgCharToEncodingFunction, int32_t, velox::Varchar>(
    {prefix + "char_to_encoding"});
  velox::registerFunction<PgEncodingToCharFunction, velox::Varchar, int32_t>(
    {prefix + "encoding_to_char"});
  velox::registerFunction<EmptyStringTextOid, velox::Varchar, velox::Varchar,
                          int64_t>({prefix + "get_expr"});
  velox::registerFunction<EmptyStringTextOidBool, velox::Varchar,
                          velox::Varchar, int64_t, bool>({prefix + "get_expr"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "get_catalog_foreign_keys"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_constraintdef"});
  velox::registerFunction<EmptyStringOidBool, velox::Varchar, int64_t, bool>(
    {prefix + "get_constraintdef"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_functiondef"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_function_arguments"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_function_identity_arguments"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_function_result"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_indexdef"});
  velox::registerFunction<EmptyStringOidIntBool, velox::Varchar, int64_t,
                          int64_t, bool>({prefix + "get_indexdef"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "get_keywords"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_partition_constraintdef"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_partkeydef"});
  velox::registerFunction<GetRuleDef, velox::Varchar, int64_t>(
    {prefix + "get_ruledef"});
  velox::registerFunction<EmptyStringOidBool, velox::Varchar, int64_t, bool>(
    {prefix + "get_ruledef"});
  velox::registerFunction<NullVarcharTextText, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "get_serial_sequence"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_statisticsobjdef"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_triggerdef"});
  velox::registerFunction<EmptyStringOidBool, velox::Varchar, int64_t, bool>(
    {prefix + "get_triggerdef"});
  velox::registerFunction<GetUserByIdFunction, velox::Varchar, int64_t>(
    {prefix + "get_userbyid"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringOidBool, velox::Varchar, int64_t, bool>(
    {prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringOidInt, velox::Varchar, int64_t, int64_t>(
    {prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringText, velox::Varchar, velox::Varchar>(
    {prefix + "get_viewdef"});
  velox::registerFunction<EmptyStringTextBool, velox::Varchar, velox::Varchar,
                          bool>({prefix + "get_viewdef"});
  velox::registerFunction<AlwaysFalseIntIntText, bool, int64_t, int64_t,
                          velox::Varchar>(
    {prefix + "index_column_has_property"});
  velox::registerFunction<AlwaysFalseIntText, bool, int64_t, velox::Varchar>(
    {prefix + "index_has_property"});
  velox::registerFunction<AlwaysFalseIntText, bool, int64_t, velox::Varchar>(
    {prefix + "indexam_has_property"});
  velox::registerFunction<NotSupported1TextArray, velox::Varchar,
                          velox::Array<velox::Varchar>>(
    {prefix + "options_to_table"});
  velox::registerFunction<EmptyTextArrayFromText, velox::Array<velox::Varchar>,
                          velox::Varchar>({prefix + "settings_get_flags"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "tablespace_databases"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "tablespace_location"});
  velox::registerFunction<PgTypeofFunction, RegtypeCustomType, velox::Any>(
    {prefix + "typeof"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regclass"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regcollation"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regnamespace"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regoper"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regoperator"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regproc"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regprocedure"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regrole"});
  velox::registerFunction<NullVarcharText, velox::Varchar, velox::Varchar>(
    {prefix + "to_regtype"});
  velox::registerFunction<NotSupportedInt1Text, int32_t, velox::Varchar>(
    {prefix + "to_regtypemod"});

  // 9.27.5 Object Information and Addressing Functions

  velox::registerFunction<NotSupported3OidOidInt, velox::Varchar, int64_t,
                          int64_t, int64_t>({prefix + "get_acl"});
  velox::registerFunction<EmptyStringOidOidInt, velox::Varchar, int64_t,
                          int64_t, int64_t>({prefix + "describe_object"});
  velox::registerFunction<NotSupported3OidOidInt, velox::Varchar, int64_t,
                          int64_t, int64_t>({prefix + "identify_object"});
  velox::registerFunction<NotSupported3OidOidInt, velox::Varchar, int64_t,
                          int64_t, int64_t>(
    {prefix + "identify_object_as_address"});
  velox::registerFunction<NotSupported3TextTextArrayTextArray, velox::Varchar,
                          velox::Varchar, velox::Array<velox::Varchar>,
                          velox::Array<velox::Varchar>>(
    {prefix + "get_object_address"});

  // 9.27.6 Comment Information Functions

  velox::registerFunction<ColDescriptionFunction, velox::Varchar, int64_t,
                          int64_t>({prefix + "col_description"});
  velox::registerFunction<ObjDescriptionFunction2, velox::Varchar, int64_t,
                          velox::Varchar>({prefix + "obj_description"});
  velox::registerFunction<ObjDescriptionFunction1, velox::Varchar, int64_t>(
    {prefix + "obj_description"});
  velox::registerFunction<ShObjDescriptionFunction, velox::Varchar, int64_t,
                          velox::Varchar>({prefix + "shobj_description"});

  // 9.27.7 Data Validity Checking Functions

  velox::registerFunction<PgInputIsValidFunction, bool, velox::Varchar,
                          velox::Varchar>({prefix + "input_is_valid"});
  velox::registerFunction<NotSupported2TextText, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "input_error_info"});

  // column_compression and column_size are not in 9.27 but related
  velox::registerFunction<NotSupported1Text, velox::Varchar, velox::Varchar>(
    {prefix + "column_compression"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "column_compression"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "column_size"});

  // 9.27.8 Transaction ID and Snapshot Information Functions

  velox::registerFunction<NotSupportedInt1Int, int32_t, int64_t>(
    {prefix + "age_xid"});
  velox::registerFunction<NotSupportedInt1Int, int32_t, int64_t>(
    {prefix + "mxid_age"});
  velox::registerFunction<NotSupported0Int, int64_t>(
    {prefix + "current_xact_id"});
  velox::registerFunction<NotSupported0Int, int64_t>(
    {prefix + "current_xact_id_if_assigned"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "xact_status"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "current_snapshot"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "snapshot_xip"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "snapshot_xmax"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "snapshot_xmin"});
  velox::registerFunction<NotSupportedBool2IntInt, bool, int64_t, int64_t>(
    {prefix + "visible_in_snapshot"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "get_multixact_members"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "get_multixact_stats"});
  velox::registerFunction<NotSupported0Int, int64_t>({prefix + "txid_current"});
  velox::registerFunction<NotSupported0Int, int64_t>(
    {prefix + "txid_current_if_assigned"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "txid_current_snapshot"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "txid_snapshot_xip"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "txid_snapshot_xmax"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "txid_snapshot_xmin"});
  velox::registerFunction<NotSupportedBool2IntInt, bool, int64_t, int64_t>(
    {prefix + "txid_visible_in_snapshot"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "txid_status"});

  // 9.27.9 Committed Transaction Information Functions

  velox::registerFunction<NotSupported1IntTimestamp, velox::Timestamp, int64_t>(
    {prefix + "xact_commit_timestamp"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "xact_commit_timestamp_origin"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "last_committed_xact"});

  // 9.27.10 Control Data Functions

  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "control_checkpoint"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "control_system"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "control_init"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "control_recovery"});

  // 9.27.11 Version Information Functions

  velox::registerFunction<VersionFunction, velox::Varchar>(
    {prefix + "version"});
  velox::registerFunction<UnicodeVersionFunction, velox::Varchar>(
    {prefix + "unicode_version"});
  velox::registerFunction<IcuUnicodeVersionFunction, velox::Varchar>(
    {prefix + "icu_unicode_version"});

  // 9.27.12 WAL Summarization Information Functions

  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "available_wal_summaries"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "wal_summary_contents"});
  velox::registerFunction<NotSupported0Text, velox::Varchar>(
    {prefix + "get_wal_summarizer_state"});

  registerExtractFunctions(prefix);

  velox::exec::registerStatefulVectorFunction(
    prefix + "generate_series",
    {velox::exec::FunctionSignatureBuilder()
       .returnType("array(bigint)")
       .argumentType("bigint")
       .argumentType("bigint")
       .build(),
     velox::exec::FunctionSignatureBuilder()
       .returnType("array(bigint)")
       .argumentType("bigint")
       .argumentType("bigint")
       .argumentType("bigint")
       .build()},
    [](const std::string&, const std::vector<velox::exec::VectorFunctionArg>&,
       const velox::core::QueryConfig&)
      -> std::shared_ptr<velox::exec::VectorFunction> {
      return std::make_shared<GenerateSeriesFunction>();
    },
    velox::exec::VectorFunctionMetadataBuilder().deterministic(false).build());

  velox::exec::registerStatefulVectorFunction(
    prefix + "generate_subscripts",
    {velox::exec::FunctionSignatureBuilder()
       .typeVariable("T")
       .returnType("array(bigint)")
       .argumentType("array(T)")
       .argumentType("bigint")
       .build(),
     velox::exec::FunctionSignatureBuilder()
       .typeVariable("T")
       .returnType("array(bigint)")
       .argumentType("array(T)")
       .argumentType("bigint")
       .argumentType("boolean")
       .build()},
    [](const std::string&, const std::vector<velox::exec::VectorFunctionArg>&,
       const velox::core::QueryConfig&)
      -> std::shared_ptr<velox::exec::VectorFunction> {
      return std::make_shared<GenerateSubscriptsFunction>();
    },
    velox::exec::VectorFunctionMetadataBuilder().deterministic(false).build());

  // pg_stat_get_* scalar stub functions (used by pg_stat_* views)
  // These return 0 or NULL as stubs.

  // Functions taking OID, returning bigint (0)
  for (const auto& name : {
         "stat_get_numscans",
         "stat_get_tuples_returned",
         "stat_get_tuples_fetched",
         "stat_get_tuples_inserted",
         "stat_get_tuples_updated",
         "stat_get_tuples_deleted",
         "stat_get_tuples_hot_updated",
         "stat_get_tuples_newpage_updated",
         "stat_get_live_tuples",
         "stat_get_dead_tuples",
         "stat_get_mod_since_analyze",
         "stat_get_ins_since_vacuum",
         "stat_get_vacuum_count",
         "stat_get_autovacuum_count",
         "stat_get_analyze_count",
         "stat_get_autoanalyze_count",
         "stat_get_blocks_fetched",
         "stat_get_blocks_hit",
         "stat_get_xact_numscans",
         "stat_get_xact_tuples_returned",
         "stat_get_xact_tuples_fetched",
         "stat_get_xact_tuples_inserted",
         "stat_get_xact_tuples_updated",
         "stat_get_xact_tuples_deleted",
         "stat_get_xact_tuples_hot_updated",
         "stat_get_xact_tuples_newpage_updated",
         "stat_get_function_calls",
         "stat_get_xact_function_calls",
         "stat_get_db_numbackends",
         "stat_get_db_xact_commit",
         "stat_get_db_xact_rollback",
         "stat_get_db_blocks_fetched",
         "stat_get_db_blocks_hit",
         "stat_get_db_tuples_returned",
         "stat_get_db_tuples_fetched",
         "stat_get_db_tuples_inserted",
         "stat_get_db_tuples_updated",
         "stat_get_db_tuples_deleted",
         "stat_get_db_conflict_all",
         "stat_get_db_temp_files",
         "stat_get_db_temp_bytes",
         "stat_get_db_deadlocks",
         "stat_get_db_checksum_failures",
         "stat_get_db_sessions",
         "stat_get_db_sessions_abandoned",
         "stat_get_db_sessions_fatal",
         "stat_get_db_sessions_killed",
         "stat_get_db_parallel_workers_to_launch",
         "stat_get_db_parallel_workers_launched",
         "stat_get_db_conflict_tablespace",
         "stat_get_db_conflict_lock",
         "stat_get_db_conflict_snapshot",
         "stat_get_db_conflict_bufferpin",
         "stat_get_db_conflict_startup_deadlock",
         "stat_get_db_conflict_logicalslot",
       }) {
    velox::registerFunction<ZeroBigintFunction1Oid, int64_t, int64_t>(
      {prefix + name});
  }

  // Functions taking OID, returning double precision (0.0)
  for (const auto& name : {
         "stat_get_total_vacuum_time",
         "stat_get_total_autovacuum_time",
         "stat_get_total_analyze_time",
         "stat_get_total_autoanalyze_time",
         "stat_get_function_total_time",
         "stat_get_function_self_time",
         "stat_get_xact_function_total_time",
         "stat_get_xact_function_self_time",
         "stat_get_db_blk_read_time",
         "stat_get_db_blk_write_time",
         "stat_get_db_session_time",
         "stat_get_db_active_time",
         "stat_get_db_idle_in_transaction_time",
       }) {
    velox::registerFunction<ZeroDoubleFunction1Oid, double, int64_t>(
      {prefix + name});
  }

  // Functions taking OID, returning timestamp (NULL)
  for (const auto& name : {
         "stat_get_lastscan",
         "stat_get_last_vacuum_time",
         "stat_get_last_autovacuum_time",
         "stat_get_last_analyze_time",
         "stat_get_last_autoanalyze_time",
         "stat_get_stat_reset_time",
         "stat_get_function_stat_reset_time",
         "stat_get_db_stat_reset_time",
         "stat_get_db_checksum_last_failure",
       }) {
    velox::registerFunction<NullTimestampFunction1Oid, velox::Timestamp,
                            int64_t>({prefix + name});
  }

  // Parameterless functions returning bigint (0)
  for (const auto& name : {
         "stat_get_bgwriter_buf_written_clean",
         "stat_get_bgwriter_maxwritten_clean",
         "stat_get_buf_alloc",
         "stat_get_checkpointer_num_timed",
         "stat_get_checkpointer_num_requested",
         "stat_get_checkpointer_num_performed",
         "stat_get_checkpointer_restartpoints_timed",
         "stat_get_checkpointer_restartpoints_requested",
         "stat_get_checkpointer_restartpoints_performed",
         "stat_get_checkpointer_buffers_written",
         "stat_get_checkpointer_slru_written",
       }) {
    velox::registerFunction<ZeroBigintFunction0, int64_t>({prefix + name});
  }

  // Parameterless functions returning double precision (0.0)
  for (const auto& name : {
         "stat_get_checkpointer_write_time",
         "stat_get_checkpointer_sync_time",
       }) {
    velox::registerFunction<ZeroDoubleFunction0, double>({prefix + name});
  }

  // Parameterless functions returning timestamp (NULL)
  for (const auto& name : {
         "stat_get_bgwriter_stat_reset_time",
         "stat_get_checkpointer_stat_reset_time",
       }) {
    velox::registerFunction<NullTimestampFunction0, velox::Timestamp>(
      {prefix + name});
  }

  // pg_sequence_last_value(oid) -> bigint (NULL)
  velox::registerFunction<NullBigintFunction1Oid, int64_t, int64_t>(
    {prefix + "sequence_last_value"});

  // pg_getdatabaseencoding() -> text
  velox::registerFunction<GetDatabaseEncodingFunction, velox::Varchar>(
    {"pg_getdatabaseencoding"});

  // pg_get_function_arg_default(oid, int) -> text (NULL)
  velox::registerFunction<NullVarcharFunction1OidInt, velox::Varchar, int64_t,
                          int64_t>({prefix + "get_function_arg_default"});

  // pg_get_statisticsobjdef_expressions(oid) -> text[] (NULL)
  velox::registerFunction<NullTextArrayFunction1Oid,
                          velox::Array<velox::Varchar>, int64_t>(
    {prefix + "get_statisticsobjdef_expressions"});
}

}  // namespace sdb::pg::functions
