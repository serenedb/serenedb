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
#include <velox/functions/Macros.h>
#include <velox/functions/Registerer.h>
#include <velox/functions/prestosql/DateTimeImpl.h>
#include <velox/type/SimpleFunctionApi.h>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "pg/connection_context.h"
#include "pg/functions/extract.h"
#include "pg/functions/interval.h"
#include "pg/functions/json.h"
#include "pg/functions/lexize.h"
#include "pg/functions/size.h"
#include "pg/pg_types.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config.h"
#include "query/types.h"

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
    auto catalog = SerenedServer::Instance()
                     .getFeature<catalog::CatalogFeature>()
                     .Global()
                     .GetSnapshot();
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
struct VersionFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(mbkkt) Don't use hard-coded version
  // PG version should be from libpg_query,
  // SereneDB version should be from build.h
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) {
    out = "PostgreSQL 18.1 (SereneDB 26.03.1)";
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
DEFINE_NOT_SUPPORTED_FUNC(NotSupported2CharInt, out_type<velox::Varchar>&,
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
struct FormatTypeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // TODO(Pasha) Account typmod?
  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int64_t>& type_oid,
                                const arg_type<int64_t>&) {
    result = RegtypeOut(static_cast<int32_t>(type_oid));
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

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result,
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
                                const arg_type<int32_t>& input) {
    result = RegtypeOut(input);
  }
};

template<typename T>
struct RegclassInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varchar>* /*input*/,
    const arg_type<int32_t>* /*location*/) {
    _conn = basics::downCast<const ConnectionContext>(config.config()).get();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<int32_t>& result,
                                const arg_type<velox::Varchar>& input,
                                const arg_type<int32_t>& location) {
    result = RegclassIn(*_conn, input);
    if (result == kInvalidOid) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_TABLE), CURSOR_POS(location),
        ERR_MSG("relation \"", std::string_view{input}, "\" does not exist"));
    }
  }

  const ConnectionContext* _conn;
};

template<typename T>
struct RegclassOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<int32_t>* /*input*/) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    _db_id = conn->GetDatabaseId();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<int32_t>& input) {
    result = RegclassOut(input);
  }

  ObjectId _db_id;
};

template<typename T>
struct PgTypeofFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
    const std::vector<velox::TypePtr>& inputTypes,
    const velox::core::QueryConfig& config, const arg_type<velox::Any>* input) {
    _type_oid = GetTypeOID(inputTypes[0]);
  }

  FOLLY_ALWAYS_INLINE bool callNullable(out_type<int32_t>& result,
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

  velox::registerFunction<RegtypeInFunction, RegtypeCustomType, velox::Varchar,
                          int32_t>({prefix + "regtypein"});
  velox::registerFunction<RegtypeOutFunction, velox::Varchar,
                          RegtypeCustomType>({prefix + "regtypeout"});
  velox::registerFunction<RegclassInFunction, RegclassCustomType,
                          velox::Varchar, int32_t>({prefix + "regclassin"});
  velox::registerFunction<RegclassOutFunction, velox::Varchar,
                          RegclassCustomType>({prefix + "regclassout"});

  // 9.27.1 Session Information Functions

  velox::registerFunction<CurrentSchemaFunction, velox::Varchar>(
    {prefix + "current_schema"});
  velox::registerFunction<CurrentSchemasFunction, velox::Array<velox::Varchar>,
                          bool>({prefix + "current_schemas"});
  velox::registerFunction<CurrentUserFunction, velox::Varchar>(
    {prefix + "current_user"});
  velox::registerFunction<CurrentDatabaseFunction, velox::Varchar>(
    {prefix + "current_database"});
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
  velox::registerFunction<AlwaysTrueFunction1Int, bool, int64_t>(
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

  velox::registerFunction<NotSupported2CharInt, velox::Varchar, int8_t,
                          int64_t>({prefix + "acldefault"});
  velox::registerFunction<NotSupported1IntArray, velox::Varchar,
                          velox::Array<int64_t>>({prefix + "aclexplode"});
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

  // 9.27.4 System Catalog Information Functions

  velox::registerFunction<FormatTypeFunction, velox::Varchar, int64_t, int64_t>(
    {prefix + "format_type"});
  velox::registerFunction<NotSupported1Int, velox::Varchar, int64_t>(
    {prefix + "basetype"});
  velox::registerFunction<PgCharToEncodingFunction, int32_t, velox::Varchar>(
    {prefix + "char_to_encoding"});
  velox::registerFunction<PgEncodingToCharFunction, velox::Varchar, int32_t>(
    {prefix + "encoding_to_char"});
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
}

}  // namespace sdb::pg::functions
