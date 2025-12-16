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
#include "pg/extract.h"
#include "pg/interval.h"
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

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<int64_t>& input) {
    if (input == 10) {
      result = StaticStrings::kPgDefaultUser;
    } else {
      result = absl::StrCat("unknown (OID=", input, ")");
    }
  }
};

template<typename T>
struct GetViewDef {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<int64_t>& input) {
    result = "";
  }
};

template<typename T>
struct GetRuleDef {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<int64_t>& input) {
    result = "";
  }
};

template<typename T>
struct ByteaOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(  // NOLINT
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config,
    const arg_type<velox::Varbinary>* /*input*/) {
    auto cfg = basics::downCast<const Config>(config.config());
    auto bytea_output = cfg->Get<VariableType::PgByteaOutput>("bytea_output");

    _bytea_output = std::move(bytea_output);
  }

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varbinary>& result, const arg_type<velox::Varchar>& input) {
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

  FOLLY_ALWAYS_INLINE bool call(  // NOLINT
    out_type<velox::Varbinary>& result, const arg_type<velox::Varchar>& input) {
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

  FOLLY_ALWAYS_INLINE void initialize(  // NOLINT
    const std::vector<velox::TypePtr>& /*inputTypes*/,
    const velox::core::QueryConfig& config) {
    auto conn_ctx = basics::downCast<const ConnectionContext>(config.config());
    SDB_ASSERT(conn_ctx);
    _schema_name = conn_ctx->GetCurrentSchema();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& out) {  // NOLINT
    out = _schema_name;
  }

 private:
  std::string _schema_name;
};

template<typename T>
struct CurrentSchemasFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(  // NOLINT
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

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Array<velox::Varchar>>& out,
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
struct PgTableIsVisible {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<bool>& result, const arg_type<int64_t>& relid) {
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

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Varchar>& pattern) {
    result =
      EscapePattern(std::string_view{pattern.data(), pattern.size()}, '\\');
  }

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Varchar>& pattern,
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

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Varchar>& pattern,
    const arg_type<velox::Varchar>& escape) {
    std::string_view escape_sv{escape.data(), escape.size()};

    if (escape_sv.size() != 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_ESCAPE_SEQUENCE),
                      ERR_MSG("invalid escape string: must be one character"));
    }

    result = EscapePattern({pattern.data(), pattern.size()}, escape_sv[0]);
  }

  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Varchar>& pattern) {
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
  FOLLY_ALWAYS_INLINE void call(  // NOLINT
    out_type<velox::Varchar>& result, const arg_type<velox::Varchar>& pattern) {
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
}  // namespace

void registerFunctions(const std::string& prefix) {
  velox::registerFunction<ByteaInFunction, velox::Varbinary, velox::Varchar>(
    {prefix + "byteain"});
  velox::registerFunction<ByteaOutFunction, velox::Varchar, velox::Varbinary>(
    {prefix + "byteaout"});
  velox::registerFunction<CurrentSchemaFunction, velox::Varchar>(
    {prefix + "current_schema"});
  velox::registerFunction<CurrentSchemasFunction, velox::Array<velox::Varchar>,
                          bool>({prefix + "current_schemas"});

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

  velox::registerFunction<GetUserByIdFunction, velox::Varchar, int64_t>(
    {prefix + "get_userbyid"});
  velox::registerFunction<GetViewDef, velox::Varchar, int64_t>(
    {prefix + "get_viewdef"});
  velox::registerFunction<GetRuleDef, velox::Varchar, int64_t>(
    {prefix + "get_ruledef"});
  velox::registerFunction<PgTableIsVisible, bool, int64_t>(
    {prefix + "table_is_visible"});
  velox::registerFunction<PgSimilarToEscape, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "similar_to_escape"});
  velox::registerFunction<PgSimilarToEscape, velox::Varchar, velox::Varchar>(
    {prefix + "similar_to_escape"});
  velox::registerFunction<PgLikeEscape, velox::Varchar, velox::Varchar,
                          velox::Varchar>({prefix + "like_escape"});
  velox::registerFunction<ProcessEscapePattern, velox::Varchar, velox::Varchar>(
    {prefix + "process_escape_pattern"});

  registerExtractFunctions(prefix);
}

}  // namespace sdb::pg::functions
