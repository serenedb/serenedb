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

#include "pg/functions/inout.h"

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
#include "pg/functions/interval.h"
#include "pg/serialize.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/types.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::functions {
namespace {

template<typename T>
struct ByteaOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varbinary>*) {
    auto cfg = basics::downCast<const Config>(config.config());
    auto bytea_output = cfg->Get<VariableType::PgByteaOutput>("bytea_output");

    _bytea_output = std::move(bytea_output);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varchar>& input) {
    if (_bytea_output == ByteaOutput::Hex) {
      const auto required_size = 2 + 2 * input.size();
      result.resize(required_size);
      ByteaOutHex<false>(result.data(), input);
    } else {
      SDB_ASSERT(_bytea_output == ByteaOutput::Escape);
      const auto required_size = ByteaOutEscapeLength<false>(input);
      result.resize(required_size);
      ByteaOutEscape<false>(result.data(), input);
    }
  }

 private:
  ByteaOutput _bytea_output;
};

template<typename T>
struct ByteaInFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(out_type<velox::Varbinary>& result,
                                const arg_type<velox::Varchar>& value) {
    std::string_view input{value};
    if (input.starts_with("\\x")) {
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
struct IntervalOutFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Varchar>& result,
                                const arg_type<Interval>& interval_packed) {
    result = IntervalOut(interval_packed);
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
    _ctx = basics::downCast<const ConnectionContext>(config.config().get());
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

}  // namespace

void registerInOutFunctions(const std::string& prefix) {
  // Internal type I/O and operator functions (not in 9.27)

  velox::registerFunction<ByteaInFunction, velox::Varbinary, velox::Varchar>(
    {prefix + "byteain"});
  velox::registerFunction<ByteaOutFunction, velox::Varchar, velox::Varbinary>(
    {prefix + "byteaout"});

  velox::registerFunction<IntervalInFunction, Interval, velox::Varchar, int32_t,
                          int32_t>({prefix + "intervalin"});
  velox::registerFunction<IntervalOutFunction, velox::Varchar, Interval>(
    {prefix + "intervalout"});

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
}

}  // namespace sdb::pg::functions
