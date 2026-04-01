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

#include "pg/functions/util.h"

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

#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "pg/connection_context.h"
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
struct PgTsLexize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(const std::vector<velox::TypePtr>&,
                                      const velox::core::QueryConfig& config,
                                      const arg_type<velox::Varchar>*,
                                      const arg_type<velox::Varchar>*) {
    auto conn = basics::downCast<const ConnectionContext>(config.config());
    db_id = conn->GetDatabaseId();
    current_schema = conn->GetCurrentSchema();
    snapshot = conn->EnsureCatalogSnapshot();
  }

  FOLLY_ALWAYS_INLINE void call(out_type<velox::Array<velox::Varchar>>& result,
                                const arg_type<velox::Varchar>& tokenizer_name,
                                const arg_type<velox::Varchar>& text) {
    auto object_name = ParseObjectName(tokenizer_name, current_schema);
    auto dict =
      snapshot->GetTokenizer(db_id, object_name.schema, object_name.relation);
    if (!dict) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_NAME),
        ERR_MSG("text search dictionary \"", std::string_view{tokenizer_name},
                "\" does not exist"));
    }

    auto tokenizer = dict->GetTokenizer();
    if (!tokenizer) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG(tokenizer.error().errorMessage()));
    }
    bool r = (*tokenizer)->reset(text);
    if (!r) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("error while preparing tokenizer"));
    }
    auto* value = irs::get<irs::TermAttr>(**tokenizer);
    while ((*tokenizer)->next()) {
      result.add_item().copy_from(irs::ViewCast<char>(value->value));
    }
  }

  ObjectId db_id;
  std::string current_schema;
  std::shared_ptr<const catalog::Snapshot> snapshot;
};

}  // namespace

void registerUtilFunctions(const std::string& prefix) {
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

  velox::registerFunction<NumNonNullsFunction, int32_t,
                          velox::Variadic<velox::Any>>(
    {prefix + "num_nonnulls"});
  velox::registerFunction<NumNullsFunction, int32_t,
                          velox::Variadic<velox::Any>>({prefix + "num_nulls"});

  velox::registerFunction<PgTsLexize, velox::Array<velox::Varchar>,
                          velox::Varchar, velox::Varchar>(
    {prefix + "ts_lexize"});
}

}  // namespace sdb::pg::functions
