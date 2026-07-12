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

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/exceptions.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search.h"
#include "ts_common.hpp"

namespace sdb::connector {

void FromTokenize(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_tokenize('quick fox') or ts_tokenize('foo', 'keyword').";
  SDB_ASSERT(func.GetChildren().size() >= 1 && func.GetChildren().size() <= 2);
  std::string text;
  GetVarcharArg(*func.GetChildren()[0], text,
                {"ts_tokenize text", kSyntaxHint});
  if (func.GetChildren().size() == 1) {
    BuildFtsTokens(parent, ctx, column_info, text, /*require_all=*/false);
    return;
  }
  std::string analyzer_name;
  GetVarcharArg(*func.GetChildren()[1], analyzer_name,
                {"ts_tokenize analyzer name", kSyntaxHint});
  if (analyzer_name == irs::StringTokenizer::type_name()) {
    BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
    return;
  }
  auto wrapper = AcquireTokenizer(ctx.client_context, analyzer_name);
  if (!wrapper) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("ts_tokenize(text, '", analyzer_name,
                            "'): tokenizer not found in catalog"),
                    ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
                             "or use 'keyword' for raw bytes."));
  }
  auto sub_ctx = ctx.WithTokenizer(*wrapper);
  BuildFtsTokens(parent, sub_ctx, column_info, text, /*require_all=*/false);
}

}  // namespace sdb::connector
