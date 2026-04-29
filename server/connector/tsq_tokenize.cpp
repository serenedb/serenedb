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

#include "functions/search.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "tsq_common.hpp"

namespace sdb::connector {

Result FromTokenize(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func) {
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            "TOKENIZE expects 1 or 2 arguments (text[, analyzer]), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "TOKENIZE text", text);
      !r.ok()) {
    return r;
  }
  if (func.children.size() == 1) {
    return BuildFtsTokens(parent, ctx, column_info, text,
                          /*require_all=*/false);
  }
  std::string analyzer_name;
  if (auto r = GetVarcharArg(*func.children[1], "TOKENIZE analyzer name",
                             analyzer_name);
      !r.ok()) {
    return r;
  }
  if (analyzer_name == irs::StringTokenizer::type_name()) {
    return BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
  }
  auto wrapper = ResolveTokenizerAnalyzer(ctx.client_context, analyzer_name);
  if (!wrapper) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("TOKENIZE(text, '", analyzer_name,
                            "'): tokenizer not found in catalog"),
                    ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
                             "or use 'identity' for raw bytes."));
  }
  auto sub_ctx = ctx.WithTokenizer(*wrapper);
  return BuildFtsTokens(parent, sub_ctx, column_info, text,
                        /*require_all=*/false);
}

}  // namespace sdb::connector
