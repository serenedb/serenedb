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
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/utils/string.hpp>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

Result SetupTermFilter(irs::ByTerm& filter, const SearchColumnInfo& column_info,
                       const duckdb::Value& value);

void BuildFtsTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::Value& value) {
  if (value.IsNull()) {
    AddFilter<irs::Empty>(parent);
    return;
  }
  auto& term =
    ctx.negated ? Negate<irs::ByTerm>(parent) : AddFilter<irs::ByTerm>(parent);
  term.boost(ctx.boost);
  if (auto r = SetupTermFilter(term, column_info, value); !r.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE), ERR_MSG(r.errorMessage()),
      ERR_HINT("The value's type must match the column's indexed type."));
  }
}

void BuildFtsTokens(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string_view text,
                    bool require_all) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    BuildFtsTerm(parent, ctx, column_info, duckdb::Value(std::string{text}));
    return;
  }
  auto& analyzer = ctx.tokenizer;
  std::vector<irs::bstring> tokens;
  if (!analyzer.reset(text)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Failed to analyse '", text, "'"),
                    ERR_HINT("The column's analyzer rejected the input text."));
  }
  const auto* tok_attr = irs::get<irs::TermAttr>(analyzer);
  while (analyzer.next()) {
    tokens.emplace_back(tok_attr->value.begin(), tok_attr->value.end());
  }

  if (tokens.empty()) {
    AddFilter<irs::Empty>(parent);
    return;
  }
  const auto field_id =
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR);
  if (tokens.size() == 1) {
    auto& term = ctx.negated ? Negate<irs::ByTerm>(parent)
                             : AddFilter<irs::ByTerm>(parent);
    term.boost(ctx.boost);
    *term.mutable_field_id() = field_id;
    term.mutable_options()->term.assign(tokens[0]);
    return;
  }
  // Multi-token: ByTerms with min_match=1 (OR) or N (AND).
  auto& terms = ctx.negated ? Negate<irs::ByTerms>(parent)
                            : AddFilter<irs::ByTerms>(parent);
  terms.boost(ctx.boost);
  *terms.mutable_field_id() = field_id;
  auto& opts = *terms.mutable_options();
  opts.min_match = require_all ? tokens.size() : 1;
  for (auto& t : tokens) {
    opts.terms.emplace(std::move(t));
  }
}
void FromTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
              const SearchColumnInfo& column_info,
              const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("bare-string term expects 1 argument (text), got ",
                            func.children.size()),
                    ERR_HINT("Example: 'word' (bare-string literal)."));
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "term text", text); !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()),
                    ERR_HINT("Example: 'word' (bare-string literal)."));
  }
  BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
}

}  // namespace sdb::connector
