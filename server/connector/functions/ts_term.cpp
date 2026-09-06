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

#include <absl/status/status.h>

#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/search/term_set.hpp>
#include <iresearch/utils/string.hpp>

#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "ts_common.hpp"

namespace sdb::connector {

absl::Status SetupTermClause(irs::TermClause& clause,
                             const SearchColumnInfo& column_info,
                             const duckdb::Value& value);

void BuildFtsTerm(BoolTarget parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::Value& value) {
  if (value.IsNull()) {
    AddFilter<irs::Empty>(parent);
    return;
  }

  irs::TermClause clause{
    .scorer = LeafScorer(column_info),
    .boost = ctx.boost,
  };
  // SetupTermClause declines for unsupported column types (it is shared with
  // the speculative comparison path); under ts_* syntax that is a user error.
  if (auto s = SetupTermClause(clause, column_info, value); !s.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE), ERR_MSG(s.message()),
      ERR_HINT("The value's type must match the column's indexed type."));
  }
  MaybeNegated(parent, ctx, column_info).Add(std::move(clause));
}

void BuildFtsTokens(BoolTarget parent, const FilterContext& ctx,
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
    AddMaybeNegated<irs::Empty>(parent, ctx, column_info);
    return;
  }
  const auto field_id =
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR);
  if (tokens.size() == 1) {
    AddTerm(MaybeNegated(parent, ctx, column_info), field_id, tokens[0],
            ctx.boost);
    return;
  }
  // Multi-token: one term-set node, every token required (AND) or one of
  // them (OR).
  AddTermSet(MaybeNegated(parent, ctx, column_info), field_id, tokens,
             require_all ? tokens.size() : 1)
    .SetBoost(ctx.boost);
}
void FromTerm(BoolTarget parent, const FilterContext& ctx,
              const SearchColumnInfo& column_info,
              const duckdb::BoundFunctionExpression& func) {
  if (func.GetChildren().size() != 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("bare-string term expects 1 argument (text), got ",
                            func.GetChildren().size()),
                    ERR_HINT("Example: 'word' (bare-string literal)."));
  }
  std::string text;
  GetVarcharArg(*func.GetChildren()[0], text,
                {"term text", "Example: 'word' (bare-string literal)."});
  BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
}

}  // namespace sdb::connector
