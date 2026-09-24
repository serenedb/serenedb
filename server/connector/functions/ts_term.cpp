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
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>

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

namespace {

template<typename Tokens>
void AnalyzeText(irs::analysis::Tokenizer& analyzer, std::string_view text,
                 Tokens& tokens) {
  irs::ValueAnalyzer value_analyzer;
  if (!value_analyzer.Analyze(
        analyzer,
        duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())},
        tokens)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Failed to analyse '", text, "'"),
                    ERR_HINT("The column's analyzer rejected the input text."));
  }
}

void AddTokenGroup(BoolTarget parent, irs::field_id field,
                   std::span<irs::bstring> group, const irs::Scorer* scorer) {
  SDB_ASSERT(!group.empty());
  if (group.size() == 1) {
    AddTerm(parent, field, group.front(), irs::kNoBoost, scorer);
    return;
  }
  auto& node = AddTermSet(parent, field, group, 1);
  node.SetMergeType(irs::ScoreMergeType::Max);
  node.SetScorer(scorer);
}

}  // namespace

void AppendTokenGroups(std::span<const duckdb::string_t> terms,
                       std::span<const uint32_t> pos, TokenGroups& groups) {
  SDB_ASSERT(terms.size() == pos.size());
  for (size_t i = 0; i < terms.size();) {
    size_t end = i + 1;
    while (end < terms.size() && pos[end] == pos[i]) {
      ++end;
    }
    auto& group = groups.emplace_back();
    group.reserve(end - i);
    for (size_t k = i; k < end; ++k) {
      group.emplace_back(irs::AsBytesView(terms[k]));
    }
    std::sort(group.begin(), group.end());
    group.erase(std::unique(group.begin(), group.end()), group.end());
    i = end;
  }
}

void AddTokenGroups(BoolTarget parent, irs::field_id field, TokenGroups& groups,
                    size_t min_match, irs::score_t boost,
                    const irs::Scorer* scorer) {
  SDB_ASSERT(!groups.empty());
  std::sort(groups.begin(), groups.end());
  groups.erase(std::unique(groups.begin(), groups.end()), groups.end());
  min_match = std::min(min_match, groups.size());
  const bool stacked = std::ranges::any_of(
    groups, [](const auto& group) { return group.size() > 1; });
  if (!stacked) {
    if (groups.size() == 1) {
      AddTerm(parent, field, groups.front().front(), boost, scorer);
      return;
    }
    std::vector<irs::bstring> terms;
    terms.reserve(groups.size());
    for (auto& group : groups) {
      terms.push_back(std::move(group.front()));
    }
    auto& node = AddTermSet(parent, field, terms, min_match);
    node.SetBoost(boost);
    node.SetScorer(scorer);
    return;
  }
  if (groups.size() == 1) {
    auto& node = AddTermSet(parent, field, groups.front(), 1);
    node.SetMergeType(irs::ScoreMergeType::Max);
    node.SetBoost(boost);
    node.SetScorer(scorer);
    return;
  }
  const bool all = min_match >= groups.size();
  const auto node =
    AddGroup(parent, all ? irs::Occur::Must : irs::Occur::Should);
  node.node->SetBoost(boost);
  for (auto& group : groups) {
    AddTokenGroup(node, field, group, scorer);
  }
  if (!all) {
    SetMinMatch(*node.node, min_match);
  }
}

void BuildFtsTokens(BoolTarget parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info, std::string_view text,
                    bool require_all) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    BuildFtsTerm(parent, ctx, column_info, duckdb::Value(std::string{text}));
    return;
  }
  irs::ValueTokens<irs::TokenLayout::TermsPos> tokens{ctx.tokenizer.Traits()};
  AnalyzeText(ctx.tokenizer, text, tokens);
  if (tokens.terms().empty()) {
    AddMaybeNegated<irs::Empty>(parent, ctx, column_info);
    return;
  }
  TokenGroups groups;
  AppendTokenGroups(tokens.terms(), tokens.pos(), groups);
  const auto min_match = require_all ? groups.size() : size_t{1};
  AddTokenGroups(
    MaybeNegated(parent, ctx, column_info),
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR), groups,
    min_match, ctx.boost);
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
