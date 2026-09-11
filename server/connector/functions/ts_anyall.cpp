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
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/search/term_set.hpp>
#include <iresearch/utils/string.hpp>

#include "connector/functions/ts_query_codec.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "search.h"
#include "ts_common.hpp"

namespace sdb::connector {
namespace {

bool IsTokenizeListCall(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& f = expr.Cast<duckdb::BoundFunctionExpression>();
  if (f.Function().GetName().GetIdentifierName() != kTSQTokenize) {
    return false;
  }
  return f.GetReturnType().id() == duckdb::LogicalTypeId::LIST &&
         IsTSQueryStructType(duckdb::ListType::GetChildType(f.GetReturnType()));
}

void FromTokenizeListInAnyAllOf(
  BoolTarget parent, const FilterContext& ctx,
  const SearchColumnInfo& column_info,
  const duckdb::BoundFunctionExpression& outer,
  const duckdb::BoundFunctionExpression& tokenize_call, bool is_any) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_any(ts_tokenize(['quick', 'brown'])). Tokenises each list "
    "element through the column analyzer.";
  SDB_ASSERT(is_any || outer.GetChildren().size() == 1);
  std::optional<size_t> min_match;
  if (is_any && outer.GetChildren().size() == 2) {
    int64_t m;
    GetIntArg(*outer.GetChildren()[1], m, {"ts_any min_match", kSyntaxHint});
    if (m < 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_any min_match must be >= 1, got ", m),
                      ERR_HINT(kSyntaxHint));
    }
    min_match = static_cast<size_t>(m);
  }

  SDB_ASSERT(tokenize_call.GetChildren().size() >= 1 &&
             tokenize_call.GetChildren().size() <= 2);
  const auto* list_const = TryGetConstant(*tokenize_call.GetChildren()[0]);
  if (!list_const) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_tokenize array form requires a constant text "
                            "array"),
                    ERR_HINT(kSyntaxHint));
  }
  if (list_const->IsNull()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_tokenize text array must not be NULL"),
                    ERR_HINT(kSyntaxHint));
  }
  const auto list_const_id = list_const->type().id();
  if (list_const_id != duckdb::LogicalTypeId::LIST &&
      list_const_id != duckdb::LogicalTypeId::ARRAY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_tokenize array form: first arg must be a list or array, "
              "got ",
              list_const->type().ToString()),
      ERR_HINT(kSyntaxHint));
  }
  bool use_identity = false;
  catalog::Tokenizer::TokenizerWrapper override_wrapper;
  if (tokenize_call.GetChildren().size() == 2) {
    std::string analyzer_name;
    GetVarcharArg(*tokenize_call.GetChildren()[1], analyzer_name,
                  {"ts_tokenize analyzer name", kSyntaxHint});
    if (analyzer_name == irs::KeywordTokenizer::type_name()) {
      use_identity = true;
    } else {
      override_wrapper = AcquireTokenizer(ctx.client_context, analyzer_name);
      if (!override_wrapper) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("ts_tokenize(text_array, '", analyzer_name,
                  "'): tokenizer not found in catalog"),
          ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY or use "
                   "'",
                   irs::KeywordTokenizer::type_name(),
                   "' for raw bytes per element."));
      }
    }
  }

  auto* analyzer = override_wrapper ? override_wrapper.get() : &ctx.tokenizer;
  if (!use_identity &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR &&
      column_info.logical_type.id() != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_tokenize array form requires a VARCHAR-indexed "
                            "column"),
                    ERR_HINT(kSyntaxHint));
  }
  std::vector<irs::bstring> tokens;
  irs::ValueAnalyzer value_analyzer;
  irs::ValueTokens value_tokens;
  const auto& elems = ListOrArrayChildren(*list_const);
  for (const auto& elem : elems) {
    if (elem.IsNull()) {
      continue;
    }
    if (elem.type().id() != duckdb::LogicalTypeId::VARCHAR &&
        elem.type().id() != duckdb::LogicalTypeId::BLOB) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_tokenize text array elements must be VARCHAR or BLOB, got ",
                elem.type().ToString()),
        ERR_HINT(kSyntaxHint));
    }
    auto raw = duckdb::StringValue::Get(elem);
    if (use_identity) {
      auto bytes = irs::ViewCast<irs::byte_type>(std::string_view{raw});
      tokens.emplace_back(bytes.begin(), bytes.end());
      continue;
    }
    if (!value_analyzer.Analyze(*analyzer, raw, value_tokens)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Failed to analyse '", raw, "'"),
        ERR_HINT("The selected analyzer rejected this list element."));
    }
    for (const auto& t : value_tokens.terms()) {
      tokens.emplace_back(irs::AsBytesView(t));
    }
  }

  if (tokens.empty()) {
    AddMaybeNegated<irs::Empty>(parent, ctx, column_info);
    return;
  }

  const auto field =
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR);

  // Single-token short-circuit -> one term clause.
  if (tokens.size() == 1) {
    AddTerm(MaybeNegated(parent, ctx, column_info), field, tokens[0], ctx.boost,
            LeafScorer(column_info));
    return;
  }

  // Aggregate as one term-set node with the min_match policy:
  //   ts_any without min_match -> 1
  //   ts_any(min_match=N) -> N (capped at tokens.size())
  //   ts_all -> tokens.size()
  size_t min_match_value = 1;
  if (!is_any) {
    min_match_value = tokens.size();
  } else if (min_match) {
    min_match_value = std::min<size_t>(*min_match, tokens.size());
  }
  auto& node = AddTermSet(MaybeNegated(parent, ctx, column_info), field, tokens,
                          min_match_value);
  node.SetBoost(ctx.boost);
  node.SetScorer(LeafScorer(column_info));
}

}  // namespace

void ExtractAnyAllOfArgs(
  const duckdb::BoundFunctionExpression& func, bool is_any,
  std::vector<const duckdb::Expression*>& args,
  std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised,
  std::optional<size_t>& min_match) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_any(['a', 'b'], 1) (OR), ts_all(['a', 'b']) (AND).";
  SDB_ASSERT(func.GetChildren().size() >= 1 && func.GetChildren().size() <= 2);
  SDB_ASSERT(is_any || func.GetChildren().size() == 1);

  const auto& list_expr = *func.GetChildren()[0];
  const auto list_type_id = list_expr.GetReturnType().id();
  if (list_type_id != duckdb::LogicalTypeId::LIST &&
      list_type_id != duckdb::LogicalTypeId::ARRAY) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_any/ts_all first argument must be a list or array"),
      ERR_HINT(kSyntaxHint));
  }
  if (list_expr.GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val =
      list_expr.Cast<duckdb::BoundConstantExpression>().GetValue();
    if (val.IsNull()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("list arg must not be NULL"),
                      ERR_HINT(kSyntaxHint));
    }
    const auto& children = ListOrArrayChildren(val);
    for (const auto& child_val : children) {
      synthesised.push_back(
        duckdb::make_uniq<duckdb::BoundConstantExpression>(child_val));
      args.push_back(synthesised.back().get());
    }
  } else if (list_expr.GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& list_fn = list_expr.Cast<duckdb::BoundFunctionExpression>();
    const auto& list_fn_name = list_fn.Function().GetName().GetIdentifierName();
    if (list_fn_name != "list_value" && list_fn_name != "array_value") {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("list arg must be a literal list or array (got: ",
                              list_fn_name, ")"),
                      ERR_HINT(kSyntaxHint));
    }
    for (const auto& e : list_fn.GetChildren()) {
      args.push_back(e.get());
    }
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("list arg must be a literal list or array"),
                    ERR_HINT(kSyntaxHint));
  }

  if (func.GetChildren().size() == 2) {
    int64_t m;
    GetIntArg(*func.GetChildren()[1], m, {"ts_any min_match", kSyntaxHint});
    if (m < 1) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_any min_match must be >= 1, got ", m),
                      ERR_HINT(kSyntaxHint));
    }
    min_match = static_cast<size_t>(m);
  }

  if (args.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(is_any ? "ts_any requires at least one argument"
                                   : "ts_all requires at least one argument"),
                    ERR_HINT(kSyntaxHint));
  }
  if (min_match && *min_match > args.size()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_any min_match (", *min_match,
              ") exceeds number of arguments (", args.size(), ")"),
      ERR_HINT(kSyntaxHint));
  }
}

void FromAnyAllOf(BoolTarget parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func, bool is_any) {
  // Special case: ts_any/ts_all wrapping a ts_tokenize(text_array[, name])
  // call. Tokenise every element into one optional bucket at the appropriate
  // threshold. Bypasses the per-arg BuildTSQuery loop so we can emit one
  // aggregated filter rather than N individual leaves.
  if (!func.GetChildren().empty() &&
      IsTokenizeListCall(*func.GetChildren()[0])) {
    FromTokenizeListInAnyAllOf(
      parent, ctx, column_info, func,
      func.GetChildren()[0]->Cast<duckdb::BoundFunctionExpression>(), is_any);
    return;
  }
  std::vector<const duckdb::Expression*> args;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> synthesised;
  std::optional<size_t> min_match;
  ExtractAnyAllOfArgs(func, is_any, args, synthesised, min_match);

  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  sub_ctx.negated = false;

  const auto group = AddGroup(MaybeNegated(parent, ctx, column_info),
                              is_any ? irs::Occur::Should : irs::Occur::Must);
  group.node->SetBoost(ctx.boost);
  for (const auto* arg : args) {
    BuildTSQuery(group, sub_ctx, column_info, *arg);
  }
  if (is_any) {
    SetMinMatch(*group.node, min_match.value_or(1));
  }
}

}  // namespace sdb::connector
