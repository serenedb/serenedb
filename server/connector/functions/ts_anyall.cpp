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
  irs::BooleanFilter& parent, const FilterContext& ctx,
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
  // Inner list -- v1 requires a constant LIST(VARCHAR).
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
  // Resolve the analyzer choice:
  //   1-arg form               -> ambient column analyzer
  //   2-arg with 'keyword'    -> raw bytes per element (no analysis)
  //   2-arg with named name    -> resolve via catalog at filter-build time
  bool use_identity = false;
  // Hold the wrapper as a stack local so its raw pointer (used by
  // `analyzer` below) stays valid for the loop. Drops at function
  // return -> analyzer goes back to the catalog Tokenizer's pool.
  catalog::Tokenizer::TokenizerWrapper override_wrapper;
  if (tokenize_call.GetChildren().size() == 2) {
    std::string analyzer_name;
    GetVarcharArg(*tokenize_call.GetChildren()[1], analyzer_name,
                  {"ts_tokenize analyzer name", kSyntaxHint});
    if (analyzer_name == irs::StringTokenizer::type_name()) {
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
                   irs::StringTokenizer::type_name(),
                   "' for raw bytes per element."));
      }
    }
  }

  // Walk every element, tokenise it (or take it raw for identity), and
  // accumulate produced tokens. Empty inputs / NULL elements are skipped
  // -- they contribute no terms.
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
    if (!analyzer->reset(raw)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Failed to analyse '", raw, "'"),
        ERR_HINT("The selected analyzer rejected this list element."));
    }
    const auto* tok_attr = irs::get<irs::TermAttr>(*analyzer);
    while (analyzer->next()) {
      tokens.emplace_back(tok_attr->value.begin(), tok_attr->value.end());
    }
  }

  if (tokens.empty()) {
    AddMaybeNegated<irs::Empty>(parent, ctx, column_info);
    return;
  }

  // Single-token short-circuit -> ByTerm.
  if (tokens.size() == 1) {
    auto& term = AddMaybeNegated<irs::ByTerm>(parent, ctx, column_info);
    term.boost(ctx.boost);
    *term.mutable_field_id() =
      PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR);
    term.mutable_options()->term.assign(tokens[0]);
    return;
  }

  // Aggregate as ByTerms with the min_match policy:
  //   ts_any without min_match -> 1
  //   ts_any(min_match=N) -> N (capped at tokens.size())
  //   ts_all -> tokens.size()
  size_t min_match_value = 1;
  if (!is_any) {
    min_match_value = tokens.size();
  } else if (min_match) {
    min_match_value = std::min<size_t>(*min_match, tokens.size());
  }
  auto& terms = AddMaybeNegated<irs::ByTerms>(parent, ctx, column_info);
  terms.boost(ctx.boost);
  *terms.mutable_field_id() =
    PickPerKindFieldId(column_info, duckdb::LogicalTypeId::VARCHAR);
  auto& opts = *terms.mutable_options();
  opts.min_match = min_match_value;
  for (auto& t : tokens) {
    opts.terms.emplace(std::move(t));
  }
}

}  // namespace

// Parses an `ts_any` / `ts_all` call into (args, synthesised, min_match).
// `synthesised` owns BoundConstantExpression wrappers when the list was
// constant-folded; the caller must keep it alive for the duration of `args`
// use. `min_match` is empty for `ts_all` and for `ts_any` when not provided.
void ExtractAnyAllOfArgs(
  const duckdb::BoundFunctionExpression& func, bool is_any,
  std::vector<const duckdb::Expression*>& args,
  std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised,
  std::optional<size_t>& min_match) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_any(['a', 'b'], 1) (OR), ts_all(['a', 'b']) (AND).";
  SDB_ASSERT(func.GetChildren().size() >= 1 && func.GetChildren().size() <= 2);
  SDB_ASSERT(is_any || func.GetChildren().size() == 1);

  // DuckDB constant-folds `['a', 'b']` into a BOUND_CONSTANT holding a
  // LIST/ARRAY Value rather than a `list_value`/`array_value` function
  // call. Support both shapes (and both LIST and fixed-length ARRAY):
  // synthesised BoundConstantExpression wrappers per child Value in the
  // folded case, raw child expression pointers otherwise.
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

void FromAnyAllOf(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func, bool is_any) {
  // Special case: ts_any/ts_all wrapping a ts_tokenize(text_array[, name])
  // call. Tokenise every element, flatten into a single ByTerms with the
  // appropriate min_match. Bypasses the per-arg BuildTSQuery loop so we
  // can emit one aggregated filter rather than N individual leaves.
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

  irs::BooleanFilter* group;
  if (is_any) {
    auto& or_group = AddMaybeNegated<irs::Or>(parent, ctx, column_info);
    if (min_match && *min_match > 1) {
      or_group.min_match_count(*min_match);
    }
    group = &or_group;
  } else {
    group = &AddMaybeNegated<irs::And>(parent, ctx, column_info);
  }
  group->boost(ctx.boost);
  for (const auto* arg : args) {
    BuildTSQuery(*group, sub_ctx, column_info, *arg);
  }
}

}  // namespace sdb::connector
