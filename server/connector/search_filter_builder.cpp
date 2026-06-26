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

#include "search_filter_builder.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/column_existence_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/levenshtein_filter.hpp>
#include <iresearch/search/mixed_boolean_filter.hpp>
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/prefix_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/regexp_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/search/wildcard_ngram_filter.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/wildcard_utils.hpp>
#include <magic_enum/magic_enum.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "basics/errors.h"
#include "basics/string_utils.h"
#include "comparison_op.hpp"
#include "connector/json_extract_names.hpp"
#include "functions/search.h"
#include "functions/string.h"
#include "functions/ts_common.hpp"
#include "geo_filter_builder.hpp"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::connector::TSQueryOp>(
  sdb::connector::TSQueryOp value) noexcept {
  using enum sdb::connector::TSQueryOp;
  switch (value) {
    case Phrase:
      return sdb::connector::kTSQPhrase;
    case Like:
      return sdb::connector::kTSQLike;
    case Prefix:
      return sdb::connector::kTSQPrefix;
    case Ngram:
      return sdb::connector::kTSQNgram;
    case Fuzzy:
      return sdb::connector::kTSQLevenshtein;
    case Any:
      return sdb::connector::kTSQAnyOf;
    case All:
      return sdb::connector::kTSQAllOf;
    case Between:
      return sdb::connector::kTSQBetween;
    case Regexp:
      return sdb::connector::kTSQRegexp;
    case Less:
      return sdb::connector::kTSQLess;
    case LessEq:
      return sdb::connector::kTSQLessEq;
    case Greater:
      return sdb::connector::kTSQGreater;
    case GreaterEq:
      return sdb::connector::kTSQGreaterEq;
    case Tokenize:
      return sdb::connector::kTSQTokenize;
    case PlainToTsquery:
      return sdb::connector::kPlainToTsquery;
    case PhraseToTsquery:
      return sdb::connector::kPhraseToTsquery;
    case WebsearchToTsquery:
      return sdb::connector::kWebsearchToTsquery;
    case TsqueryPhrase:
      return sdb::connector::kTsqueryPhrase;
    case Or:
      return sdb::connector::kTSQueryOr;
    case And:
      return sdb::connector::kTSQueryAnd;
    case Not:
      return sdb::connector::kTSQueryNot;
    case Boost:
      return sdb::connector::kTSQueryBoost;
    case PhraseSeq:
      return sdb::connector::kTSQueryPhraseSeq;
    case ToTSQuery:
      return sdb::connector::kToTsquery;
    case Compound:
      return sdb::connector::kTSQCompound;
    case IsNull:
      return sdb::connector::kTSQIsNull;
    case IsNotNull:
      return sdb::connector::kTSQIsNotNull;
    case Unknown:
    case Term:
      return invalid_tag;
  }
  return invalid_tag;
}

}  // namespace magic_enum
namespace sdb::connector {
namespace {

// Returns the raw byte content of a Value whose physical type is
// VARCHAR (covers both LogicalType::VARCHAR and LogicalType::BLOB) as
// an irs::bytes_view ready for term-dictionary use. Use this instead
// of GetValue<std::string>() at sites where the constant may arrive
// as BLOB: DuckDB's regex_range_filter optimizer rewrites
// regexp_full_match(col, pat) into
//   col >= BLOB_RAW(min) AND col <= BLOB_RAW(max)
// so the comparison constant against a VARCHAR column may be BLOB
// even though the column is VARCHAR. GetValue<std::string>() on a
// BLOB returns the textual display form (e.g. "\xF4\xBF\xBF\xC0" as
// 24 ASCII chars), which is wrong as a byte-wise term-dictionary
// bound. StringValue::Get returns the raw stored bytes for both
// types because they share PhysicalType::VARCHAR.
irs::bytes_view AsRawBytes(const duckdb::Value& value) {
  return irs::ViewCast<irs::byte_type>(
    std::string_view{duckdb::StringValue::Get(value)});
}

}  // namespace

Result SetupTermFilter(irs::ByTerm& filter, const SearchColumnInfo& column_info,
                       const duckdb::Value& value) {
  SDB_ASSERT(!value.IsNull(),
             "UNKNOWN and Nulls should be handled as part of IS NULL operator. "
             "For regular filter it should be just irs::Empty!");

  auto type_id = column_info.logical_type.id();

  if (auto r = ValidateFilterType(type_id); !r.ok()) {
    return r;
  }

  if (type_id == duckdb::LogicalTypeId::VARCHAR ||
      type_id == duckdb::LogicalTypeId::BLOB) {
    filter.mutable_options()->term.assign(AsRawBytes(value));
  } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
    filter.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(
      irs::BooleanTokenizer::value(value.GetValue<bool>())));
  } else if (IsNumericTypeId(type_id)) {
    irs::NumericTokenizer stream;
    const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
    ResetNumericStream(stream, type_id, value);
    stream.next();
    filter.mutable_options()->term.assign(token->value);
  } else {
    return {ERROR_NOT_IMPLEMENTED,
            "Unsupported type for term filter: ", static_cast<int>(type_id)};
  }

  *filter.mutable_field_id() = PickPerKindFieldId(column_info, type_id);
  return {};
}

namespace {

ComparisonOp InvertComparisonOp(ComparisonOp op) {
  switch (op) {
    case ComparisonOp::Le:
      return ComparisonOp::Gt;
    case ComparisonOp::Ge:
      return ComparisonOp::Lt;
    case ComparisonOp::Gt:
      return ComparisonOp::Le;
    case ComparisonOp::Lt:
      return ComparisonOp::Ge;
    case ComparisonOp::None:
      return ComparisonOp::None;
  }
  SDB_UNREACHABLE();
}

ComparisonOp GetComparisonOp(duckdb::ExpressionType type) {
  switch (type) {
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
      return ComparisonOp::Le;
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
      return ComparisonOp::Lt;
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      return ComparisonOp::Ge;
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return ComparisonOp::Gt;
    default:
      return ComparisonOp::None;
  }
}

template<typename... Args>
std::vector<duckdb::unique_ptr<duckdb::Expression>> MakeChildren(
  Args&&... children) {
  std::vector<duckdb::unique_ptr<duckdb::Expression>> v;
  v.reserve(sizeof...(children));
  (v.emplace_back(std::forward<Args>(children)), ...);
  return v;
}

bool IsAnyTSQueryType(const duckdb::LogicalType& type) {
  if (type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return false;
  }
  const auto alias = type.GetAlias();
  return alias == kTSQueryTypeName || alias == kTokenizedTSQueryTypeName ||
         alias == kBoostedTSQueryTypeName;
}

// Bind does NOT pre-resolve the tokenizer name to a live analyzer
// because the analyzer is stateful (one tokenization stream per use)
// and can't be shared across queries.
std::string_view TryGetTokenizerModifier(const duckdb::LogicalType& type) {
  if (!IsAnyTSQueryType(type) || !type.HasExtensionInfo()) {
    return {};
  }
  const auto* ext = type.GetExtensionInfo().get();
  const auto& mods = ext->modifiers;
  if (mods.empty() || mods[0].value.IsNull() ||
      mods[0].value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {};
  }
  return duckdb::StringValue::Get(mods[0].value);
}

// Boost and tokenizer modifiers are distinguished by value type
// (DOUBLE vs VARCHAR) so the two never alias each other.
std::optional<double> TryGetBoostModifier(const duckdb::LogicalType& type) {
  if (!IsAnyTSQueryType(type) || !type.HasExtensionInfo()) {
    return {};
  }
  const auto* ext = type.GetExtensionInfo().get();
  const auto& mods = ext->modifiers;
  if (mods.empty() || mods[0].value.IsNull() ||
      mods[0].value.type().id() != duckdb::LogicalTypeId::DOUBLE) {
    return {};
  }
  return mods[0].value.GetValue<double>();
}

bool IsComparisonExpr(const duckdb::Expression& expr) {
  return duckdb::BoundComparisonExpression::IsComparison(expr) &&
         GetComparisonOp(expr.GetExpressionType()) != ComparisonOp::None;
}

// Unwraps reinterpret casts between VARCHAR / TSQUERY / TOKENIZED_TSQUERY
// so the filter builder sees through both directions of implicit
// promotion:
//   - VARCHAR -> TSQUERY  (bare string literals into TSQUERY contexts)
//   - TSQUERY -> VARCHAR  (TSQUERY-typed children flowing into VARCHAR
//     mirror overloads of `##`, e.g. ts_phrase('a') ## 1, where DuckDB
//     wraps the LHS in BOUND_CAST<VARCHAR>)
//   - TOK <-> TSQ / VARCHAR transit casts that DON'T carry a tokenize
//     modifier on the cast's return_type. Modifier-bearing casts are
//     preserved here so the BuildTSQuery walker can read the override
//     before continuing to dispatch the inner expression.
// Iterative because casts can chain (e.g. ts_phrase('x')::tokenize('y')
// inside @@ becomes BoundCast<TSQ>(BoundCast<TOK-mod-y>(PHRASE)) -- we
// peel the outer transit cast and stop at the modifier-bearing cast).
// Peels the BOOSTED_TSQUERY -> BOOLEAN coercion that the WhereBinder
// inserts when a `(predicate)::boost(K)` cast appears at the WHERE
// root. Returns the inner cast (whose return_type carries the boost
// modifier) so the SQL-surface peel can read the factor. If `expr`
// isn't that exact shape, returns `expr` unchanged.
const duckdb::Expression& UnwrapBoostBoolCoercion(
  const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return expr;
  }
  const auto& cast = expr.Cast<duckdb::BoundCastExpression>();
  if (!cast.child) {
    return expr;
  }
  if (cast.GetReturnType().id() != duckdb::LogicalTypeId::BOOLEAN) {
    return expr;
  }
  if (!TryGetBoostModifier(cast.child->GetReturnType())) {
    return expr;
  }
  return *cast.child;
}

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr);
void FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& lhs,
                      const duckdb::Expression& rhs);

template<typename Filter>
Result MakeGroup(irs::BooleanFilter& parent, const FilterContext& ctx,
                 const duckdb::BoundConjunctionExpression& conj) {
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  irs::BooleanFilter* group_root;
  if (ctx.negated && absl::c_all_of(conj.children, [](const auto& child) {
        SDB_ASSERT(child);
        return IsComparisonExpr(*child);
      })) {
    // De Morgan's law: if we negate a group of comparisons, comparisons
    // consume negation by inversion so we can reduce NOT filters.
    group_root =
      irs::Type<Filter>::id() == irs::Type<irs::And>::id()
        ? static_cast<irs::BooleanFilter*>(&AddFilter<irs::Or>(parent))
        : static_cast<irs::BooleanFilter*>(&AddFilter<irs::And>(parent));
  } else {
    group_root =
      ctx.negated
        ? static_cast<irs::BooleanFilter*>(&Negate<Filter>(parent))
        : static_cast<irs::BooleanFilter*>(&AddFilter<Filter>(parent));
    sub_ctx.negated = false;
  }
  group_root->boost(ctx.boost);
  for (const auto& child : conj.children) {
    auto result = FromExpression(*group_root, sub_ctx, *child);
    if (!result.ok()) {
      return result;
    }
  }
  return {};
}

Result FromIsNull(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const duckdb::BoundOperatorExpression& op_expr) {
  SDB_ASSERT(op_expr.children.size() == 1);
  const auto* column_info = FindColumnInfoForExpr(ctx, *op_expr.children[0]);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER,
            "IS NULL input is not a reference to an indexed column"};
  }
  auto& term_filter =
    ctx.negated ? Negate<irs::ByTerm>(filter) : AddFilter<irs::ByTerm>(filter);
  term_filter.boost(ctx.boost);
  SDB_ENSURE(irs::field_limits::valid(column_info->null_field_id),
             ERROR_INTERNAL,
             "FromIsNull: column_info has no null_field_id allocated");
  *term_filter.mutable_field_id() = column_info->null_field_id;
  term_filter.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  return {};
}

template<bool GenericVersion>
Result FromBinaryEq(irs::BooleanFilter& filter, const FilterContext& ctx,
                    const duckdb::Expression& left_expr,
                    const duckdb::Expression& right_expr, bool not_equal) {
  // ST_Distance_Centroid(field, centroid) = / != distance  --  rewrite to
  // range.
  if constexpr (GenericVersion) {
    if (const auto* geo_call = TryGetGeoDistanceCall(ctx, left_expr)) {
      FilterContext geo_ctx = ctx;
      geo_ctx.negated = (ctx.negated != not_equal);
      return FromGeoDistanceBinaryEq(filter, geo_ctx, *geo_call, right_expr);
    }
  }

  // Use the JSON-path-aware resolver so `(content->>'val')::int = 42` is
  // claimed by the index: the cast is peeled and routed to the numeric-
  // mangled field, so rows whose leaf isn't numeric simply aren't in the
  // posting list (no runtime cast on incompatible rows).
  const auto* column_info = FindColumnInfoForExpr(ctx, left_expr);
  const auto* const_val = TryGetConstant(right_expr);

  if (!column_info || !const_val) {
    return {ERROR_BAD_PARAMETER,
            "Expected indexed column reference on the left and constant on "
            "the right"};
  }

  if (const_val->IsNull()) {
    // foo == NULL is always false and foo != NULL is false too.
    AddFilter<irs::Empty>(filter);
    return {};
  }
  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by keyword analyzer. Use `col @@ "
              "'value'` (tokenised) or `col @@ "
              "'value'::tokenize('keyword')` (raw)."};
    }
  }

  auto& term_filter = (ctx.negated != not_equal)
                        ? Negate<irs::ByTerm>(filter)
                        : AddFilter<irs::ByTerm>(filter);

  term_filter.boost(ctx.boost);
  return SetupTermFilter(term_filter, *column_info, *const_val);
}

template<bool GenericVersion>
Result FromComparison(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& field_expr,
                      const duckdb::Expression& value_expr, ComparisonOp op) {
  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }

  // ST_Distance_Centroid(field, centroid) </<=/>/>= distance  --  rewrite to
  // range. We've already absorbed `ctx.negated` into `op` via
  // InvertComparisonOp above, so clear it before recursing -- otherwise the
  // geo filter would Negate the range that is itself already inverted.
  if constexpr (GenericVersion) {
    if (const auto* geo_call = TryGetGeoDistanceCall(ctx, field_expr)) {
      FilterContext geo_ctx = ctx;
      geo_ctx.negated = false;
      return FromGeoDistanceComparison(filter, geo_ctx, *geo_call, value_expr,
                                       op);
    }
  }

  const auto* column_info = FindColumnInfoForExpr(ctx, field_expr);
  const auto* const_val = TryGetConstant(value_expr);

  if (!column_info || !const_val) {
    return {ERROR_BAD_PARAMETER,
            "Expected indexed column reference and constant for comparison"};
  }

  if (const_val->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return {};
  }
  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {
        ERROR_BAD_PARAMETER,
        "Field is not indexed by keyword analyzer. Range predicates "
        "(<, <=, >, >=, BETWEEN) require an keyword-analyzed column. "
        "Use `col @@ ts_lt('value')` / `LESS_EQ` / `GREATER` / "
        "`GREATER_EQ` / `ts_between(min, max, ...)` (tokenised through the "
        "column's analyzer) instead."};
    }
  }

  auto type_id = column_info->logical_type.id();

  auto setup_base_filter = [&](auto& range_filter) -> decltype(auto) {
    *range_filter.mutable_field_id() =
      PickPerKindFieldId(*column_info, type_id);
    range_filter.boost(ctx.boost);
    switch (op) {
      case ComparisonOp::Lt:
        range_filter.mutable_options()->range.max_type =
          irs::BoundType::Exclusive;
        return (range_filter.mutable_options()->range.max);
      case ComparisonOp::Le:
        range_filter.mutable_options()->range.max_type =
          irs::BoundType::Inclusive;
        return (range_filter.mutable_options()->range.max);
      case ComparisonOp::Gt:
        range_filter.mutable_options()->range.min_type =
          irs::BoundType::Exclusive;
        return (range_filter.mutable_options()->range.min);
      case ComparisonOp::Ge:
        range_filter.mutable_options()->range.min_type =
          irs::BoundType::Inclusive;
        return (range_filter.mutable_options()->range.min);
      default:
        SDB_ASSERT(false, "Not all comparison operations implemented");
    }
    SDB_UNREACHABLE();
  };

  if (auto r = ValidateFilterType(type_id); !r.ok()) {
    return r;
  }

  if (type_id == duckdb::LogicalTypeId::VARCHAR) {
    auto& range_filter = AddFilter<irs::ByRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    setup_base_filter(range_filter).assign(AsRawBytes(*const_val));
  } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range_filter = AddFilter<irs::ByRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    setup_base_filter(range_filter)
      .assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(const_val->GetValue<bool>())));
  } else if (IsNumericTypeId(type_id)) {
    auto& range_filter = AddFilter<irs::ByGranularRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    irs::NumericTokenizer stream;
    ResetNumericStream(stream, type_id, *const_val);
    irs::SetGranularTerm(setup_base_filter(range_filter), stream);
  } else {
    return {ERROR_NOT_IMPLEMENTED, "Unsupported type for range comparison: ",
            static_cast<int>(type_id)};
  }
  return {};
}

Result FromBetween(irs::BooleanFilter& filter, const FilterContext& ctx,
                   const duckdb::BoundFunctionExpression& between) {
  // Decompose BETWEEN into conjunction of two range comparisons.
  // BETWEEN a AND b  =>  field >= a (or >) AND field <= b (or <)
  // NOT BETWEEN       =>  field < a (or <=) OR field > b (or >=)
  const auto& between_input = duckdb::BoundBetweenExpression::Input(between);
  const auto& between_lower =
    duckdb::BoundBetweenExpression::LowerBound(between);
  const auto& between_upper =
    duckdb::BoundBetweenExpression::UpperBound(between);
  const bool lower_inclusive =
    duckdb::BoundBetweenExpression::LowerInclusive(between);
  const bool upper_inclusive =
    duckdb::BoundBetweenExpression::UpperInclusive(between);
  const auto* lower_val = TryGetConstant(between_lower);
  const auto* upper_val = TryGetConstant(between_upper);
  if (!lower_val || !upper_val) {
    return {ERROR_BAD_PARAMETER, "BETWEEN bounds must be constants"};
  }

  if (!ctx.negated) {
    // field >= lower AND field <= upper (with inclusivity flags)
    auto lower = lower_inclusive ? ComparisonOp::Ge : ComparisonOp::Gt;
    auto upper = upper_inclusive ? ComparisonOp::Le : ComparisonOp::Lt;

    auto& group = AddFilter<irs::And>(filter);
    group.boost(ctx.boost);

    // Sub-context: not negated, no extra boost (already on group)
    FilterContext sub_ctx = ctx;
    sub_ctx.negated = false;
    sub_ctx.boost = irs::kNoBoost;

    auto r =
      FromComparison<true>(group, sub_ctx, between_input, between_lower, lower);
    if (!r.ok()) {
      return r;
    }
    return FromComparison<true>(group, sub_ctx, between_input, between_upper,
                                upper);
  }

  // NOT BETWEEN: De Morgan -> field < lower OR field > upper
  auto lower = lower_inclusive ? ComparisonOp::Lt : ComparisonOp::Le;
  auto upper = upper_inclusive ? ComparisonOp::Gt : ComparisonOp::Ge;

  auto& group = AddFilter<irs::Or>(filter);
  group.boost(ctx.boost);

  FilterContext sub_ctx = ctx;
  sub_ctx.negated = false;
  sub_ctx.boost = irs::kNoBoost;

  auto r =
    FromComparison<true>(group, sub_ctx, between_input, between_lower, lower);
  if (!r.ok()) {
    return r;
  }
  return FromComparison<true>(group, sub_ctx, between_input, between_upper,
                              upper);
}

template<bool GenericVersion>
Result FromIn(irs::BooleanFilter& filter, const FilterContext& ctx,
              const duckdb::BoundOperatorExpression& op_expr) {
  SDB_ASSERT(op_expr.children.size() >= 2);

  const auto* column_info = FindColumnInfoForExpr(ctx, *op_expr.children[0]);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER,
            "IN input is not a reference to an indexed column"};
  }

  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by keyword analyzer. Use `col @@ "
              "ts_any('a', 'b', ...)` (tokenised) or `col @@ ts_any("
              "'a'::tokenize('keyword'), ...)` (raw)."};
    }
  }

  // Collect constant values from children[1..]
  std::vector<const duckdb::Value*> values;
  values.reserve(op_expr.children.size() - 1);
  for (size_t i = 1; i < op_expr.children.size(); ++i) {
    const auto* val = TryGetConstant(*op_expr.children[i]);
    if (!val) {
      return {ERROR_BAD_PARAMETER, "Failed to evaluate IN value as constant"};
    }
    if (!val->IsNull()) {
      values.push_back(val);
    }
  }

  if (values.empty()) {
    AddFilter<irs::Empty>(filter);
    return {};
  }

  auto type_id = column_info->logical_type.id();
  if (auto r = ValidateFilterType(type_id); !r.ok()) {
    return r;
  }

  auto& terms_filter = ctx.negated ? Negate<irs::ByTerms>(filter)
                                   : AddFilter<irs::ByTerms>(filter);
  terms_filter.boost(ctx.boost);
  *terms_filter.mutable_field_id() = PickPerKindFieldId(*column_info, type_id);
  auto& opts = *terms_filter.mutable_options();

  for (const auto* value : values) {
    if (type_id == duckdb::LogicalTypeId::VARCHAR) {
      opts.terms.emplace(AsRawBytes(*value));
    } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
      opts.terms.emplace(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(value->GetValue<bool>())));
    } else if (IsNumericTypeId(type_id)) {
      irs::NumericTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      ResetNumericStream(stream, type_id, *value);
      stream.next();
      opts.terms.emplace(token->value);
    } else {
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported type for IN filter: ", static_cast<int>(type_id)};
    }
  }
  return {};
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> MakeTSQueryCall(
  std::string_view ts_name, duckdb::LogicalType return_type,
  std::vector<duckdb::unique_ptr<duckdb::Expression>> children) {
  duckdb::ScalarFunction fn(std::string{ts_name}, {}, return_type, nullptr);
  duckdb::BoundScalarFunction bound_fn(fn);
  bound_fn.SetName(std::string{ts_name});
  return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    std::move(bound_fn), std::move(children), nullptr);
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> MakeTSQueryCall(
  std::string_view ts_name,
  std::vector<duckdb::unique_ptr<duckdb::Expression>> children) {
  return MakeTSQueryCall(ts_name, MakeTSQueryType(), std::move(children));
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> MakeTSQueryTokenizeList(
  duckdb::unique_ptr<duckdb::Expression> list) {
  return MakeTSQueryCall(kTSQTokenize,
                         duckdb::LogicalType::LIST(MakeTSQueryType()),
                         MakeChildren(std::move(list)));
}

template<const std::string_view& Name>
duckdb::unique_ptr<duckdb::Expression> BuildPassthrough(
  std::vector<duckdb::unique_ptr<duckdb::Expression>>&& args) {
  return MakeTSQueryCall(Name, std::move(args));
}

duckdb::unique_ptr<duckdb::Expression> BuildAllTokens(
  std::vector<duckdb::unique_ptr<duckdb::Expression>>&& args) {
  return MakeTSQueryCall(
    kTSQAllOf, MakeChildren(MakeTSQueryTokenizeList(std::move(args.at(0)))));
}

duckdb::unique_ptr<duckdb::Expression> WrapTextAsConstantList(
  duckdb::unique_ptr<duckdb::Expression> text) {
  const auto* val = TryGetConstant(*text);
  if (!val || val->IsNull()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG(kHasAnyTokens,
              "(col, text, min_match) requires a constant non-NULL text"),
      ERR_HINT("Pass a literal string or a list-form: ", kHasAnyTokens,
               "(col, ['text'], n)."));
  }
  duckdb::vector<duckdb::Value> elems;
  elems.emplace_back(*val);
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::LIST(duckdb::LogicalType::VARCHAR, std::move(elems)));
}

duckdb::unique_ptr<duckdb::Expression> BuildAnyToken(
  std::vector<duckdb::unique_ptr<duckdb::Expression>>&& args) {
  const bool is_text =
    args.at(0)->GetReturnType().id() == duckdb::LogicalTypeId::VARCHAR;
  const bool has_min_match = args.size() >= 2;

  if (!is_text) {
    auto tokenize = MakeTSQueryTokenizeList(std::move(args[0]));
    return MakeTSQueryCall(
      kTSQAnyOf, has_min_match
                   ? MakeChildren(std::move(tokenize), std::move(args[1]))
                   : MakeChildren(std::move(tokenize)));
  }
  if (!has_min_match) {
    return std::move(args[0]);
  }
  auto list = WrapTextAsConstantList(std::move(args[0]));
  auto tokenize = MakeTSQueryTokenizeList(std::move(list));
  return MakeTSQueryCall(kTSQAnyOf,
                         MakeChildren(std::move(tokenize), std::move(args[1])));
}

using PredicateInnerBuilder = duckdb::unique_ptr<duckdb::Expression> (*)(
  std::vector<duckdb::unique_ptr<duckdb::Expression>>&& args);

const containers::FlatHashMap<std::string_view, PredicateInnerBuilder>
  kSugarBuilders = {
    {kPhraseMatches, BuildPassthrough<kTSQPhrase>},
    {kNgramMatches, BuildPassthrough<kTSQNgram>},
    {kLevenshteinMatches, BuildPassthrough<kTSQLevenshtein>},
    {kHasAllTokens, BuildAllTokens},
    {kHasAnyTokens, BuildAnyToken},
};

Result FromPredicate(irs::BooleanFilter& filter, const FilterContext& ctx,
                     PredicateInnerBuilder build_inner,
                     const duckdb::BoundFunctionExpression& func) {
  SDB_ASSERT(!func.children.empty());

  auto tail = func.children | std::views::drop(1) |
              std::views::transform([](const auto& e) { return e->Copy(); }) |
              std::ranges::to<std::vector>();
  auto inner = build_inner(std::move(tail));
  FromTSQueryMatch(filter, ctx, *func.children[0], *inner);
  return {};
}

using StringBuiltinBuilder =
  duckdb::unique_ptr<duckdb::BoundFunctionExpression> (*)(
    std::string_view literal);

duckdb::unique_ptr<duckdb::BoundFunctionExpression> BuildTSStartsWith(
  std::string_view literal) {
  return MakeTSQueryCall(
    kTSQPrefix, MakeChildren(duckdb::make_uniq<duckdb::BoundConstantExpression>(
                  duckdb::Value(std::string{literal}))));
}

void AppendEscapedLikePattern(std::string_view s, std::string& out) {
  for (char c : s) {
    if (c == '\\' || c == '%' || c == '_') {
      out.push_back('\\');
    }
    out.push_back(c);
  }
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> BuildTSContainsLike(
  std::string_view literal) {
  std::string pattern;
  pattern.reserve(literal.size() * 2 + 2);
  pattern.push_back('%');
  AppendEscapedLikePattern(literal, pattern);
  pattern.push_back('%');
  return MakeTSQueryCall(
    kTSQLike, MakeChildren(duckdb::make_uniq<duckdb::BoundConstantExpression>(
                duckdb::Value(std::move(pattern)))));
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> BuildTSEndsWithLike(
  std::string_view literal) {
  std::string pattern;
  pattern.reserve(literal.size() * 2 + 1);
  pattern.push_back('%');
  AppendEscapedLikePattern(literal, pattern);
  return MakeTSQueryCall(
    kTSQLike, MakeChildren(duckdb::make_uniq<duckdb::BoundConstantExpression>(
                duckdb::Value(std::move(pattern)))));
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> BuildTSRegexp(
  std::string_view literal) {
  return MakeTSQueryCall(
    kTSQRegexp, MakeChildren(duckdb::make_uniq<duckdb::BoundConstantExpression>(
                  duckdb::Value(std::string{literal}))));
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> BuildTSLike(
  std::string_view literal) {
  return MakeTSQueryCall(
    kTSQLike, MakeChildren(duckdb::make_uniq<duckdb::BoundConstantExpression>(
                duckdb::Value(std::string{literal}))));
}

using AnalyzerPredicate = bool (*)(irs::TypeInfo::type_id);

bool IsKeywordAnalyzer(irs::TypeInfo::type_id t) {
  return t == irs::Type<irs::StringTokenizer>::id();
}

bool IsLikeCompatibleAnalyzer(irs::TypeInfo::type_id t) {
  return t == irs::Type<irs::StringTokenizer>::id() ||
         t == irs::Type<irs::analysis::WildcardAnalyzer>::id();
}

const containers::FlatHashMap<std::string_view, StringBuiltinBuilder>
  kBuiltinBuilder = {
    {"contains", &BuildTSContainsLike},
    {"^@", &BuildTSStartsWith},
    {"starts_with", &BuildTSStartsWith},
    {"prefix", &BuildTSStartsWith},
    {"suffix", &BuildTSEndsWithLike},
    {"ends_with", &BuildTSEndsWithLike},
    {"regexp_matches", &BuildTSRegexp},
    {"regexp_like", &BuildTSRegexp},
    {"~~", &BuildTSLike},
};

Result FromFunctionExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundFunctionExpression& func) {
  std::string_view name = func.function.GetName();
  std::span args = func.children;

  if (name == kTSQueryMatch) {
    // Anything that fails inside `@@` would otherwise fall through to
    // the runtime stub and surface the generic "TSQUERY expression
    // evaluated outside @@" error -- losing the specific cause. Throw
    // at this boundary so users see the actual reason + a hint.
    SDB_ASSERT(args.size() == 2);
    FromTSQueryMatch(filter, ctx, *args[0], *args[1]);
    return {};
  }
  if (auto geo = TryDispatchGeoFunction(filter, ctx, func)) {
    return std::move(*geo);
  }

  char escape_char = '\\';
  if (name == "like_escape") {
    SDB_ASSERT(args.size() == 3);
    std::string escape_str;
    if (auto r = GetVarcharArg(*args[2], "LIKE ESCAPE", escape_str); !r.ok()) {
      return r;
    }
    if (escape_str.size() != 1) {
      return {ERROR_BAD_PARAMETER, "LIKE ESCAPE must be a single character"};
    }
    escape_char = escape_str.front();
    args = args.subspan(0, 2);
    name = "~~";
  }

  if (args.size() == 2) {
    auto builtin = kBuiltinBuilder.find(name);
    if (auto builder =
          builtin != kBuiltinBuilder.end() ? builtin->second : nullptr) {
      SDB_ASSERT(args.size() == 2);
      if (args[0]->GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_NOT_IMPLEMENTED, func.function.GetName(),
                ": VARCHAR overload only -- declined for ",
                args[0]->GetReturnType().ToString()};
      }
      std::string pattern;
      if (auto r = GetVarcharArg(*args[1], name, pattern); !r.ok()) {
        return r;
      }
      auto validator = &IsKeywordAnalyzer;

      if (builder == &BuildTSLike) {
        pattern = LikeEscapePattern(pattern, escape_char);
        validator = &IsLikeCompatibleAnalyzer;
      }

      const auto* column_info = FindColumnInfoForExpr(ctx, *args[0]);
      if (!column_info) {
        return {ERROR_NOT_IMPLEMENTED, name,
                ": first arg is not a reference to an indexed column"};
      }
      if (!validator(column_info->tokenizer.analyzer->type())) {
        return {ERROR_BAD_PARAMETER, name, ": column analyzer not supported"};
      }
      auto inner = builder(pattern);
      FromTSQueryMatch(filter, ctx, *args[0], *inner);
      return {};
    }
  }

  if (auto sugar = kSugarBuilders.find(name); sugar != kSugarBuilders.end()) {
    return FromPredicate(filter, ctx, sugar->second, func);
  }

  return {ERROR_NOT_IMPLEMENTED, "Unsupported function: ", name};
}

void FromTSQueryConjunction(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func,
                            bool is_and) {
  SDB_ASSERT(func.children.size() == 2);
  irs::BooleanFilter* group;
  if (is_and) {
    group =
      ctx.negated ? &Negate<irs::And>(parent) : &AddFilter<irs::And>(parent);
  } else {
    group =
      ctx.negated ? &Negate<irs::Or>(parent) : &AddFilter<irs::Or>(parent);
  }
  group->boost(ctx.boost);
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  sub_ctx.negated = false;
  for (const auto& child : func.children) {
    BuildTSQuery(*group, sub_ctx, column_info, *child);
  }
}

// TSQUERY `!!` -- prefix NOT. Flips ctx.negated and recurses; no new
// filter node is added at this level (the inner expression's emitter
// will wrap itself in irs::Not when ctx.negated is true).
void FromTSQueryNot(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func) {
  SDB_ASSERT(func.children.size() == 1);
  auto neg = ctx;
  neg.negated = !ctx.negated;
  BuildTSQuery(parent, neg, column_info, *func.children[0]);
}

// TSQUERY `^` -- boost. Multiplies the inherited ctx.boost by the
// factor and recurses into the inner expression.
void FromTSQueryBoost(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_phrase('text') ^ 2.0. Factor must be >= 0; "
    "for composable boost use ::boost(K).";
  SDB_ASSERT(func.children.size() == 2);
  double factor_d;
  if (auto r = GetDoubleArg(*func.children[1], "boost factor", factor_d);
      !r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(r.errorMessage()), ERR_HINT(kSyntaxHint));
  }
  const auto factor = static_cast<irs::score_t>(factor_d);
  if (factor < 0.0f) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("boost factor must be >= 0, got ", factor),
                    ERR_HINT(kSyntaxHint));
  }
  BuildTSQuery(parent, ctx.WithBoost(factor), column_info, *func.children[0]);
}

// `(...)::boost(K)` -- multiplies ctx.boost by the modifier's factor
// and recurses on the inner. Returns false if `peeled` carries no
// boost modifier; true if it claimed and dispatched the cast (any
// dispatch failure throws via the inner BuildTSQuery / BuildFts*).
bool TryDispatchBoostCast(irs::BooleanFilter& parent, const FilterContext& ctx,
                          const SearchColumnInfo& column_info,
                          const duckdb::Expression& peeled) {
  if (peeled.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
    const auto boost = TryGetBoostModifier(cast_expr.GetReturnType());
    if (!boost || !cast_expr.child) {
      return false;
    }
    BuildTSQuery(parent, ctx.WithBoost(static_cast<irs::score_t>(*boost)),
                 column_info, *cast_expr.child);
    return true;
  }
  if (peeled.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& cv = peeled.Cast<duckdb::BoundConstantExpression>().value;
    const auto boost = TryGetBoostModifier(cv.type());
    if (!boost) {
      return false;
    }
    // Strip the BOOSTED alias before recursing, otherwise we re-enter
    // this branch on the same value.
    duckdb::Value cleaned = cv;
    cleaned.Reinterpret(MakeTSQueryType());
    duckdb::BoundConstantExpression cleaned_expr(std::move(cleaned));
    BuildTSQuery(parent, ctx.WithBoost(static_cast<irs::score_t>(*boost)),
                 column_info, cleaned_expr);
    return true;
  }
  return false;
}

bool TryDispatchSqlBoostCast(irs::BooleanFilter& filter,
                             const FilterContext& ctx,
                             const duckdb::Expression& peeled) {
  if (peeled.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return false;
  }
  const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
  const auto boost = TryGetBoostModifier(cast_expr.GetReturnType());
  if (!boost || !cast_expr.child) {
    return false;
  }
  auto r = FromExpression(
    filter, ctx.WithBoost(static_cast<irs::score_t>(*boost)), *cast_expr.child);
  if (!r.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("::boost(K) used on a predicate the inverted index could not "
              "claim: ",
              r.errorMessage()),
      ERR_HINT("boost is only meaningful inside an inverted-index match. "
               "Move the boost into an `@@` match or remove it."));
  }
  return true;
}

// `(...)::tokenize('<name>')` -- 'keyword' bypasses tokenisation;
// any other name resolves via the catalog. Returns false if `peeled`
// carries no tokenize modifier.
bool TryDispatchTokenizeCast(irs::BooleanFilter& parent,
                             const FilterContext& ctx,
                             const SearchColumnInfo& column_info,
                             const duckdb::Expression& peeled) {
  std::string_view tokenizer;
  const duckdb::Expression* expr = nullptr;
  const duckdb::Value* val = nullptr;
  if (peeled.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
    tokenizer = TryGetTokenizerModifier(cast_expr.GetReturnType());
    if (!tokenizer.empty() && cast_expr.child) {
      expr = cast_expr.child.get();
      val = TryGetConstant(UnwrapTSQueryCast(*expr));
    }
  } else if (peeled.GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& cv = peeled.Cast<duckdb::BoundConstantExpression>().value;
    tokenizer = TryGetTokenizerModifier(cv.type());
    if (!tokenizer.empty()) {
      val = &cv;
    }
  }
  if (tokenizer.empty()) {
    return false;
  }
  if (tokenizer == irs::StringTokenizer::type_name()) {
    if (val && !val->IsNull() &&
        val->type().id() == duckdb::LogicalTypeId::VARCHAR) {
      BuildFtsTerm(parent, ctx, column_info, *val);
      return true;
    }
    if (expr) {
      BuildTSQuery(parent, ctx.WithTokenizer(ctx.identity), column_info, *expr);
      return true;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("::tokenize('keyword'): inner expression has unsupported "
              "shape"));
  }
  auto wrapper = AcquireTokenizer(ctx.client_context, tokenizer);
  if (!wrapper) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("::tokenize('", tokenizer, "'): tokenizer not found in catalog"),
      ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
               "or use 'keyword' for raw bytes."));
  }
  auto sub_ctx = ctx.WithTokenizer(*wrapper);
  if (val) {
    // Don't recurse on a folded constant: its type still carries the
    // modifier and would re-enter this branch.
    if (val->IsNull() || (val->type().id() != duckdb::LogicalTypeId::VARCHAR &&
                          val->type().id() != duckdb::LogicalTypeId::BLOB)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("::tokenize(<name>): inner value must be "
                              "VARCHAR or BLOB"));
    }
    BuildFtsTokens(parent, sub_ctx, column_info, duckdb::StringValue::Get(*val),
                   /*require_all=*/false);
    return true;
  }
  BuildTSQuery(parent, sub_ctx, column_info, *expr);
  return true;
}

// `@@` is commutative -- either side may be the column.
void FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& lhs,
                      const duckdb::Expression& rhs) {
  // `@@` accepts either a bare column reference or a JSON-path expression
  // (e.g. `content->>'host'`) on the field side. FindColumnInfoForExpr
  // handles both, peeling any cast wrappers; the TSQuery cast is peeled
  // up-front by UnwrapTSQueryCast.
  const auto* left_info = FindColumnInfoForExpr(ctx, UnwrapTSQueryCast(lhs));
  const auto* right_info = FindColumnInfoForExpr(ctx, UnwrapTSQueryCast(rhs));
  if (left_info && right_info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("@@ has column references on both sides"),
      ERR_HINT("Wrap one side in 'word'::TSQUERY or a constructor "
               "(ts_phrase, ts_like, ...)."));
  }
  const auto* column_info = left_info ? left_info : right_info;
  if (!column_info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("@@ requires an inverted-indexed column on one side"),
      ERR_HINT("Use: <indexed_col> @@ <tsquery_expr>. CREATE INDEX ... "
               "USING inverted(<col>) if missing."));
  }
  const auto& expr = left_info ? rhs : lhs;
  auto* tokenizer = column_info->tokenizer.analyzer.get();
  if (!tokenizer) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("@@ column has no analyzer (not a text-indexed column)"),
      ERR_HINT("Reindex the VARCHAR column with a text-search analyzer."));
  }
  auto sub_ctx = ctx.WithTokenizer(*tokenizer);
  BuildTSQuery(filter, sub_ctx, *column_info, expr);
}

Result FromComparisonExpression(irs::BooleanFilter& filter,
                                const FilterContext& ctx,
                                const duckdb::BoundFunctionExpression& cmp) {
  const auto& left = duckdb::BoundComparisonExpression::Left(cmp);
  const auto& right = duckdb::BoundComparisonExpression::Right(cmp);
  const auto cmp_type = cmp.GetExpressionType();
  switch (cmp_type) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      return FromBinaryEq<true>(filter, ctx, left, right, false);
    case duckdb::ExpressionType::COMPARE_NOTEQUAL:
      return FromBinaryEq<true>(filter, ctx, left, right, true);
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
      auto op = GetComparisonOp(cmp_type);
      return FromComparison<true>(filter, ctx, left, right, op);
    }
    default:
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported comparison type: ", static_cast<int>(cmp_type)};
  }
}

Result FromOperatorExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundOperatorExpression& op_expr) {
  const auto op_type = op_expr.GetExpressionType();
  switch (op_type) {
    case duckdb::ExpressionType::OPERATOR_NOT: {
      SDB_ASSERT(op_expr.children.size() == 1);
      auto negated_ctx = ctx;
      negated_ctx.negated = !ctx.negated;
      return FromExpression(filter, negated_ctx, *op_expr.children[0]);
    }
    case duckdb::ExpressionType::OPERATOR_IS_NULL:
      return FromIsNull(filter, ctx, op_expr);
    case duckdb::ExpressionType::OPERATOR_IS_NOT_NULL: {
      FilterContext sub_ctx = ctx;
      sub_ctx.negated = !ctx.negated;
      return FromIsNull(filter, sub_ctx, op_expr);
    }
    case duckdb::ExpressionType::COMPARE_IN:
      return FromIn<true>(filter, ctx, op_expr);
    case duckdb::ExpressionType::COMPARE_NOT_IN: {
      FilterContext sub_ctx = ctx;
      sub_ctx.negated = !ctx.negated;
      return FromIn<true>(filter, sub_ctx, op_expr);
    }
    default:
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported operator type: ", static_cast<int>(op_type)};
  }
}

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr) {
  // Peel the BOOSTED_TSQUERY -> BOOLEAN coercion that the WHERE-binder
  // inserts around `(predicate)::boost(K)` at the predicate root.
  if (TryDispatchSqlBoostCast(filter, ctx, UnwrapBoostBoolCoercion(expr))) {
    return {};
  }
  if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
    return FromComparisonExpression(
      filter, ctx, expr.Cast<duckdb::BoundFunctionExpression>());
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_BETWEEN) {
    return FromBetween(filter, ctx,
                       expr.Cast<duckdb::BoundFunctionExpression>());
  }
  switch (expr.GetExpressionClass()) {
    case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
      const auto& conj = expr.Cast<duckdb::BoundConjunctionExpression>();
      const auto conj_type = conj.GetExpressionType();
      if (conj_type == duckdb::ExpressionType::CONJUNCTION_AND) {
        return MakeGroup<irs::And>(filter, ctx, conj);
      }
      if (conj_type == duckdb::ExpressionType::CONJUNCTION_OR) {
        return MakeGroup<irs::Or>(filter, ctx, conj);
      }
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported conjunction type: ", static_cast<int>(conj_type)};
    }
    case duckdb::ExpressionClass::BOUND_OPERATOR:
      return FromOperatorExpression(
        filter, ctx, expr.Cast<duckdb::BoundOperatorExpression>());
    case duckdb::ExpressionClass::BOUND_FUNCTION:
      return FromFunctionExpression(
        filter, ctx, expr.Cast<duckdb::BoundFunctionExpression>());
    default:
      return {ERROR_NOT_IMPLEMENTED, "Unsupported expression class: ",
              static_cast<int>(expr.GetExpressionClass())};
  }
}

}  // namespace

const duckdb::Value* TryGetConstant(const duckdb::Expression& expr) {
  // Peel cast wrappers: the standalone ts_offsets/ts_highlight forms
  // run the filter builder mid-bind, before the optimizer folds
  // redundant casts the binder may have inserted around literals.
  const auto* cur = &expr;
  while (cur->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = cur->Cast<duckdb::BoundCastExpression>();
    if (!cast.child) {
      return nullptr;
    }
    cur = cast.child.get();
  }
  if (cur->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &cur->Cast<duckdb::BoundConstantExpression>().value;
}

const SearchColumnInfo* FindColumnRefInfo(
  const FilterContext& ctx, const duckdb::BoundColumnRefExpression& ref) {
  auto cache_it = ctx.column_cache.find(ref.binding);
  if (cache_it != ctx.column_cache.end()) {
    SDB_ASSERT(cache_it->second.logical_type.id() !=
                 duckdb::LogicalTypeId::VARCHAR ||
               cache_it->second.tokenizer.analyzer);
    return &cache_it->second;
  }
  auto info = ctx.column_getter(ref);
  if (!info) {
    return nullptr;
  }
  if (info->logical_type.id() == duckdb::LogicalTypeId::LIST) {
    info->logical_type = duckdb::ListType::GetChildType(info->logical_type);
  } else if (info->logical_type.id() == duckdb::LogicalTypeId::ARRAY) {
    info->logical_type = duckdb::ArrayType::GetChildType(info->logical_type);
  }
  return &ctx.column_cache.emplace(ref.binding, std::move(info.value()))
            .first->second;
}

const duckdb::BoundColumnRefExpression* TryGetColumnRef(
  const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundColumnRefExpression>();
}

bool IsNumericTypeId(duckdb::LogicalTypeId id) {
  return catalog::term_dict::IsNumeric(catalog::term_dict::Classify(id));
}

const duckdb::BoundColumnRefExpression* TryGetJsonColumnRef(
  const duckdb::Expression& expr, std::vector<std::string>& out_path) {
  out_path.clear();
  // Reject when outermost extraction does not return string.
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION ||
      !IsJsonExtractString(
        expr.Cast<duckdb::BoundFunctionExpression>().function.GetName())) {
    return nullptr;
  }

  // Walk the chain: every node must be some JSON-extract
  // until we reach a column ref.
  const duckdb::Expression* cur = &expr;
  while (cur->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& f = cur->Cast<duckdb::BoundFunctionExpression>();
    // TODO(mkornaukhov) first must be extracting string,
    // all the others should be extracing json
    if (!IsJsonExtract(f.function.GetName()) || f.children.size() != 2) {
      return nullptr;
    }
    const auto* key_val = TryGetConstant(*f.children[1]);
    if (!key_val || key_val->IsNull() ||
        !AppendJsonPathKey(*key_val, out_path)) {
      return nullptr;
    }
    cur = f.children[0].get();
  }
  absl::c_reverse(out_path);
  return TryGetColumnRef(*cur);
}

struct UnwrappedField {
  const duckdb::Expression* expr;
  std::optional<duckdb::LogicalType> override_type;
};

UnwrappedField UnwrapFieldCast(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return {&expr, std::nullopt};
  }
  const auto& c = expr.Cast<duckdb::BoundCastExpression>();
  if (!c.child) {
    return {&expr, std::nullopt};
  }
  return {c.child.get(), c.GetReturnType()};
}

const SearchColumnInfo* FindColumnInfoForExpr(const FilterContext& ctx,
                                              const duckdb::Expression& expr) {
  if (const auto* col_ref = TryGetColumnRef(expr)) {
    return FindColumnRefInfo(ctx, *col_ref);
  }

  const auto unwrapped = UnwrapFieldCast(expr);
  std::optional<SearchColumnInfo> info;
  bool matched_unwrapped = false;
  if (ctx.expr_getter) {
    info = (*ctx.expr_getter)(*unwrapped.expr);
    if (info) {
      matched_unwrapped = true;
    } else {
      info = (*ctx.expr_getter)(expr);
    }
  }
  if (!info) {
    return nullptr;
  }

  if (info->logical_type.id() == duckdb::LogicalTypeId::LIST) {
    info->logical_type = duckdb::ListType::GetChildType(info->logical_type);
  } else if (info->logical_type.id() == duckdb::LogicalTypeId::ARRAY) {
    info->logical_type = duckdb::ArrayType::GetChildType(info->logical_type);
  }

  if (matched_unwrapped && unwrapped.override_type) {
    if (IsNumericTypeId(unwrapped.override_type->id())) {
      info->logical_type = duckdb::LogicalType::DOUBLE;
    } else {
      info->logical_type = *unwrapped.override_type;
    }
  }

  if (auto r = ValidateFilterType(info->logical_type.id()); !r.ok()) {
    return nullptr;
  }
  info->field_id = PickPerKindFieldId(*info, info->logical_type.id());
  auto it = ctx.expr_cache.find(info->field_id);
  if (it != ctx.expr_cache.end()) {
    return &it->second;
  }
  const auto field_id = info->field_id;
  return &ctx.expr_cache.emplace(field_id, std::move(info.value()))
            .first->second;
}

Result ValidateFilterType(duckdb::LogicalTypeId type_id) {
  if (!catalog::term_dict::IsSupported(catalog::term_dict::Classify(type_id))) {
    return {ERROR_NOT_IMPLEMENTED, "Unsupported type id ",
            static_cast<int>(type_id), " for filter"};
  }
  return {};
}

void ResetNumericStream(irs::NumericTokenizer& stream,
                        duckdb::LogicalTypeId type_id,
                        const duckdb::Value& value) {
  switch (catalog::term_dict::Classify(type_id)) {
    case catalog::term_dict::Kind::NumericI32:
      stream.reset(value.GetValue<int32_t>());
      break;
    case catalog::term_dict::Kind::NumericI64:
      stream.reset(value.GetValue<int64_t>());
      break;
    case catalog::term_dict::Kind::NumericF32:
      stream.reset(value.GetValue<float>());
      break;
    case catalog::term_dict::Kind::NumericF64:
      stream.reset(value.GetValue<double>());
      break;
    default:
      SDB_ASSERT(false, "ResetNumericStream called with non-numeric type");
  }
}

bool IsRangeNumericValueType(duckdb::LogicalTypeId id) {
  return catalog::term_dict::IsNumeric(catalog::term_dict::Classify(id)) ||
         id == duckdb::LogicalTypeId::DECIMAL;
}

const duckdb::Expression& UnwrapTSQueryCast(const duckdb::Expression& expr) {
  const duckdb::Expression* cur = &expr;
  while (cur->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = cur->Cast<duckdb::BoundCastExpression>();
    if (!cast.child) {
      break;
    }
    const auto& target = cast.GetReturnType();
    const auto& source = cast.child->GetReturnType();
    // Modifier-bearing casts must be preserved so the walker sees them.
    if (!TryGetTokenizerModifier(target).empty() ||
        TryGetBoostModifier(target)) {
      break;
    }
    // Peel transit casts within {VARCHAR, TSQUERY, TOKENIZED_TSQUERY}
    // only -- a plain VARCHAR->VARCHAR cast must stay.
    if (target.id() != duckdb::LogicalTypeId::VARCHAR ||
        source.id() != duckdb::LogicalTypeId::VARCHAR) {
      break;
    }
    if (!IsAnyTSQueryType(target) && !IsAnyTSQueryType(source)) {
      break;
    }
    cur = cast.child.get();
  }
  return *cur;
}

// After cast-peel the peeled Value's type may not match the target
// (e.g. binder rewrites BOOLEAN literal as `Cast(BOOLEAN, Const(VARCHAR
// "t"))` in standalone bind paths). Coerce via DefaultTryCastAs.
template<typename T>
bool TryCoerce(const duckdb::Value& val, duckdb::LogicalTypeId target_id,
               T& out) {
  if (val.type().id() == target_id) {
    out = val.GetValue<T>();
    return true;
  }
  duckdb::Value coerced;
  std::string err;
  if (!val.DefaultTryCastAs(duckdb::LogicalType{target_id}, coerced, &err)) {
    return false;
  }
  out = coerced.GetValue<T>();
  return true;
}

Result GetVarcharArg(const duckdb::Expression& expr, std::string_view label,
                     std::string& out) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null VARCHAR constant"};
  }
  if (val->type().id() == duckdb::LogicalTypeId::BLOB) {
    out = duckdb::StringValue::Get(*val);
    return {};
  }
  if (!TryCoerce(*val, duckdb::LogicalTypeId::VARCHAR, out)) {
    return {ERROR_BAD_PARAMETER, label, " must be a VARCHAR constant"};
  }
  return {};
}

Result GetIntArg(const duckdb::Expression& expr, std::string_view label,
                 int64_t& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null INTEGER constant"};
  }
  switch (val->type().id()) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
      out = val->GetValue<int64_t>();
      return {};
    default:
      if (!TryCoerce(*val, duckdb::LogicalTypeId::BIGINT, out)) {
        return {ERROR_BAD_PARAMETER, label, " must be an INTEGER constant"};
      }
      return {};
  }
}

Result GetBoolArg(const duckdb::Expression& expr, std::string_view label,
                  bool& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null BOOLEAN constant"};
  }
  if (!TryCoerce(*val, duckdb::LogicalTypeId::BOOLEAN, out)) {
    return {ERROR_BAD_PARAMETER, label, " must be a BOOLEAN constant"};
  }
  return {};
}

Result GetDoubleArg(const duckdb::Expression& expr, std::string_view label,
                    double& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null numeric constant"};
  }
  switch (val->type().id()) {
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DECIMAL:
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
      out = val->GetValue<double>();
      return {};
    default:
      if (!TryCoerce(*val, duckdb::LogicalTypeId::DOUBLE, out)) {
        return {ERROR_BAD_PARAMETER, label, " must be a numeric constant"};
      }
      return {};
  }
}

// All From* entry points throw THROW_SQL_ERROR on failure.
void FromPhrase(irs::BooleanFilter&, const FilterContext&,
                const SearchColumnInfo&,
                const duckdb::BoundFunctionExpression&);
void FromNgram(irs::BooleanFilter&, const FilterContext&,
               const SearchColumnInfo&, const duckdb::BoundFunctionExpression&);
void FromLevenshtein(irs::BooleanFilter&, const FilterContext&,
                     const SearchColumnInfo&,
                     const duckdb::BoundFunctionExpression&);
void FromTerm(irs::BooleanFilter&, const FilterContext&,
              const SearchColumnInfo&, const duckdb::BoundFunctionExpression&);
void FromLike(irs::BooleanFilter&, const FilterContext&,
              const SearchColumnInfo&, const duckdb::BoundFunctionExpression&);
void FromPrefix(irs::BooleanFilter&, const FilterContext&,
                const SearchColumnInfo&,
                const duckdb::BoundFunctionExpression&);
void FromTokenize(irs::BooleanFilter&, const FilterContext&,
                  const SearchColumnInfo&,
                  const duckdb::BoundFunctionExpression&);
void FromHalfRange(irs::BooleanFilter&, const FilterContext&,
                   const SearchColumnInfo&,
                   const duckdb::BoundFunctionExpression&,
                   std::string_view label, bool is_lower, bool inclusive);
void FromRegexp(irs::BooleanFilter&, const FilterContext&,
                const SearchColumnInfo&,
                const duckdb::BoundFunctionExpression&);
void FromBetween(irs::BooleanFilter&, const FilterContext&,
                 const SearchColumnInfo&,
                 const duckdb::BoundFunctionExpression&);
void FromCompound(irs::BooleanFilter&, const FilterContext&,
                  const SearchColumnInfo&,
                  const duckdb::BoundFunctionExpression&);
void FromAnyAllOf(irs::BooleanFilter&, const FilterContext&,
                  const SearchColumnInfo&,
                  const duckdb::BoundFunctionExpression&, bool is_any);
void FromPlainToTsquery(irs::BooleanFilter&, const FilterContext&,
                        const SearchColumnInfo&,
                        const duckdb::BoundFunctionExpression&);
void FromTsqueryPhrase(irs::BooleanFilter&, const FilterContext&,
                       const SearchColumnInfo&,
                       const duckdb::BoundFunctionExpression&);
void FromToTsquery(irs::BooleanFilter&, const FilterContext&,
                   const SearchColumnInfo&,
                   const duckdb::BoundFunctionExpression&);
void FromWebsearchToTsquery(irs::BooleanFilter&, const FilterContext&,
                            const SearchColumnInfo&,
                            const duckdb::BoundFunctionExpression&);
void FromTSQueryPhraseSeq(irs::BooleanFilter&, const FilterContext&,
                          const SearchColumnInfo&,
                          const duckdb::BoundFunctionExpression&);

TSQueryOp ClassifyTSQueryFunction(std::string_view name) {
  return magic_enum::enum_cast<TSQueryOp>(name).value_or(TSQueryOp::Unknown);
}

void BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::Expression& expr) {
  const duckdb::Expression& unwrapped = UnwrapTSQueryCast(expr);

  // Trivial-constant short-circuit: NULL -> Empty, true -> All,
  // false -> Empty. Surfaces as either a NULL TSQUERY constant or a
  // BoundCast<TSQUERY> wrapping a BOOLEAN constant. Works at any
  // TSQUERY position thanks to the recursive walker.
  if (unwrapped.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = unwrapped.Cast<duckdb::BoundCastExpression>();
    if (cast.child &&
        cast.child->GetReturnType().id() == duckdb::LogicalTypeId::BOOLEAN) {
      const auto* val = TryGetConstant(*cast.child);
      if (!val) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("BOOLEAN inside TSQUERY must be a constant"),
                        ERR_HINT("Use a literal true / false / NULL."));
      }
      if (val->IsNull() || !val->GetValue<bool>()) {
        AddFilter<irs::Empty>(parent);
      } else {
        AddFilter<irs::All>(parent);
      }
      return;
    }
  }
  if (const auto* val = TryGetConstant(unwrapped); val && val->IsNull()) {
    AddFilter<irs::Empty>(parent);
    return;
  }

  if (TryDispatchBoostCast(parent, ctx, column_info, unwrapped)) {
    return;
  }

  if (TryDispatchTokenizeCast(parent, ctx, column_info, unwrapped)) {
    return;
  }

  // Bare string (promoted via VARCHAR -> TSQUERY cast) -> tokenize via
  // the ambient (column) analyzer. Multi-token input composes with OR
  // (min_match=1) per the plan's "col @@ 'Quick Fox' ≡ ANY_OF(tokens)"
  // rule. Non-VARCHAR / analyzer-less paths fall back to raw ByTerm.
  if (unwrapped.GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val = unwrapped.Cast<duckdb::BoundConstantExpression>().value;
    if (val.IsNull()) {
      AddFilter<irs::Empty>(parent);
      return;
    }
    if (val.type().id() == duckdb::LogicalTypeId::VARCHAR ||
        val.type().id() == duckdb::LogicalTypeId::BLOB) {
      BuildFtsTokens(parent, ctx, column_info, duckdb::StringValue::Get(val),
                     /*require_all=*/false);
      return;
    }
    BuildFtsTerm(parent, ctx, column_info, val);
    return;
  }

  if (unwrapped.GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_FUNCTION) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("Unsupported TSQUERY expression class: ",
                            static_cast<int>(unwrapped.GetExpressionClass())),
                    ERR_HINT("Use a TSQUERY constructor (ts_phrase, ts_like, "
                             "...) or 'literal'::TSQUERY."));
  }

  const auto& func = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  const auto op = ClassifyTSQueryFunction(func.function.GetName());

  switch (op) {
    case TSQueryOp::Phrase:
      return FromPhrase(parent, ctx, column_info, func);
    case TSQueryOp::Term:
      return FromTerm(parent, ctx, column_info, func);
    case TSQueryOp::Like:
      return FromLike(parent, ctx, column_info, func);
    case TSQueryOp::Prefix:
      return FromPrefix(parent, ctx, column_info, func);
    case TSQueryOp::Ngram:
      return FromNgram(parent, ctx, column_info, func);
    case TSQueryOp::Fuzzy:
      return FromLevenshtein(parent, ctx, column_info, func);
    case TSQueryOp::Or:
      return FromTSQueryConjunction(parent, ctx, column_info, func,
                                    /*is_and=*/false);
    case TSQueryOp::And:
      return FromTSQueryConjunction(parent, ctx, column_info, func,
                                    /*is_and=*/true);
    case TSQueryOp::Not:
      return FromTSQueryNot(parent, ctx, column_info, func);
    case TSQueryOp::Boost:
      return FromTSQueryBoost(parent, ctx, column_info, func);
    case TSQueryOp::PhraseSeq:
      return FromTSQueryPhraseSeq(parent, ctx, column_info, func);
    case TSQueryOp::PhraseToTsquery:
      return FromPhrase(parent, ctx, column_info, func);
    case TSQueryOp::Any:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/true);
    case TSQueryOp::All:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/false);
    case TSQueryOp::Compound:
      return FromCompound(parent, ctx, column_info, func);
    case TSQueryOp::Between:
      return FromBetween(parent, ctx, column_info, func);
    case TSQueryOp::Regexp:
      return FromRegexp(parent, ctx, column_info, func);
    case TSQueryOp::Less:
      return FromHalfRange(parent, ctx, column_info, func, "ts_lt",
                           /*is_lower=*/false, /*inclusive=*/false);
    case TSQueryOp::LessEq:
      return FromHalfRange(parent, ctx, column_info, func, "ts_le",
                           /*is_lower=*/false, /*inclusive=*/true);
    case TSQueryOp::Greater:
      return FromHalfRange(parent, ctx, column_info, func, "ts_gt",
                           /*is_lower=*/true, /*inclusive=*/false);
    case TSQueryOp::GreaterEq:
      return FromHalfRange(parent, ctx, column_info, func, "ts_ge",
                           /*is_lower=*/true, /*inclusive=*/true);
    case TSQueryOp::Tokenize:
      return FromTokenize(parent, ctx, column_info, func);
    case TSQueryOp::PlainToTsquery:
      return FromPlainToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::WebsearchToTsquery:
      return FromWebsearchToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::TsqueryPhrase:
      return FromTsqueryPhrase(parent, ctx, column_info, func);
    case TSQueryOp::ToTSQuery:
      return FromToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::IsNull:
    case TSQueryOp::IsNotNull: {
      // `col @@ ts_is_null()` -> Not(ByColumnExistence(col_id));
      // `col @@ ts_is_not_null()` -> ByColumnExistence(col_id). The cs
      // column id space and the catalog column id space are aligned
      // (see search_pk_lookup.h's cast for the PK).
      const bool exists = (op == TSQueryOp::IsNotNull);
      auto& existence = (ctx.negated ^ exists)
                          ? AddFilter<irs::ByColumnExistence>(parent)
                          : Negate<irs::ByColumnExistence>(parent);
      existence.boost(ctx.boost);
      *existence.mutable_id() = column_info.field_id;
      return;
    }
    case TSQueryOp::Unknown:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Not a TSQUERY-producing function: ", func.function.GetName()),
        ERR_HINT("Use a TSQUERY constructor (ts_phrase, ts_like, ts_between, "
                 "ts_ngram, ts_levenshtein, ts_regexp, ts_any, ts_all, "
                 "ts_compound, ...) or 'literal'::TSQUERY."));
  }
  SDB_UNREACHABLE();
}

Result MakeSearchFilter(
  irs::And& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter, duckdb::ClientContext& context,
  const ExpressionGetter& expr_getter) {
  irs::StringTokenizer identity;
  duckdb::column_binding_map_t<SearchColumnInfo> column_cache;
  containers::NodeHashMap<irs::field_id, SearchColumnInfo> expr_cache;

  size_t scored_terms_limit = 1024;
  duckdb::Value v;
  if (context.TryGetCurrentSetting("sdb_scored_terms_limit", v) &&
      !v.IsNull()) {
    scored_terms_limit = static_cast<size_t>(v.GetValue<int32_t>());
  }

  FilterContext ctx{
    .negated = false,
    .column_getter = column_getter,
    .expr_getter = expr_getter ? &expr_getter : nullptr,
    .column_cache = column_cache,
    .expr_cache = expr_cache,
    .identity = identity,
    .tokenizer = identity,
    .client_context = context,
    .scored_terms_limit = scored_terms_limit,
  };

  for (const auto& expr : conjuncts) {
    SDB_ASSERT(expr);

    auto r = FromExpression(root, ctx, *expr);
    if (!r.ok()) {
      return r;
    }
  }
  return {};
}

}  // namespace sdb::connector
