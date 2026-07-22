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
#include <duckdb/planner/expression_iterator.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/automaton_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
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
#include <iresearch/utils/automaton_utils.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/wildcard_utils.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/node_hash_map.h"
#include "basics/system-compiler.h"
#include "comparison_op.hpp"
#include "connector/common.h"
#include "functions/search.h"
#include "functions/string.h"
#include "functions/ts_common.hpp"
#include "functions/ts_query_codec.h"
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

bool TryEncodeTerm(duckdb::LogicalTypeId type_id, const duckdb::Value& value,
                   irs::bstring& out) {
  if (type_id == duckdb::LogicalTypeId::VARCHAR ||
      type_id == duckdb::LogicalTypeId::BLOB) {
    out.assign(AsRawBytes(value));
    return true;
  }
  if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
    out.assign(
      irs::ViewCast<irs::byte_type>(irs::BooleanTerm(value.GetValue<bool>())));
    return true;
  }
  if (IsNumericTypeId(type_id)) {
    WithNumericValue(type_id, value, [&](auto v) {
      irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
      out.assign(irs::numeric_utils::EncodeNumericTerm(buf, v));
    });
    return true;
  }
  return false;
}

}  // namespace

absl::Status SetupTermFilter(irs::ByTerm& filter,
                             const SearchColumnInfo& column_info,
                             const duckdb::Value& value) {
  SDB_ASSERT(!value.IsNull(),
             "UNKNOWN and Nulls should be handled as part of IS NULL operator. "
             "For regular filter it should be just irs::Empty!");

  auto type_id = column_info.logical_type.id();

  if (!IsFilterableType(type_id)) {
    return absl::UnimplementedError(absl::StrCat(
      "Unsupported type id ", static_cast<int>(type_id), " for filter"));
  }

  if (!TryEncodeTerm(type_id, value, filter.mutable_options()->term)) {
    return absl::UnimplementedError(absl::StrCat(
      "Unsupported type for term filter: ", static_cast<int>(type_id)));
  }

  *filter.mutable_field_id() = PickPerKindFieldId(column_info, type_id);
  return absl::OkStatus();
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

}  // namespace

// Bind does NOT pre-resolve the tokenizer name to a live analyzer
// because the analyzer is stateful (one tokenization stream per use)
// and can't be shared across queries.
std::string_view TryGetTokenizerModifier(const duckdb::LogicalType& type) {
  if (!IsTSQueryStructType(type) || !type.HasExtensionInfo()) {
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
  if (!IsTSQueryStructType(type) || !type.HasExtensionInfo()) {
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

namespace {

bool IsComparisonExpr(const duckdb::Expression& expr) {
  return duckdb::BoundComparisonExpression::IsComparison(expr) &&
         GetComparisonOp(expr.GetExpressionType()) != ComparisonOp::None;
}

absl::Status RequireKeywordAnalyzed(const SearchColumnInfo& info,
                                    std::string_view hint) {
  if (info.logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
      info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
    return absl::InvalidArgumentError(
      absl::StrCat("Field is not indexed by keyword analyzer. ", hint));
  }
  return absl::OkStatus();
}

// Peels the TSQUERY_MODIFIER -> BOOLEAN coercion that the WhereBinder
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
  if (cast.GetReturnType().id() != duckdb::LogicalTypeId::BOOLEAN) {
    return expr;
  }
  if (!TryGetBoostModifier(cast.Child().GetReturnType())) {
    return expr;
  }
  return cast.Child();
}

absl::Status FromExpression(irs::BooleanFilter& filter,
                            const FilterContext& ctx,
                            const duckdb::Expression& expr);
void FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& lhs,
                      const duckdb::Expression& rhs);

void FillNullMarker(irs::ByTerm& term, irs::field_id null_field_id) {
  *term.mutable_field_id() = null_field_id;
  term.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::kNullTerm));
}

}  // namespace

irs::ByTerm& AddNullMarkerTerm(irs::BooleanFilter& parent,
                               irs::field_id null_field_id) {
  auto& term = AddFilter<irs::ByTerm>(parent);
  FillNullMarker(term, null_field_id);
  return term;
}

void AddNegated(irs::BooleanFilter& parent, const SearchColumnInfo& info,
                irs::Filter::ptr target) {
  if (irs::field_limits::valid(info.null_field_id)) {
    auto& group = Negate<irs::Or>(parent);
    group.add(std::move(target));
    AddNullMarkerTerm(group, info.null_field_id);
    return;
  }
  AddNot(parent).mutable_filter() = std::move(target);
}

namespace {

void CollectNullableMarkers(const FilterContext& ctx,
                            const duckdb::Expression& expr,
                            std::vector<irs::field_id>& markers) {
  if (const auto* info = FindColumnInfoForExpr(ctx, expr)) {
    if (irs::field_limits::valid(info->null_field_id) &&
        !absl::c_linear_search(markers, info->null_field_id)) {
      markers.push_back(info->null_field_id);
    }
    return;
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      CollectNullableMarkers(ctx, child, markers);
    });
}

catalog::Tokenizer::TokenizerWrapper ResolveTokenizerOrThrow(
  const FilterContext& ctx, std::string_view name) {
  auto wrapper = AcquireTokenizer(ctx.client_context, name);
  if (!wrapper) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("::tokenize('", name, "'): tokenizer not found in catalog"),
      ERR_HINT("Create it via CREATE TEXT SEARCH DICTIONARY "
               "or use 'keyword' for raw bytes."));
  }
  return wrapper;
}

template<typename Pred>
bool AnyExprNode(const duckdb::Expression& expr, const Pred& pred) {
  if (pred(expr)) {
    return true;
  }
  bool found = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      found = found || AnyExprNode(child, pred);
    });
  return found;
}

bool ContainsNullConstant(const duckdb::Expression& expr) {
  return AnyExprNode(expr, [](const duckdb::Expression& node) {
    return node.GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_CONSTANT &&
           node.Cast<duckdb::BoundConstantExpression>().GetValue().IsNull();
  });
}

}  // namespace

bool IsIndexOnlyPredicateName(std::string_view name) {
  return name == "@@" || name == kPhraseMatches || name == kNgramMatches ||
         name == kLevenshteinMatches || name == kHasAllTokens ||
         name == kHasAnyTokens;
}

bool ContainsIndexOnlyPredicate(const duckdb::Expression& expr) {
  return AnyExprNode(expr, [](const duckdb::Expression& node) {
    return node.GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_FUNCTION &&
           IsIndexOnlyPredicateName(node.Cast<duckdb::BoundFunctionExpression>()
                                      .Function()
                                      .GetName()
                                      .GetIdentifierName());
  });
}

// Term-surface comparison shapes: comparisons plus the scalar pattern
// functions, excluding the index-only match sugar.
bool IsStrictComparisonShape(const duckdb::Expression& expr) {
  switch (expr.GetExpressionType()) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
    case duckdb::ExpressionType::COMPARE_NOTEQUAL:
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
    case duckdb::ExpressionType::COMPARE_BETWEEN:
    case duckdb::ExpressionType::COMPARE_IN:
    case duckdb::ExpressionType::COMPARE_NOT_IN:
      return true;
    default:
      break;
  }
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& name = expr.Cast<duckdb::BoundFunctionExpression>()
                         .Function()
                         .GetName()
                         .GetIdentifierName();
    return name == "~~" || name == "prefix" || name == "starts_with" ||
           name == "^@";
  }
  return false;
}

namespace {

// UNKNOWN exactly on NULL operands; false for shapes whose null behavior
// is not provably strict.
bool IsStrictPredicate(const duckdb::Expression& expr) {
  if (IsStrictComparisonShape(expr)) {
    return true;
  }
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    return IsIndexOnlyPredicateName(expr.Cast<duckdb::BoundFunctionExpression>()
                                      .Function()
                                      .GetName()
                                      .GetIdentifierName());
  }
  return false;
}

template<typename Filter>
absl::Status MakeGroup(irs::BooleanFilter& parent, const FilterContext& ctx,
                       const duckdb::BoundConjunctionExpression& conj) {
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  std::vector<irs::field_id> markers;
  irs::BooleanFilter* group_root;
  if (ctx.negated && absl::c_all_of(conj.GetChildren(), [](const auto& child) {
        SDB_ASSERT(child);
        return IsComparisonExpr(*child);
      })) {
    // De Morgan's law: if we negate a group of comparisons, comparisons
    // consume negation by inversion so we can reduce NOT filters.
    group_root =
      irs::Type<Filter>::id() == irs::Type<irs::And>::id()
        ? static_cast<irs::BooleanFilter*>(&AddFilter<irs::Or>(parent))
        : static_cast<irs::BooleanFilter*>(&AddFilter<irs::And>(parent));
  } else if (ctx.negated) {
    // A negated group claims soundly only as a strict DISJUNCTION over
    // nullable columns: any NULL operand keeps the OR non-false, so SQL
    // rejects the row and the group's exclusion may match the columns'
    // null markers too. A negated conjunction has no such shape (Kleene
    // FALSE AND UNKNOWN is FALSE), so it stays a row filter. A NULL
    // literal inside a child makes it UNKNOWN-capable on non-NULL rows
    // (its positive claim compiles the literal away), so it defeats the
    // claim regardless of column nullability.
    if (absl::c_any_of(conj.GetChildren(), [](const auto& child) {
          return ContainsNullConstant(*child);
        })) {
      return absl::InvalidArgumentError(
        "negated group with NULL literals can be UNKNOWN on non-NULL rows");
    }
    for (const auto& child : conj.GetChildren()) {
      CollectNullableMarkers(ctx, *child, markers);
    }
    if (!markers.empty()) {
      const bool claimable =
        irs::Type<Filter>::id() == irs::Type<irs::Or>::id() &&
        absl::c_all_of(conj.GetChildren(), [](const auto& child) {
          return IsStrictPredicate(*child);
        });
      if (!claimable) {
        if (absl::c_any_of(conj.GetChildren(), [](const auto& child) {
              return ContainsIndexOnlyPredicate(*child);
            })) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("negated group over nullable columns mixes index-only "
                    "search predicates with a shape that must stay a row "
                    "filter"),
            ERR_HINT("Rewrite the negation as a strict disjunction, or add "
                     "`IS NOT NULL` conjuncts on the nullable columns."));
        }
        return absl::InvalidArgumentError(
          "negated group over nullable columns is only index-claimable as a "
          "strict disjunction");
      }
    }
    group_root = &Negate<Filter>(parent);
    sub_ctx.negated = false;
  } else {
    group_root = &AddFilter<Filter>(parent);
    sub_ctx.negated = false;
  }
  group_root->boost(ctx.boost);
  for (const auto& child : conj.GetChildren()) {
    if (auto s = FromExpression(*group_root, sub_ctx, *child); !s.ok()) {
      return s;
    }
  }
  for (const auto marker : markers) {
    AddNullMarkerTerm(*group_root, marker);
  }
  return absl::OkStatus();
}

absl::Status FromIsNull(irs::BooleanFilter& filter, const FilterContext& ctx,
                        const duckdb::BoundOperatorExpression& op_expr) {
  SDB_ASSERT(op_expr.GetChildren().size() == 1);
  const auto* column_info =
    FindColumnInfoForExpr(ctx, *op_expr.GetChildren()[0]);
  if (!column_info) {
    return absl::InvalidArgumentError(
      "IS NULL input is not a reference to an indexed column");
  }
  if (!irs::field_limits::valid(column_info->null_field_id)) {
    return absl::InvalidArgumentError(
      "IS NULL over a column without a null-marker field (NOT NULL or "
      "legacy index)");
  }
  auto& term_filter =
    ctx.negated ? Negate<irs::ByTerm>(filter) : AddFilter<irs::ByTerm>(filter);
  term_filter.boost(ctx.boost);
  FillNullMarker(term_filter, column_info->null_field_id);
  return absl::OkStatus();
}

template<bool GenericVersion>
absl::Status FromBinaryEq(irs::BooleanFilter& filter, const FilterContext& ctx,
                          const duckdb::Expression& left_expr,
                          const duckdb::Expression& right_expr,
                          bool not_equal) {
  // ST_Distance_Centroid(field, centroid) = / != distance  --  rewrite to
  // range.
  if constexpr (GenericVersion) {
    if (const auto* geo_call = TryGetGeoDistanceCall(ctx, left_expr)) {
      FilterContext geo_ctx = ctx;
      geo_ctx.negated = (ctx.negated != not_equal);
      FromGeoDistanceBinaryEq(filter, geo_ctx, *geo_call, right_expr);
      return absl::OkStatus();
    }
  }

  // Use the JSON-path-aware resolver so `(content->>'val')::int = 42` is
  // claimed by the index: the cast is peeled and routed to the numeric-
  // mangled field, so rows whose leaf isn't numeric simply aren't in the
  // posting list (no runtime cast on incompatible rows).
  const auto* column_info = FindColumnInfoForExpr(ctx, left_expr);
  const auto* const_val = TryGetConstant(right_expr);

  if (!column_info || !const_val) {
    return absl::InvalidArgumentError(
      "Expected indexed column reference on the left and constant on "
      "the right");
  }

  if (const_val->IsNull()) {
    // foo == NULL is always false and foo != NULL is false too.
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  if constexpr (GenericVersion) {
    if (auto s =
          RequireKeywordAnalyzed(*column_info,
                                 "Use `col @@ 'value'` (tokenised) or `col @@ "
                                 "'value'::tokenize('keyword')` (raw).");
        !s.ok()) {
      return s;
    }
  }

  auto& term_filter = (ctx.negated != not_equal)
                        ? NegateScoped<irs::ByTerm>(filter, *column_info)
                        : AddFilter<irs::ByTerm>(filter);

  term_filter.boost(ctx.boost);
  return SetupTermFilter(term_filter, *column_info, *const_val);
}

template<bool GenericVersion>
absl::Status FromComparison(irs::BooleanFilter& filter,
                            const FilterContext& ctx,
                            const duckdb::Expression& field_expr,
                            const duckdb::Expression& value_expr,
                            ComparisonOp op) {
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
      FromGeoDistanceComparison(filter, geo_ctx, *geo_call, value_expr, op);
      return absl::OkStatus();
    }
  }

  const auto* column_info = FindColumnInfoForExpr(ctx, field_expr);
  const auto* const_val = TryGetConstant(value_expr);

  if (!column_info || !const_val) {
    return absl::InvalidArgumentError(
      "Expected indexed column reference and constant for comparison");
  }

  if (const_val->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }
  if constexpr (GenericVersion) {
    if (auto s = RequireKeywordAnalyzed(
          *column_info,
          "Range predicates (<, <=, >, >=, BETWEEN) require an "
          "keyword-analyzed column. Use `col @@ ts_lt('value')` / `LESS_EQ` "
          "/ `GREATER` / `GREATER_EQ` / `ts_between(min, max, ...)` "
          "(tokenised through the column's analyzer) instead.");
        !s.ok()) {
      return s;
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

  if (!IsFilterableType(type_id)) {
    return absl::UnimplementedError(absl::StrCat(
      "Unsupported type id ", static_cast<int>(type_id), " for filter"));
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
        irs::BooleanTerm(const_val->GetValue<bool>())));
  } else if (IsNumericTypeId(type_id)) {
    auto& range_filter = AddFilter<irs::ByGranularRange>(filter);
    range_filter.mutable_options()->scored_terms_limit = ctx.scored_terms_limit;
    WithNumericValue(type_id, *const_val, [&](auto v) {
      irs::SetGranularNumericTerm(setup_base_filter(range_filter), v);
    });
  } else {
    return absl::UnimplementedError(absl::StrCat(
      "Unsupported type for range comparison: ", static_cast<int>(type_id)));
  }
  return absl::OkStatus();
}

absl::Status FromBetween(irs::BooleanFilter& filter, const FilterContext& ctx,
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
    return absl::InvalidArgumentError("BETWEEN bounds must be constants");
  }

  const auto lower =
    ctx.negated ? (lower_inclusive ? ComparisonOp::Lt : ComparisonOp::Le)
                : (lower_inclusive ? ComparisonOp::Ge : ComparisonOp::Gt);
  const auto upper =
    ctx.negated ? (upper_inclusive ? ComparisonOp::Gt : ComparisonOp::Ge)
                : (upper_inclusive ? ComparisonOp::Le : ComparisonOp::Lt);

  auto& group = ctx.negated
                  ? static_cast<irs::BooleanFilter&>(AddFilter<irs::Or>(filter))
                  : AddFilter<irs::And>(filter);
  group.boost(ctx.boost);

  FilterContext sub_ctx = ctx;
  sub_ctx.negated = false;
  sub_ctx.boost = irs::kNoBoost;

  if (auto s = FromComparison<true>(group, sub_ctx, between_input,
                                    between_lower, lower);
      !s.ok()) {
    return s;
  }
  return FromComparison<true>(group, sub_ctx, between_input, between_upper,
                              upper);
}

template<bool GenericVersion>
absl::Status FromIn(irs::BooleanFilter& filter, const FilterContext& ctx,
                    const duckdb::BoundOperatorExpression& op_expr) {
  SDB_ASSERT(op_expr.GetChildren().size() >= 2);

  const auto* column_info =
    FindColumnInfoForExpr(ctx, *op_expr.GetChildren()[0]);
  if (!column_info) {
    return absl::InvalidArgumentError(
      "IN input is not a reference to an indexed column");
  }

  if constexpr (GenericVersion) {
    if (auto s = RequireKeywordAnalyzed(
          *column_info,
          "Use `col @@ ts_any('a', 'b', ...)` (tokenised) or `col @@ "
          "ts_any('a'::tokenize('keyword'), ...)` (raw).");
        !s.ok()) {
      return s;
    }
  }

  // Collect constant values from children[1..]. NULL elements never
  // satisfy the IN, but they keep NOT IN from ever being TRUE (the
  // conjunct `x != NULL` is UNKNOWN), so under negation they force the
  // empty result.
  std::vector<const duckdb::Value*> values;
  values.reserve(op_expr.GetChildren().size() - 1);
  bool has_null = false;
  for (size_t i = 1; i < op_expr.GetChildren().size(); ++i) {
    const auto* val = TryGetConstant(*op_expr.GetChildren()[i]);
    if (!val) {
      return absl::InvalidArgumentError(
        "Failed to evaluate IN value as constant");
    }
    if (val->IsNull()) {
      has_null = true;
    } else {
      values.push_back(val);
    }
  }

  if ((ctx.negated && has_null) || values.empty()) {
    AddFilter<irs::Empty>(filter);
    return absl::OkStatus();
  }

  auto type_id = column_info->logical_type.id();
  if (!IsFilterableType(type_id)) {
    return absl::UnimplementedError(absl::StrCat(
      "Unsupported type id ", static_cast<int>(type_id), " for filter"));
  }

  auto& terms_filter = AddMaybeNegated<irs::ByTerms>(filter, ctx, *column_info);
  terms_filter.boost(ctx.boost);
  *terms_filter.mutable_field_id() = PickPerKindFieldId(*column_info, type_id);
  auto& opts = *terms_filter.mutable_options();

  for (const auto* value : values) {
    irs::bstring term;
    if (!TryEncodeTerm(type_id, *value, term)) {
      return absl::UnimplementedError(absl::StrCat(
        "Unsupported type for IN filter: ", static_cast<int>(type_id)));
    }
    opts.terms.emplace(std::move(term));
  }
  return absl::OkStatus();
}

duckdb::unique_ptr<duckdb::BoundFunctionExpression> MakeTSQueryCall(
  std::string_view ts_name, duckdb::LogicalType return_type,
  std::vector<duckdb::unique_ptr<duckdb::Expression>> children) {
  duckdb::ScalarFunction fn(duckdb::Identifier{ts_name}, {}, return_type,
                            nullptr);
  duckdb::BoundScalarFunction bound_fn(fn);
  bound_fn.SetName(duckdb::Identifier{ts_name});
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

absl::Status FromPredicate(irs::BooleanFilter& filter, const FilterContext& ctx,
                           PredicateInnerBuilder build_inner,
                           const duckdb::BoundFunctionExpression& func) {
  SDB_ASSERT(!func.GetChildren().empty());

  auto tail = func.GetChildren() | std::views::drop(1) |
              std::views::transform([](const auto& e) { return e->Copy(); }) |
              std::ranges::to<std::vector>();
  auto inner = build_inner(std::move(tail));
  FromTSQueryMatch(filter, ctx, *func.GetChildren()[0], *inner);
  return absl::OkStatus();
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

absl::Status FromFunctionExpression(
  irs::BooleanFilter& filter, const FilterContext& ctx,
  const duckdb::BoundFunctionExpression& func) {
  std::string_view name = func.Function().GetName().GetIdentifierName();
  std::span args = func.GetChildren();

  if (name == kTSQueryMatch) {
    // Anything that fails inside `@@` would otherwise fall through to
    // the runtime stub and surface the generic "TSQUERY expression
    // evaluated outside @@" error -- losing the specific cause. Throw
    // at this boundary so users see the actual reason + a hint.
    SDB_ASSERT(args.size() == 2);
    FromTSQueryMatch(filter, ctx, *args[0], *args[1]);
    return absl::OkStatus();
  }
  if (TryDispatchGeoFunction(filter, ctx, func)) {
    return absl::OkStatus();
  }

  char escape_char = '\\';
  if (name == "like_escape") {
    SDB_ASSERT(args.size() == 3);
    const auto* escape_val = TryGetConstant(*args[2]);
    if (!escape_val || escape_val->IsNull() ||
        escape_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
      return absl::InvalidArgumentError(
        "LIKE ESCAPE must be a non-null VARCHAR constant");
    }
    const std::string_view escape_str = duckdb::StringValue::Get(*escape_val);
    if (escape_str.size() != 1) {
      return absl::InvalidArgumentError(
        "LIKE ESCAPE must be a single character");
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
        return absl::UnimplementedError(
          absl::StrCat(func.Function().GetName().GetIdentifierName(),
                       ": VARCHAR overload only -- declined for ",
                       args[0]->GetReturnType().ToString()));
      }
      const auto* pattern_val = TryGetConstant(*args[1]);
      if (!pattern_val || pattern_val->IsNull() ||
          pattern_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
        return absl::InvalidArgumentError(
          absl::StrCat(name, " must be a non-null VARCHAR constant"));
      }
      std::string pattern{duckdb::StringValue::Get(*pattern_val)};
      auto validator = &IsKeywordAnalyzer;

      if (builder == &BuildTSLike) {
        pattern = LikeEscapePattern(pattern, escape_char);
        validator = &IsLikeCompatibleAnalyzer;
      }

      const auto* column_info = FindColumnInfoForExpr(ctx, *args[0]);
      if (!column_info) {
        return absl::InvalidArgumentError(absl::StrCat(
          name, ": first arg is not a reference to an indexed column"));
      }
      if (!validator(column_info->tokenizer.analyzer->type())) {
        return absl::InvalidArgumentError(
          absl::StrCat(name, ": column analyzer not supported"));
      }
      auto inner = builder(pattern);
      FromTSQueryMatch(filter, ctx, *args[0], *inner);
      return absl::OkStatus();
    }
  }

  if (auto sugar = kSugarBuilders.find(name); sugar != kSugarBuilders.end()) {
    return FromPredicate(filter, ctx, sugar->second, func);
  }

  return absl::UnimplementedError(absl::StrCat("Unsupported function: ", name));
}

void FromTSQueryConjunction(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func,
                            bool is_and) {
  SDB_ASSERT(func.GetChildren().size() == 2);
  irs::BooleanFilter* group;
  if (is_and) {
    group = &AddMaybeNegated<irs::And>(parent, ctx, column_info);
  } else {
    group = &AddMaybeNegated<irs::Or>(parent, ctx, column_info);
  }
  group->boost(ctx.boost);
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  sub_ctx.negated = false;
  for (const auto& child : func.GetChildren()) {
    BuildTSQuery(*group, sub_ctx, column_info, *child);
  }
}

// TSQUERY `!!` -- prefix NOT. Flips ctx.negated and recurses; no new
// filter node is added at this level (the inner expression's emitter
// will wrap itself in irs::Not when ctx.negated is true).
void FromTSQueryNot(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func) {
  SDB_ASSERT(func.GetChildren().size() == 1);
  auto neg = ctx;
  neg.negated = !ctx.negated;
  BuildTSQuery(parent, neg, column_info, *func.GetChildren()[0]);
}

// TSQUERY `^` -- boost. Multiplies the inherited ctx.boost by the
// factor and recurses into the inner expression.
void FromTSQueryBoost(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      const duckdb::BoundFunctionExpression& func) {
  static constexpr std::string_view kSyntaxHint =
    "Example: ts_phrase('text') ^ 2.0. Factor must be >= 0; "
    "for composable boost use ::boost(K).";
  SDB_ASSERT(func.GetChildren().size() == 2);
  double factor_d;
  GetDoubleArg(*func.GetChildren()[1], factor_d, {"boost factor", kSyntaxHint});
  const auto factor = static_cast<irs::score_t>(factor_d);
  if (factor < 0.0f) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("boost factor must be >= 0, got ", factor),
                    ERR_HINT(kSyntaxHint));
  }
  BuildTSQuery(parent, ctx.WithBoost(factor), column_info,
               *func.GetChildren()[0]);
}

// `(...)::boost(K)` -- multiplies ctx.boost by the modifier's factor
// and recurses on the inner. Returns false if `peeled` carries no
// boost modifier; true if it claimed and dispatched the cast (any
// dispatch failure throws via the inner BuildTSQuery / BuildFts*).
const duckdb::Expression* TryPeelBoostCast(const duckdb::Expression& peeled,
                                           irs::score_t& factor) {
  if (peeled.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return nullptr;
  }
  const auto& cast_expr = peeled.Cast<duckdb::BoundCastExpression>();
  const auto boost = TryGetBoostModifier(cast_expr.GetReturnType());
  if (!boost) {
    return nullptr;
  }
  factor = static_cast<irs::score_t>(*boost);
  return &cast_expr.Child();
}

bool TryDispatchBoostCast(irs::BooleanFilter& parent, const FilterContext& ctx,
                          const SearchColumnInfo& column_info,
                          const duckdb::Expression& peeled) {
  irs::score_t factor;
  const auto* child = TryPeelBoostCast(peeled, factor);
  if (!child) {
    return false;
  }
  BuildTSQuery(parent, ctx.WithBoost(factor), column_info, *child);
  return true;
}

bool TryDispatchSqlBoostCast(irs::BooleanFilter& filter,
                             const FilterContext& ctx,
                             const duckdb::Expression& peeled) {
  irs::score_t factor;
  const auto* child = TryPeelBoostCast(peeled, factor);
  if (!child) {
    return false;
  }
  // ::boost is only meaningful inside an inverted-index match, so a child
  // predicate the index cannot claim is a user error even when building
  // speculatively.
  if (auto s = FromExpression(filter, ctx.WithBoost(factor), *child); !s.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("::boost(K) used on a predicate the inverted index could not "
              "claim: ",
              s.message()),
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
    if (!tokenizer.empty()) {
      expr = &cast_expr.Child();
      val = TryGetConstant(UnwrapTSQueryCast(*expr));
      if (val && IsTSQueryStructType(val->type())) {
        val = nullptr;
      }
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
  auto wrapper = ResolveTokenizerOrThrow(ctx, tokenizer);
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
  BuildTSQuery(filter, ctx.WithTokenizer(*tokenizer), *column_info, expr);
}

absl::Status FromComparisonExpression(
  irs::BooleanFilter& filter, const FilterContext& ctx,
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
      return absl::UnimplementedError(absl::StrCat(
        "Unsupported comparison type: ", static_cast<int>(cmp_type)));
  }
}

absl::Status FromOperatorExpression(
  irs::BooleanFilter& filter, const FilterContext& ctx,
  const duckdb::BoundOperatorExpression& op_expr) {
  const auto op_type = op_expr.GetExpressionType();
  switch (op_type) {
    case duckdb::ExpressionType::OPERATOR_NOT: {
      SDB_ASSERT(op_expr.GetChildren().size() == 1);
      auto negated_ctx = ctx;
      negated_ctx.negated = !ctx.negated;
      return FromExpression(filter, negated_ctx, *op_expr.GetChildren()[0]);
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
      return absl::UnimplementedError(
        absl::StrCat("Unsupported operator type: ", static_cast<int>(op_type)));
  }
}

absl::Status FromExpression(irs::BooleanFilter& filter,
                            const FilterContext& ctx,
                            const duckdb::Expression& expr) {
  // Peel the TSQUERY_MODIFIER -> BOOLEAN coercion that the WHERE-binder
  // inserts around `(predicate)::boost(K)` at the predicate root.
  if (TryDispatchSqlBoostCast(filter, ctx, UnwrapBoostBoolCoercion(expr))) {
    return absl::OkStatus();
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
      return absl::UnimplementedError(absl::StrCat(
        "Unsupported conjunction type: ", static_cast<int>(conj_type)));
    }
    case duckdb::ExpressionClass::BOUND_OPERATOR:
      return FromOperatorExpression(
        filter, ctx, expr.Cast<duckdb::BoundOperatorExpression>());
    case duckdb::ExpressionClass::BOUND_FUNCTION:
      return FromFunctionExpression(
        filter, ctx, expr.Cast<duckdb::BoundFunctionExpression>());
    default:
      return absl::UnimplementedError(
        absl::StrCat("Unsupported expression class: ",
                     static_cast<int>(expr.GetExpressionClass())));
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
    cur = &cast.Child();
  }
  if (cur->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &cur->Cast<duckdb::BoundConstantExpression>().GetValue();
}

const SearchColumnInfo* FindColumnRefInfo(
  const FilterContext& ctx, const duckdb::BoundColumnRefExpression& ref) {
  auto cache_it = ctx.column_cache.find(ref.Binding());
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
  return &ctx.column_cache.emplace(ref.Binding(), std::move(info.value()))
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

struct UnwrappedField {
  const duckdb::Expression* expr;
  std::optional<duckdb::LogicalType> override_type;
};

UnwrappedField UnwrapFieldCast(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return {&expr, std::nullopt};
  }
  const auto& c = expr.Cast<duckdb::BoundCastExpression>();
  return {&c.Child(), c.GetReturnType()};
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

  if (matched_unwrapped && unwrapped.override_type &&
      unwrapped.override_type->id() != info->logical_type.id()) {
    if (IsNumericTypeId(unwrapped.override_type->id())) {
      if (!irs::field_limits::valid(info->numeric_field_id)) {
        return nullptr;
      }
      // Numeric field id is set up -> it's json leaf
      info->logical_type = duckdb::LogicalType::DOUBLE;
    } else {
      info->logical_type = *unwrapped.override_type;
    }
  }

  if (!catalog::term_dict::IsSupported(
        catalog::term_dict::Classify(info->logical_type.id()))) {
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

bool IsFilterableType(duckdb::LogicalTypeId type_id) {
  return catalog::term_dict::IsSupported(catalog::term_dict::Classify(type_id));
}

void ValidateFilterType(duckdb::LogicalTypeId type_id) {
  if (!IsFilterableType(type_id)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("Unsupported type id ", static_cast<int>(type_id), " for filter"),
      ERR_HINT("The value's type must match the column's indexed type."));
  }
}

bool IsRangeNumericValueType(duckdb::LogicalTypeId id) {
  return catalog::term_dict::IsNumeric(catalog::term_dict::Classify(id)) ||
         id == duckdb::LogicalTypeId::DECIMAL;
}

// Sees through the transit casts
// between string-ish types and the TSQUERY struct family in both
// directions of implicit promotion:
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
const duckdb::Expression& UnwrapTSQueryCast(const duckdb::Expression& expr) {
  const auto is_stringish = [](const duckdb::LogicalType& type) {
    return type.id() == duckdb::LogicalTypeId::VARCHAR ||
           type.id() == duckdb::LogicalTypeId::BLOB ||
           type.id() == duckdb::LogicalTypeId::STRING_LITERAL;
  };
  const duckdb::Expression* cur = &expr;
  while (cur->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = cur->Cast<duckdb::BoundCastExpression>();
    const auto& target = cast.GetReturnType();
    const auto& source = cast.Child().GetReturnType();
    // Modifier-bearing casts must be preserved so the walker sees them.
    if (!TryGetTokenizerModifier(target).empty() ||
        TryGetBoostModifier(target)) {
      break;
    }
    // Peel transit casts between string-ish types and the TSQUERY
    // struct family only -- a plain VARCHAR->VARCHAR cast must stay.
    const auto in_family = [&](const duckdb::LogicalType& type) {
      return IsTSQueryStructType(type) || is_stringish(type);
    };
    if ((!IsTSQueryStructType(target) && !IsTSQueryStructType(source)) ||
        !in_family(target) || !in_family(source)) {
      break;
    }
    cur = &cast.Child();
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

namespace {

[[noreturn]] void ThrowArgError(const ArgError& err, std::string_view suffix) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG(err.label, suffix), ERR_HINT(err.hint));
}

}  // namespace

void GetVarcharArg(const duckdb::Expression& expr, std::string& out,
                   ArgError err) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val || val->IsNull()) {
    ThrowArgError(err, " must be a non-null VARCHAR constant");
  }
  if (IsTSQueryStructType(val->type())) {
    auto parts = TryGetTSQueryParts(*val);
    if (!parts) {
      ThrowArgError(err, " must be a non-null VARCHAR constant");
    }
    if (!parts->tokenizer.empty() || parts->boost != 1.0f) {
      ThrowArgError(err, " must not carry boost or tokenize modifiers");
    }
    out = std::move(parts->text);
    return;
  }
  if (val->type().id() == duckdb::LogicalTypeId::BLOB) {
    out = duckdb::StringValue::Get(*val);
    return;
  }
  if (!TryCoerce(*val, duckdb::LogicalTypeId::VARCHAR, out)) {
    ThrowArgError(err, " must be a VARCHAR constant");
  }
}

void GetIntArg(const duckdb::Expression& expr, int64_t& out, ArgError err) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    ThrowArgError(err, " must be a non-null INTEGER constant");
  }
  if (!TryCoerce(*val, duckdb::LogicalTypeId::BIGINT, out)) {
    ThrowArgError(err, " must be an INTEGER constant");
  }
}

void GetBoolArg(const duckdb::Expression& expr, bool& out, ArgError err) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    ThrowArgError(err, " must be a non-null BOOLEAN constant");
  }
  if (!TryCoerce(*val, duckdb::LogicalTypeId::BOOLEAN, out)) {
    ThrowArgError(err, " must be a BOOLEAN constant");
  }
}

void GetDoubleArg(const duckdb::Expression& expr, double& out, ArgError err) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    ThrowArgError(err, " must be a non-null numeric constant");
  }
  if (!TryCoerce(*val, duckdb::LogicalTypeId::DOUBLE, out)) {
    ThrowArgError(err, " must be a numeric constant");
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

namespace {

void BuildTSQueryValue(irs::BooleanFilter& parent, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::Value& value) {
  auto parts = TryGetTSQueryParts(value);
  if (!parts) {
    AddFilter<irs::Empty>(parent);
    return;
  }
  const auto structured =
    TryParseStructuredTSQueryText(parts->text, ctx.client_context);
  const auto emit = [&](const FilterContext& sub_ctx) {
    if (structured) {
      BuildTSQuery(parent, sub_ctx, column_info, *structured);
    } else {
      BuildFtsTokens(parent, sub_ctx, column_info, parts->text,
                     /*require_all=*/false);
    }
  };
  auto boosted = ctx.WithBoost(parts->boost);
  if (parts->tokenizer.empty()) {
    return emit(boosted);
  }
  if (parts->tokenizer == irs::StringTokenizer::type_name()) {
    return emit(boosted.WithTokenizer(boosted.identity));
  }
  auto wrapper = ResolveTokenizerOrThrow(ctx, parts->tokenizer);
  emit(boosted.WithTokenizer(*wrapper));
}

}  // namespace

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
    if (cast.Child().GetReturnType().id() == duckdb::LogicalTypeId::BOOLEAN) {
      const auto* val = TryGetConstant(cast.Child());
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
    const auto& val =
      unwrapped.Cast<duckdb::BoundConstantExpression>().GetValue();
    if (val.IsNull()) {
      AddFilter<irs::Empty>(parent);
      return;
    }
    if (IsTSQueryStructType(val.type())) {
      BuildTSQueryValue(parent, ctx, column_info, val);
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
  const auto op =
    ClassifyTSQueryFunction(func.Function().GetName().GetIdentifierName());

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
    case TSQueryOp::Unknown:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("Not a TSQUERY-producing function: ",
                func.Function().GetName().GetIdentifierName()),
        ERR_HINT("Use a TSQUERY constructor (ts_phrase, ts_like, ts_between, "
                 "ts_ngram, ts_levenshtein, ts_regexp, ts_any, ts_all, "
                 "ts_compound, ...) or 'literal'::TSQUERY."));
  }
  SDB_UNREACHABLE();
}

absl::Status MakeSearchFilter(
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

    if (auto s = FromExpression(root, ctx, *expr); !s.ok()) {
      return s;
    }
  }
  return absl::OkStatus();
}

}  // namespace sdb::connector
