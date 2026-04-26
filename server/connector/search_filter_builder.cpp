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
#include <iresearch/parser/parser.h>

#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/all_filter.hpp>
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
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/wildcard_utils.hpp>

#include "basics/result_or.h"
#include "basics/string_utils.h"
#include "catalog/mangling.h"
#include "functions/search.h"
#include "functions/string.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_filter.hpp"

namespace sdb::connector {

struct FilterContext {
  bool negated = false;
  irs::score_t boost = irs::kNoBoost;
  const ColumnGetter& column_getter;
  containers::FlatHashMap<catalog::Column::Id, SearchColumnInfo>& column_cache;
  // Optional override for the tokenizer used by tokenisation-driven
  // emitters (BuildFtsTokens / BuildFtsPhrase / FromNgramMatch /
  // EmitPhraseTokens). When null, the column's tokenizer is used. Set
  // by the `<expr>::tokenizer('<name>')` cast for the inner subtree.
  irs::analysis::Analyzer* tokenizer = nullptr;
};

// Returns the tokenizer the current ctx wants used for tokenisation:
// the cast-imposed override if set, otherwise the column's tokenizer.
irs::analysis::Analyzer* ActiveTokenizer(const FilterContext& ctx,
                                         const SearchColumnInfo& column_info) {
  return ctx.tokenizer ? ctx.tokenizer : column_info.tokenizer.analyzer.get();
}

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr);

namespace {

const duckdb::BoundColumnRefExpression* TryGetColumnRef(
  const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundColumnRefExpression>();
}

const duckdb::Value* TryGetConstant(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

const SearchColumnInfo* FindColumnInfo(
  const FilterContext& ctx, const duckdb::BoundColumnRefExpression& ref) {
  // Try cache first -- keyed on column_id from a previous resolution.
  // We do a two-step lookup: first resolve via column_getter to get the
  // column_id, then check cache.  The column_getter itself may be cheap
  // (just a span lookup), but caching avoids repeated analyzer copies.

  // We cannot cache by binding alone (table_index + column_index) because
  // different bindings may map to the same catalog column_id.  Instead
  // we resolve first, then cache by column_id.

  auto info = ctx.column_getter(ref);
  if (!info) {
    return nullptr;
  }

  auto cache_it = ctx.column_cache.find(info->column_id);
  if (cache_it != ctx.column_cache.end()) {
    SDB_ASSERT(cache_it->second.logical_type.id() !=
                 duckdb::LogicalTypeId::VARCHAR ||
               cache_it->second.tokenizer.analyzer);
    return &cache_it->second;
  }

  auto column_id = info->column_id;
  return &ctx.column_cache.emplace(column_id, std::move(info.value()))
            .first->second;
}

}  // namespace

void MakeFieldName(catalog::Column::Id column_id, std::string& field_name) {
  basics::StrResize(field_name, sizeof(column_id));
  absl::big_endian::Store(field_name.data(), column_id);
}

void MakeFieldName(const SearchColumnInfo& column, std::string& field_name) {
  MakeFieldName(column.column_id, field_name);
}

namespace {

Result MangleForType(duckdb::LogicalTypeId type_id, std::string& field_name) {
  switch (type_id) {
    case duckdb::LogicalTypeId::VARCHAR:
      search::mangling::MangleString(field_name);
      return {};
    case duckdb::LogicalTypeId::BOOLEAN:
      search::mangling::MangleBool(field_name);
      return {};
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DATE:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      search::mangling::MangleNumeric(field_name);
      return {};
    default:
      return {ERROR_NOT_IMPLEMENTED, "Unsupported type id ",
              static_cast<int>(type_id), " for field mangling"};
  }
}

void ResetNumericStream(irs::NumericTokenizer& stream,
                        duckdb::LogicalTypeId type_id,
                        const duckdb::Value& value) {
  switch (type_id) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::DATE:
      stream.reset(value.GetValue<int32_t>());
      break;
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      stream.reset(value.GetValue<int64_t>());
      break;
    case duckdb::LogicalTypeId::FLOAT:
      stream.reset(value.GetValue<float>());
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      stream.reset(value.GetValue<double>());
      break;
    default:
      SDB_ASSERT(false, "ResetNumericStream called with non-numeric type");
  }
}

bool IsNumericTypeId(duckdb::LogicalTypeId id) {
  switch (id) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::FLOAT:
    case duckdb::LogicalTypeId::DOUBLE:
    case duckdb::LogicalTypeId::DATE:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return true;
    default:
      return false;
  }
}

// Looser numeric check used by IN_RANGE bound validation: accepts the
// same set as IsNumericTypeId plus DECIMAL. IN_RANGE casts bound values
// to the column's logical type before tokenising, so DECIMAL bounds on
// a DOUBLE/INT/BIGINT column work as expected. We don't fold DECIMAL
// into IsNumericTypeId itself because the legacy FromComparison /
// SetupTermFilter paths feed `type_id` directly into ResetNumericStream,
// which doesn't handle DECIMAL.
bool IsInRangeNumericValueType(duckdb::LogicalTypeId id) {
  return IsNumericTypeId(id) || id == duckdb::LogicalTypeId::DECIMAL;
}

// Sets up a ByTerm filter for a single constant value against a column.
Result SetupTermFilter(irs::ByTerm& filter, std::string& field_name,
                       const SearchColumnInfo& column_info,
                       const duckdb::Value& value) {
  SDB_ASSERT(!value.IsNull(),
             "UNKNOWN and Nulls should be handled as part of IS NULL operator. "
             "For regular filter it should be just irs::Empty!");

  auto type_id = column_info.logical_type.id();

  if (auto r = MangleForType(type_id, field_name); !r.ok()) {
    return r;
  }

  if (type_id == duckdb::LogicalTypeId::VARCHAR) {
    auto sv = value.GetValue<std::string>();
    filter.mutable_options()->term.assign(
      irs::ViewCast<irs::byte_type>(std::string_view{sv}));
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

  *filter.mutable_field() = field_name;
  return {};
}

template<typename Filter, typename Source>
auto& AddFilter(Source& parent) {
  if constexpr (std::is_same_v<Filter, irs::All>) {
    static_assert(std::is_base_of_v<irs::BooleanFilter, Source>);
    return parent.add(std::make_unique<irs::All>());
  } else {
    if constexpr (std::is_same_v<irs::Not, Source>) {
      return parent.template filter<Filter>();
    } else {
      return parent.template add<Filter>();
    }
  }
}

template<typename Filter, typename Source>
Filter& Negate(Source& parent) {
  return AddFilter<Filter>(AddFilter<irs::Not>(
    parent.type() == irs::Type<irs::Or>::id() ? AddFilter<irs::And>(parent)
                                              : parent));
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

bool IsComparisonExpr(const duckdb::Expression& expr) {
  return expr.expression_class == duckdb::ExpressionClass::BOUND_COMPARISON &&
         GetComparisonOp(expr.type) != ComparisonOp::None;
}

// Unwraps reinterpret casts between VARCHAR and TSQUERY so the filter
// builder can see through both directions of implicit promotion:
//   - VARCHAR -> TSQUERY (bare string literals into TSQUERY contexts)
//   - TSQUERY -> VARCHAR (TSQUERY-typed children flowing into VARCHAR
//     mirror overloads of `##`, e.g. PHRASE('a') ## 1, where DuckDB
//     wraps the LHS in BOUND_CAST<VARCHAR>).
// Iterative because a TSQUERY-cast can wrap a VARCHAR-cast and vice
// versa when overloads chain.
const duckdb::Expression& UnwrapTSQueryCast(const duckdb::Expression& expr) {
  const duckdb::Expression* cur = &expr;
  while (cur->expression_class == duckdb::ExpressionClass::BOUND_CAST) {
    const auto& cast = cur->Cast<duckdb::BoundCastExpression>();
    if (!cast.child) {
      break;
    }
    const auto& target = cast.return_type;
    const auto& source = cast.child->return_type;
    const bool is_tsq_to_v = IsTSQueryType(source) &&
                             target.id() == duckdb::LogicalTypeId::VARCHAR &&
                             !IsTSQueryType(target);
    const bool is_v_to_tsq =
      IsTSQueryType(target) && source.id() == duckdb::LogicalTypeId::VARCHAR;
    if (!is_tsq_to_v && !is_v_to_tsq) {
      break;
    }
    cur = cast.child.get();
  }
  return *cur;
}

Result GetVarcharArg(const duckdb::Expression& expr, std::string_view label,
                     std::string& out) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null VARCHAR constant"};
  }
  if (val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, label, " must be a VARCHAR constant"};
  }
  out = val->GetValue<std::string>();
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
      return {ERROR_BAD_PARAMETER, label, " must be an INTEGER constant"};
  }
}

Result GetBoolArg(const duckdb::Expression& expr, std::string_view label,
                  bool& out) {
  const auto* val = TryGetConstant(expr);
  if (!val || val->IsNull()) {
    return {ERROR_BAD_PARAMETER, label, " must be a non-null BOOLEAN constant"};
  }
  if (val->type().id() != duckdb::LogicalTypeId::BOOLEAN) {
    return {ERROR_BAD_PARAMETER, label, " must be a BOOLEAN constant"};
  }
  out = val->GetValue<bool>();
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
      return {ERROR_BAD_PARAMETER, label, " must be a numeric constant"};
  }
}

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
  if (op_expr.children.size() != 1) {
    return {ERROR_NOT_IMPLEMENTED, "IS NULL has ", op_expr.children.size(),
            " inputs but 1 expected"};
  }
  const auto* col_ref = TryGetColumnRef(*op_expr.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }
  std::string field_name;
  MakeFieldName(*column_info, field_name);
  search::mangling::MangleNull(field_name);
  auto& term_filter =
    ctx.negated ? Negate<irs::ByTerm>(filter) : AddFilter<irs::ByTerm>(filter);
  term_filter.boost(ctx.boost);
  *term_filter.mutable_field() = field_name;
  term_filter.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  return {};
}

template<bool GenericVersion>
Result FromBinaryEq(irs::BooleanFilter& filter, const FilterContext& ctx,
                    const duckdb::Expression& left_expr,
                    const duckdb::Expression& right_expr, bool not_equal) {
  const auto* col_ref = TryGetColumnRef(left_expr);
  const auto* const_val = TryGetConstant(right_expr);

  if (!col_ref || !const_val) {
    return {ERROR_BAD_PARAMETER,
            "Expected column reference on the left and constant on the right"};
  }

  if (const_val->IsNull()) {
    // foo == NULL is always false and foo != NULL is false too.
    AddFilter<irs::Empty>(filter);
    return {};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }
  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use `col @@ "
              "'value'` (tokenised) or `col @@ "
              "'value'::tokenizer('identity')` (raw)."};
    }
  }

  auto& term_filter = (ctx.negated != not_equal)
                        ? Negate<irs::ByTerm>(filter)
                        : AddFilter<irs::ByTerm>(filter);

  term_filter.boost(ctx.boost);
  std::string field_name;
  MakeFieldName(*column_info, field_name);
  return SetupTermFilter(term_filter, field_name, *column_info, *const_val);
}

template<bool GenericVersion>
Result FromComparison(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& field_expr,
                      const duckdb::Expression& value_expr, ComparisonOp op) {
  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }

  const auto* col_ref = TryGetColumnRef(field_expr);
  const auto* const_val = TryGetConstant(value_expr);

  if (!col_ref || !const_val) {
    return {ERROR_BAD_PARAMETER,
            "Expected column reference and constant for comparison"};
  }

  if (const_val->IsNull()) {
    AddFilter<irs::Empty>(filter);
    return {};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }
  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Range predicates "
              "(<, <=, >, >=, BETWEEN) require an identity-analyzed column."};
    }
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  auto type_id = column_info->logical_type.id();

  auto setup_base_filter = [&](auto& range_filter,
                               std::string&& fn) -> decltype(auto) {
    *range_filter.mutable_field() = std::move(fn);
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

  if (auto r = MangleForType(type_id, field_name); !r.ok()) {
    return r;
  }

  if (type_id == duckdb::LogicalTypeId::VARCHAR) {
    auto& range_filter = AddFilter<irs::ByRange>(filter);
    auto sv = const_val->GetValue<std::string>();
    setup_base_filter(range_filter, std::move(field_name))
      .assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
  } else if (type_id == duckdb::LogicalTypeId::BOOLEAN) {
    auto& range_filter = AddFilter<irs::ByRange>(filter);
    setup_base_filter(range_filter, std::move(field_name))
      .assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(const_val->GetValue<bool>())));
  } else if (IsNumericTypeId(type_id)) {
    auto& range_filter = AddFilter<irs::ByGranularRange>(filter);
    irs::NumericTokenizer stream;
    ResetNumericStream(stream, type_id, *const_val);
    irs::SetGranularTerm(setup_base_filter(range_filter, std::move(field_name)),
                         stream);
  } else {
    return {ERROR_NOT_IMPLEMENTED, "Unsupported type for range comparison: ",
            static_cast<int>(type_id)};
  }
  return {};
}

Result FromBetween(irs::BooleanFilter& filter, const FilterContext& ctx,
                   const duckdb::BoundBetweenExpression& between) {
  // Decompose BETWEEN into conjunction of two range comparisons.
  // BETWEEN a AND b  =>  field >= a (or >) AND field <= b (or <)
  // NOT BETWEEN       =>  field < a (or <=) OR field > b (or >=)

  const auto* col_ref = TryGetColumnRef(*between.input);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "BETWEEN input is not a column reference"};
  }
  const auto* lower_val = TryGetConstant(*between.lower);
  const auto* upper_val = TryGetConstant(*between.upper);
  if (!lower_val || !upper_val) {
    return {ERROR_BAD_PARAMETER, "BETWEEN bounds must be constants"};
  }

  if (!ctx.negated) {
    // field >= lower AND field <= upper (with inclusivity flags)
    auto lower_op =
      between.lower_inclusive ? ComparisonOp::Ge : ComparisonOp::Gt;
    auto upper_op =
      between.upper_inclusive ? ComparisonOp::Le : ComparisonOp::Lt;

    auto& group = AddFilter<irs::And>(filter);
    group.boost(ctx.boost);

    // Sub-context: not negated, no extra boost (already on group)
    FilterContext sub_ctx = ctx;
    sub_ctx.negated = false;
    sub_ctx.boost = irs::kNoBoost;

    auto r = FromComparison<true>(group, sub_ctx, *between.input,
                                  *between.lower, lower_op);
    if (!r.ok()) {
      return r;
    }
    return FromComparison<true>(group, sub_ctx, *between.input, *between.upper,
                                upper_op);
  }

  // NOT BETWEEN: De Morgan -> field < lower OR field > upper
  auto lower_op = between.lower_inclusive ? ComparisonOp::Lt : ComparisonOp::Le;
  auto upper_op = between.upper_inclusive ? ComparisonOp::Gt : ComparisonOp::Ge;

  auto& group = AddFilter<irs::Or>(filter);
  group.boost(ctx.boost);

  FilterContext sub_ctx = ctx;
  sub_ctx.negated = false;
  sub_ctx.boost = irs::kNoBoost;

  auto r = FromComparison<true>(group, sub_ctx, *between.input, *between.lower,
                                lower_op);
  if (!r.ok()) {
    return r;
  }
  return FromComparison<true>(group, sub_ctx, *between.input, *between.upper,
                              upper_op);
}

template<bool GenericVersion>
Result FromIn(irs::BooleanFilter& filter, const FilterContext& ctx,
              const duckdb::BoundOperatorExpression& op_expr) {
  if (op_expr.children.size() < 2) {
    return {ERROR_NOT_IMPLEMENTED, "IN has ", op_expr.children.size(),
            " inputs but at least 2 expected"};
  }

  const auto* col_ref = TryGetColumnRef(*op_expr.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }

  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR &&
        column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use `col @@ "
              "ANY_OF('a', 'b', ...)` (tokenised) or `col @@ ANY_OF("
              "'a'::tokenizer('identity'), ...)` (raw)."};
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

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  auto type_id = column_info->logical_type.id();
  auto r = MangleForType(type_id, field_name);
  if (!r.ok()) {
    return r;
  }

  auto& terms_filter = ctx.negated ? Negate<irs::ByTerms>(filter)
                                   : AddFilter<irs::ByTerms>(filter);
  terms_filter.boost(ctx.boost);
  *terms_filter.mutable_field() = field_name;
  auto& opts = *terms_filter.mutable_options();

  for (const auto* value : values) {
    if (type_id == duckdb::LogicalTypeId::VARCHAR) {
      auto sv = value->GetValue<std::string>();
      opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
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

template<bool GenericVersion>
Result FromLike(irs::BooleanFilter& filter, const FilterContext& ctx,
                const duckdb::Expression& field_expr,
                const duckdb::Expression& pattern_expr,
                char escape_char = '\\') {
  const auto* col_ref = TryGetColumnRef(field_expr);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* const_val = TryGetConstant(pattern_expr);
  if (!const_val) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate LIKE pattern as constant"};
  }

  if (const_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate LIKE pattern as VARCHAR"};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column is not indexed"};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  if constexpr (GenericVersion) {
    if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
      return {ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR"};
    }
    if (column_info->tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use `col @@ "
              "LIKE('pattern')`."};
    }
  } else {
    SDB_ASSERT(column_info->logical_type.id() == duckdb::LogicalTypeId::VARCHAR,
               ERROR_BAD_PARAMETER, "TERM_LIKE field is not VARCHAR");
  }

  search::mangling::MangleString(field_name);
  auto& wildcard_filter = ctx.negated ? Negate<irs::ByWildcard>(filter)
                                      : AddFilter<irs::ByWildcard>(filter);
  wildcard_filter.boost(ctx.boost);
  *wildcard_filter.mutable_field() = field_name;
  auto pattern =
    LikeEscapePattern(const_val->GetValue<std::string>(), escape_char);
  wildcard_filter.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(std::string_view{pattern}));
  return {};
}

// Phrase-gap representation, shared between PHRASE (variadic
// text/INTEGER/INTEGER[] grammar) and the `##` operator. Offsets are
// already +1-adjusted to raw iresearch positions: a gap of N
// "intervening words" maps to offs_min = offs_max = N + 1, and a 2-
// element interval [lo, hi] maps to offs_min = lo + 1, offs_max = hi + 1.
struct PhraseGap {
  size_t offs_min{0};
  size_t offs_max{0};
};

// True iff `id` is one of the integral types we accept as a phrase-gap
// scalar (any signed integer narrower than or equal to BIGINT). Values
// in this set need a sign check at parse time.
bool IsPhraseGapIntegralTypeId(duckdb::LogicalTypeId id) {
  return id == duckdb::LogicalTypeId::TINYINT ||
         id == duckdb::LogicalTypeId::SMALLINT ||
         id == duckdb::LogicalTypeId::INTEGER ||
         id == duckdb::LogicalTypeId::BIGINT;
}

// True iff a Value is shaped like a phrase gap (integral scalar or a
// LIST/ARRAY of two integers). FromPhrase uses this to distinguish
// gaps from VARCHAR text tokens at runtime in its variadic loop. The
// `##` walker uses a parallel return-type predicate
// (IsPhraseSeqGapType) that operates on Expression rather than Value.
bool IsPhraseGapValue(const duckdb::Value& val) {
  if (val.IsNull()) {
    return false;
  }
  const auto id = val.type().id();
  return IsPhraseGapIntegralTypeId(id) || id == duckdb::LogicalTypeId::LIST ||
         id == duckdb::LogicalTypeId::ARRAY;
}

// Parses a constant Value into a PhraseGap. Accepts a non-negative
// integral scalar for an exact gap, or a 2-element LIST/ARRAY of
// non-negative integers for an interval gap (min <= max). The returned
// PhraseGap has offsets already +1-adjusted; callers can hand it
// directly to ByPhraseOptions::push_back(offs_min, offs_max). On error,
// returns a Result whose message is prefixed with `label` (e.g.
// "PHRASE", "##") so the call site is identifiable.
ResultOr<PhraseGap> ParsePhraseGap(const duckdb::Value& val,
                                   std::string_view label) {
  auto err = [&](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (val.IsNull()) {
    return err(label, " gap must be a non-null constant");
  }
  const auto id = val.type().id();
  if (IsPhraseGapIntegralTypeId(id)) {
    auto raw = val.GetValue<int64_t>();
    if (raw < 0) {
      return err(label, " gap must be >= 0, got ", raw);
    }
    return PhraseGap{.offs_min = static_cast<size_t>(raw) + 1,
                     .offs_max = static_cast<size_t>(raw) + 1};
  }
  if (id == duckdb::LogicalTypeId::LIST || id == duckdb::LogicalTypeId::ARRAY) {
    const auto& children = id == duckdb::LogicalTypeId::ARRAY
                             ? duckdb::ArrayValue::GetChildren(val)
                             : duckdb::ListValue::GetChildren(val);
    if (children.size() != 2) {
      return err(label,
                 " interval gap must be a 2-element list [min, max], got ",
                 children.size(), " elements");
    }
    if (!IsPhraseGapIntegralTypeId(children[0].type().id()) ||
        !IsPhraseGapIntegralTypeId(children[1].type().id()) ||
        children[0].IsNull() || children[1].IsNull()) {
      return err(label, " interval gap elements must be non-negative integers");
    }
    auto lo = children[0].GetValue<int64_t>();
    auto hi = children[1].GetValue<int64_t>();
    if (lo < 0 || hi < 0 || lo > hi) {
      return err(label, " interval gap must satisfy 0 <= min <= max, got [", lo,
                 ", ", hi, "]");
    }
    return PhraseGap{.offs_min = static_cast<size_t>(lo) + 1,
                     .offs_max = static_cast<size_t>(hi) + 1};
  }
  return err(label, " gap has unsupported type: ", val.type().ToString());
}

Result FromPhrase(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  // PHRASE is registered with at least one VARCHAR arg (plus variadic
  // ANY tail), so DuckDB's function resolver rejects empty calls at
  // bind time before we get here.
  SDB_ASSERT(!func.children.empty());

  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }

  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(filter)
                             : AddFilter<irs::ByPhrase>(filter);
  phrase.boost(ctx.boost);
  *phrase.mutable_field() = field_name;
  auto* opts = phrase.mutable_options();
  auto* analyzer = ActiveTokenizer(ctx, column_info);
  const irs::TermAttr* token = irs::get<irs::TermAttr>(*analyzer);

  std::optional<PhraseGap> pending_gap;

  // A non-constant argument is an index-only restriction -- a future
  // full-scan PHRASE executor could handle it -- so we return Result
  // and let the caller roll back any partially-built phrase filter so
  // this predicate falls through to regular execution. All OTHER
  // violations below are gap-grammar errors: the PHRASE call is
  // malformed and no executor can satisfy it, so they throw with a
  // specific message rather than letting SearchStubFn surface its
  // generic "outside inverted index context" error.
  for (size_t i = 0; i < func.children.size(); ++i) {
    const auto* const_val = TryGetConstant(*func.children[i]);
    if (!const_val) {
      return {ERROR_BAD_PARAMETER, "PHRASE argument ", i,
              " must be a constant"};
    }
    if (const_val->type().id() == duckdb::LogicalTypeId::VARCHAR) {
      auto text = const_val->GetValue<std::string>();
      analyzer->reset(std::string_view{text});
      while (analyzer->next()) {
        if (pending_gap) {
          // First token of a new text pattern: apply pending gap.
          opts
            ->push_back<irs::ByTermOptions>(pending_gap->offs_min,
                                            pending_gap->offs_max)
            .term.assign(token->value);
        } else {
          // No pending gap: first term or adjacent token within same
          // pattern.
          opts->push_back<irs::ByTermOptions>().term.assign(token->value);
        }
        pending_gap.reset();
      }
      continue;
    }
    if (!IsPhraseGapValue(*const_val)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERROR_BAD_PARAMETER),
        ERR_MSG("PHRASE argument ", i,
                " has unsupported type; expected text, non-negative "
                "integer, or non-negative integer array"));
    }
    if (opts->empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                      ERR_MSG("PHRASE gap at argument ", i,
                              " must be preceded by a text pattern"));
    }
    if (pending_gap) {
      THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                      ERR_MSG("PHRASE has consecutive gaps at argument ", i));
    }
    auto gap_or = ParsePhraseGap(*const_val, "PHRASE");
    if (!gap_or) {
      THROW_SQL_ERROR(
        ERR_CODE(ERROR_BAD_PARAMETER),
        ERR_MSG(gap_or.error().errorMessage(), " (argument ", i, ")"));
    }
    pending_gap = *gap_or;
  }

  if (pending_gap) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_BAD_PARAMETER),
      ERR_MSG("PHRASE ends with a gap; a text pattern must follow each gap"));
  }
  if (opts->empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_BAD_PARAMETER),
      ERR_MSG("PHRASE text arguments produced no searchable terms"));
  }
  return {};
}

Result FromBoost(irs::BooleanFilter& filter, const FilterContext& ctx,
                 const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "BOOST has ", func.children.size(),
            " inputs but 2 expected"};
  }

  const auto* boost_val = TryGetConstant(*func.children[1]);
  if (!boost_val) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate boost value as constant"};
  }

  const auto boost = static_cast<irs::score_t>(boost_val->GetValue<double>());
  if (boost < 0.0) {
    return {ERROR_BAD_PARAMETER, "BOOST value must be >= 0, got ", boost};
  }

  auto boosted_ctx = ctx;
  boosted_ctx.boost = boost;
  return FromExpression(filter, boosted_ctx, *func.children[0]);
}

void FillByWildcardOptions(std::string_view pattern,
                           irs::ByWildcardOptions& out) {
  auto escaped = LikeEscapePattern(std::string{pattern}, '\\');
  out.term.assign(irs::ViewCast<irs::byte_type>(std::string_view{escaped}));
}

// LEVENSHTEIN argument parsing + range validation. Used identically by
// the standalone TSQUERY-surface emitter (`FromLevenshteinMatch`) and
// the phrase-part emitter inside EmitPhraseSeq.
struct LevenshteinArgs {
  std::string text;
  int64_t distance = 1;
  bool with_transpositions = true;
};

ResultOr<LevenshteinArgs> ParseLevenshteinArgs(
  const duckdb::BoundFunctionExpression& func) {
  auto err = [](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (func.children.empty() || func.children.size() > 3) {
    return err(
      "LEVENSHTEIN expects 1 to 3 arguments "
      "(text, distance?, transpositions?), got ",
      func.children.size());
  }
  LevenshteinArgs out;
  if (auto r = GetVarcharArg(*func.children[0], "LEVENSHTEIN text", out.text);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (func.children.size() >= 2) {
    if (auto r =
          GetIntArg(*func.children[1], "LEVENSHTEIN distance", out.distance);
        !r.ok()) {
      return std::unexpected<Result>{std::in_place, std::move(r)};
    }
  }
  if (out.distance < 0 || out.distance > 4) {
    return err("LEVENSHTEIN distance must be between 0 and 4, got ",
               out.distance);
  }
  if (func.children.size() >= 3) {
    if (auto r = GetBoolArg(*func.children[2], "LEVENSHTEIN transpositions",
                            out.with_transpositions);
        !r.ok()) {
      return std::unexpected<Result>{std::in_place, std::move(r)};
    }
  }
  if (out.with_transpositions && out.distance > 3) {
    return err(
      "LEVENSHTEIN distance must be between 0 and 3 when "
      "transpositions is true, got ",
      out.distance);
  }
  return out;
}

void FillByEditDistanceOptions(const LevenshteinArgs& args,
                               irs::ByEditDistanceOptions& out) {
  out.term.assign(irs::ViewCast<irs::byte_type>(std::string_view{args.text}));
  out.max_distance = static_cast<uint8_t>(args.distance);
  out.with_transpositions = args.with_transpositions;
  out.max_terms = 64;
}

// Tokenises `text` via the column analyzer and pushes ByTermOptions
// parts into `options`. The FIRST token gets `base_gap` -- the gap
// from the previous phrase part. Subsequent tokens are strictly
// adjacent ({1, 1}). Errors if the analyzer produces no tokens. Shared
// between BuildFtsPhrase (called with PhraseGap{}) and EmitPhraseSeq's
// nested-PHRASE phrase-part branch.
Result EmitPhraseTokens(irs::ByPhraseOptions& options, const FilterContext& ctx,
                        const SearchColumnInfo& column_info,
                        std::string_view text, PhraseGap base_gap) {
  auto* analyzer = ActiveTokenizer(ctx, column_info);
  if (!analyzer || !analyzer->reset(text)) {
    return {ERROR_BAD_PARAMETER, "PHRASE failed to analyse '", text, "'"};
  }
  const auto* tok = irs::get<irs::TermAttr>(*analyzer);
  bool first = true;
  while (analyzer->next()) {
    const PhraseGap g = first ? base_gap : PhraseGap{1, 1};
    auto& part = options.push_back<irs::ByTermOptions>(g.offs_min, g.offs_max);
    part.term.assign(tok->value);
    first = false;
  }
  if (first) {
    return {ERROR_BAD_PARAMETER, "PHRASE('", text,
            "') produced no tokens after analysis"};
  }
  return {};
}

Result FromNgram(irs::BooleanFilter& filter, const FilterContext& ctx,
                 const SearchColumnInfo& column_info,
                 const duckdb::BoundFunctionExpression& func) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "NGRAM field is not VARCHAR"};
  }
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            "NGRAM expects 1 or 2 arguments (text[, threshold]), got ",
            func.children.size()};
  }

  std::string target;
  if (auto r = GetVarcharArg(*func.children[0], "NGRAM text", target);
      !r.ok()) {
    return r;
  }

  float threshold = 0.7f;
  if (func.children.size() == 2) {
    double thr;
    if (auto r = GetDoubleArg(*func.children[1], "NGRAM threshold", thr);
        !r.ok()) {
      return r;
    }
    threshold = static_cast<float>(thr);
  }
  if (threshold < 0.f || threshold > 1.f) {
    return {ERROR_BAD_PARAMETER, "NGRAM threshold must be between 0 and 1"};
  }

  if ((column_info.tokenizer.features &
       irs::NGramSimilarityQuery::kRequiredFeatures) !=
      irs::NGramSimilarityQuery::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "NGRAM field should have Positions and Frequency features "
            "enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& ngram = ctx.negated ? Negate<irs::ByNGramSimilarity>(filter)
                            : AddFilter<irs::ByNGramSimilarity>(filter);
  ngram.boost(ctx.boost);
  *ngram.mutable_field() = field_name;
  ngram.mutable_options()->threshold = threshold;
  auto* analyzer = ActiveTokenizer(ctx, column_info);
  analyzer->reset(std::string_view{target});
  const irs::TermAttr* token = irs::get<irs::TermAttr>(*analyzer);
  while (analyzer->next()) {
    ngram.mutable_options()->ngrams.emplace_back(token->value);
  }
  return {};
}

Result FromLevenshtein(irs::BooleanFilter& filter, const FilterContext& ctx,
                       const SearchColumnInfo& column_info,
                       const duckdb::BoundFunctionExpression& func) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LEVENSHTEIN field is not VARCHAR"};
  }
  if (func.children.empty() || func.children.size() > 3) {
    return {ERROR_BAD_PARAMETER,
            "LEVENSHTEIN expects 1 to 3 arguments "
            "(text, distance?, transpositions?), got ",
            func.children.size()};
  }

  auto args_or = ParseLevenshteinArgs(func);
  if (!args_or) {
    return std::move(args_or.error());
  }
  const auto& args = *args_or;

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& edit_filter = ctx.negated ? Negate<irs::ByEditDistance>(filter)
                                  : AddFilter<irs::ByEditDistance>(filter);
  edit_filter.boost(ctx.boost);
  *edit_filter.mutable_field() = field_name;
  FillByEditDistanceOptions(args, *edit_filter.mutable_options());
  return {};
}

Result FromTermIn(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() < 2) {
    return {ERROR_NOT_IMPLEMENTED, "TERM_IN has ", func.children.size(),
            " inputs but at least 2 expected"};
  }

  const auto* col_ref = TryGetColumnRef(*func.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column was not found"};
  }

  // Collect constant values from children[1..]
  std::vector<const duckdb::Value*> values;
  values.reserve(func.children.size() - 1);
  for (size_t i = 1; i < func.children.size(); ++i) {
    const auto* val = TryGetConstant(*func.children[i]);
    if (!val) {
      return {ERROR_BAD_PARAMETER, "Failed to evaluate value as constant"};
    }
    if (!val->IsNull()) {
      values.push_back(val);
    }
  }

  if (values.empty()) {
    AddFilter<irs::Empty>(filter);
    return {};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  auto type_id = column_info->logical_type.id();
  auto res = MangleForType(type_id, field_name);
  if (!res.ok()) {
    return res;
  }

  auto& terms_filter = ctx.negated ? Negate<irs::ByTerms>(filter)
                                   : AddFilter<irs::ByTerms>(filter);
  terms_filter.boost(ctx.boost);
  *terms_filter.mutable_field() = field_name;
  auto& opts = *terms_filter.mutable_options();

  for (const auto* value : values) {
    if (type_id == duckdb::LogicalTypeId::VARCHAR) {
      auto sv = value->GetValue<std::string>();
      opts.terms.emplace(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
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
              "Unsupported type for TERM_IN: ", static_cast<int>(type_id)};
    }
  }
  return {};
}

// ---------------------------------------------------------------------------
// TSQUERY dispatch (`@@`, `||`, `&&`, `!!`, `^`, leaf constructors)
//
// Leaf logic below lives alongside the legacy FromPhrase/FromLike/... above
// rather than sharing code with them -- the legacy entries extract a column
// ref from child[0], whereas TSQUERY leaves carry only string literals and
// the column comes from the enclosing @@ match.
// ---------------------------------------------------------------------------

enum class TSQueryOp {
  Unknown,
  Phrase,
  Term,
  Like,
  Prefix,
  Ngram,
  Fuzzy,
  AnyOf,
  AllOf,
  InRange,
  Tokenize,
  PlainToTsquery,
  PhraseToTsquery,
  WebsearchToTsquery,
  TsqueryPhrase,
  Or,
  And,
  Not,
  Boost,
  PhraseSeq,
  ToTSQuery,
};

TSQueryOp ClassifyTSQueryFunction(std::string_view name) {
  if (name == kTSQPhrase) {
    return TSQueryOp::Phrase;
  }
  if (name == kTSQLike) {
    return TSQueryOp::Like;
  }
  if (name == kTSQPrefix) {
    return TSQueryOp::Prefix;
  }
  if (name == kTSQNgram) {
    return TSQueryOp::Ngram;
  }
  if (name == kTSQLevenshtein) {
    return TSQueryOp::Fuzzy;
  }
  if (name == kTSQAnyOf) {
    return TSQueryOp::AnyOf;
  }
  if (name == kTSQAllOf) {
    return TSQueryOp::AllOf;
  }
  if (name == kTSQInRange) {
    return TSQueryOp::InRange;
  }
  if (name == kTSQTokenize) {
    return TSQueryOp::Tokenize;
  }
  if (name == kPlainToTsquery) {
    return TSQueryOp::PlainToTsquery;
  }
  if (name == kPhraseToTsquery) {
    return TSQueryOp::PhraseToTsquery;
  }
  if (name == kWebsearchToTsquery) {
    return TSQueryOp::WebsearchToTsquery;
  }
  if (name == kTsqueryPhrase) {
    return TSQueryOp::TsqueryPhrase;
  }
  if (name == kTSQueryOr) {
    return TSQueryOp::Or;
  }
  if (name == kTSQueryAnd) {
    return TSQueryOp::And;
  }
  if (name == kTSQueryNot) {
    return TSQueryOp::Not;
  }
  if (name == kTSQueryBoost) {
    return TSQueryOp::Boost;
  }
  if (name == kTSQueryPhraseSeq) {
    return TSQueryOp::PhraseSeq;
  }
  if (name == kToTsquery) {
    return TSQueryOp::ToTSQuery;
  }
  return TSQueryOp::Unknown;
}

Result BuildFtsPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }
  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);
  return EmitPhraseTokens(*phrase.mutable_options(), ctx, column_info, text,
                          PhraseGap{});
}

Result BuildFtsTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Value& value) {
  if (value.IsNull()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }
  auto& term =
    ctx.negated ? Negate<irs::ByTerm>(parent) : AddFilter<irs::ByTerm>(parent);
  term.boost(ctx.boost);
  std::string field_name;
  MakeFieldName(column_info, field_name);
  return SetupTermFilter(term, field_name, column_info, value);
}

// Tokenises `text` via the column's analyzer and emits an iresearch
// filter matching any (min_match=1) or all (min_match=token_count) of
// the resulting tokens. For non-VARCHAR columns falls back to a raw
// ByTerm. For 0 tokens emits irs::Empty; for 1 token emits ByTerm.
// Used by the bare-string VARCHAR path, TOKENIZE(text), and
// plainto_tsquery(text).
Result BuildFtsTokens(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view text, bool require_all) {
  auto* analyzer = ActiveTokenizer(ctx, column_info);
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR ||
      !analyzer) {
    // Non-VARCHAR or no analyzer: emit a raw term.
    return BuildFtsTerm(parent, ctx, column_info,
                        duckdb::Value(std::string{text}));
  }
  // Collect tokens via the active analyzer (override or column).
  std::vector<irs::bstring> tokens;
  if (!analyzer->reset(text)) {
    return {ERROR_BAD_PARAMETER, "Failed to analyse '", text, "'"};
  }
  const auto* tok_attr = irs::get<irs::TermAttr>(*analyzer);
  while (analyzer->next()) {
    tokens.emplace_back(tok_attr->value.begin(), tok_attr->value.end());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  if (tokens.empty()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }
  if (tokens.size() == 1) {
    auto& term = ctx.negated ? Negate<irs::ByTerm>(parent)
                             : AddFilter<irs::ByTerm>(parent);
    term.boost(ctx.boost);
    *term.mutable_field() = field_name;
    term.mutable_options()->term.assign(tokens[0]);
    return {};
  }
  // Multi-token: ByTerms with min_match=1 (OR) or N (AND).
  auto& terms = ctx.negated ? Negate<irs::ByTerms>(parent)
                            : AddFilter<irs::ByTerms>(parent);
  terms.boost(ctx.boost);
  *terms.mutable_field() = field_name;
  auto& opts = *terms.mutable_options();
  opts.min_match = require_all ? tokens.size() : 1;
  for (auto& t : tokens) {
    opts.terms.emplace(std::move(t));
  }
  return {};
}

Result BuildFtsLike(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    std::string_view like_pattern) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR"};
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  auto& wild = ctx.negated ? Negate<irs::ByWildcard>(parent)
                           : AddFilter<irs::ByWildcard>(parent);
  wild.boost(ctx.boost);
  *wild.mutable_field() = field_name;
  FillByWildcardOptions(like_pattern, *wild.mutable_options());
  return {};
}

Result BuildFtsPrefix(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      std::string_view prefix) {
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PREFIX field is not VARCHAR"};
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  auto& pf = ctx.negated ? Negate<irs::ByPrefix>(parent)
                         : AddFilter<irs::ByPrefix>(parent);
  pf.boost(ctx.boost);
  *pf.mutable_field() = field_name;
  pf.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(prefix));
  return {};
}

// ---------------------------------------------------------------------------
// Phrase sequence (`##`): variable-distance phrase composition
//
// `a ## 2 ## b` parses left-assoc as `## ( ## (a, 2), b )`. The walker
// flattens any ##-tree into (parts[], gaps[]) where
//   gaps.size() == parts.size() - 1
// and each gap is either an exact `size_t N` or an interval
// {min, max}. Gap `N` means N intervening words, which lands as a
// push_back offset of N (iresearch adds the implicit +1 for the part
// itself). Interval {lo, hi} -> push_back(part, lo + 1, hi + 1).
// ---------------------------------------------------------------------------

struct PhraseSeq {
  // Parts are stored as raw Expression pointers; resolved into irs
  // phrase parts (ByTermOptions / ByPrefixOptions / ...) at emission
  // time. The expressions are owned by the bound DuckDB tree, which
  // outlives the filter-builder pass.
  std::vector<const duckdb::Expression*> parts;
  std::vector<PhraseGap> gaps;       // length == parts.size() - 1
  std::optional<PhraseGap> pending;  // trailing `## N` with no following part
};

// Expression-level wrapper for `##`: extracts the constant Value from
// `expr` (unwrapping any TSQUERY casts) and delegates to ParsePhraseGap.
ResultOr<PhraseGap> ParsePhraseSeqGap(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  const auto* val = TryGetConstant(unwrapped);
  if (!val) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   "## gap must be a constant"};
  }
  return ParsePhraseGap(*val, "##");
}

// True iff `expr`'s return_type indicates a gap operand (bare INTEGER
// or INTEGER[]) rather than a TSQUERY part. Used to distinguish the
// RHS of a `##` binary: if INTEGER/INTEGER[], treat as gap; else as
// next phrase part.
bool IsPhraseSeqGapType(const duckdb::Expression& expr) {
  const auto id = expr.return_type.id();
  return id == duckdb::LogicalTypeId::INTEGER ||
         id == duckdb::LogicalTypeId::BIGINT ||
         id == duckdb::LogicalTypeId::SMALLINT ||
         id == duckdb::LogicalTypeId::TINYINT ||
         id == duckdb::LogicalTypeId::LIST;
}

// True iff `expr` is a ## FunctionExpression.
bool IsPhraseSeqNode(const duckdb::Expression& expr) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (unwrapped.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  return f.function.name == kTSQueryPhraseSeq;
}

Result FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq);

// Attaches `next` (a part OR a gap-bearing sub-expression) to `seq`,
// using the `pending` gap if any, defaulting to "adjacent" otherwise.
Result AttachPart(PhraseSeq& seq, const duckdb::Expression& next) {
  if (IsPhraseSeqNode(next)) {
    // Delegate to the sub-walker; it will merge its flattened parts into
    // whatever the caller wanted next.
    PhraseSeq sub;
    if (auto r = FlattenPhraseSeq(next, sub); !r.ok()) {
      return r;
    }
    if (sub.parts.empty()) {
      return {ERROR_BAD_PARAMETER, "## produced no parts"};
    }
    // Splice sub into seq with the pending gap between seq.last and sub[0].
    for (size_t i = 0; i < sub.parts.size(); ++i) {
      if (!seq.parts.empty() && i == 0) {
        seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
        seq.pending.reset();
      } else if (i > 0) {
        seq.gaps.push_back(sub.gaps[i - 1]);
      }
      seq.parts.push_back(sub.parts[i]);
    }
    seq.pending = sub.pending;
    return {};
  }
  // Leaf part.
  if (!seq.parts.empty()) {
    seq.gaps.push_back(seq.pending.value_or(PhraseGap{1, 1}));
    seq.pending.reset();
  }
  seq.parts.push_back(&next);
  return {};
}

Result FlattenPhraseSeq(const duckdb::Expression& expr, PhraseSeq& seq) {
  const auto& unwrapped = UnwrapTSQueryCast(expr);
  if (!IsPhraseSeqNode(unwrapped)) {
    // Bare leaf part. A lone INTEGER / INTEGER[] here is a user error
    // (a gap without parts around it).
    if (IsPhraseSeqGapType(unwrapped)) {
      return {ERROR_BAD_PARAMETER,
              "## gap must appear between two phrase parts"};
    }
    seq.parts.push_back(&unwrapped);
    return {};
  }
  const auto& f = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  if (f.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "## expects 2 arguments (lhs ## rhs), got ",
            f.children.size()};
  }

  // Walk left first so parts arrive in source order.
  if (auto r = FlattenPhraseSeq(*f.children[0], seq); !r.ok()) {
    return r;
  }

  // Right child is either an INTEGER / INTEGER[] gap operand or a
  // TSQUERY phrase part.
  const auto& right = *f.children[1];
  if (IsPhraseSeqGapType(right)) {
    if (seq.pending.has_value()) {
      return {ERROR_BAD_PARAMETER, "## gap must be followed by a phrase part"};
    }
    auto gap_or = ParsePhraseSeqGap(right);
    if (!gap_or) {
      return std::move(gap_or.error());
    }
    seq.pending = *gap_or;
    return {};
  }
  return AttachPart(seq, right);
}

// IN_RANGE argument structure -- shared by the standalone FromInRange
// emitter and the ## phrase-part dispatcher. Pointers reference the
// bound function's constant children; nullptr means a NULL operand
// (unbounded that side).
struct InRangeArgs {
  const duckdb::Value* min_val = nullptr;
  const duckdb::Value* max_val = nullptr;
  bool min_incl = false;
  bool max_incl = false;
};

ResultOr<InRangeArgs> ParseInRangeArgs(
  const duckdb::BoundFunctionExpression& func) {
  auto err = [](auto&&... args) {
    return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                   std::forward<decltype(args)>(args)...};
  };
  if (func.children.size() != 4) {
    return err("IN_RANGE expects 4 arguments "
               "(min, max, min_incl, max_incl), got ",
               func.children.size());
  }
  InRangeArgs out;
  for (size_t i = 0; i < 2; ++i) {
    const auto* val = TryGetConstant(*func.children[i]);
    if (!val) {
      return err("IN_RANGE bound ", i, " must be a constant");
    }
    if (!val->IsNull()) {
      (i == 0 ? out.min_val : out.max_val) = val;
    }
  }
  if (auto r = GetBoolArg(*func.children[2], "IN_RANGE min_incl", out.min_incl);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (auto r = GetBoolArg(*func.children[3], "IN_RANGE max_incl", out.max_incl);
      !r.ok()) {
    return std::unexpected<Result>{std::in_place, std::move(r)};
  }
  if (out.min_val && out.max_val &&
      out.min_val->type().id() != out.max_val->type().id()) {
    throw duckdb::InvalidInputException(
      "IN_RANGE bounds have mismatched types: %s vs %s",
      out.min_val->type().ToString(), out.max_val->type().ToString());
  }
  return out;
}

// Fills the VARCHAR-form ByRangeOptions::range from parsed IN_RANGE args.
// Shared by FromInRange (standalone VARCHAR path) and the ## phrase-part
// dispatcher (only the VARCHAR variant is meaningful inside a phrase).
void FillByRangeOptionsVarchar(const InRangeArgs& args,
                               irs::ByRangeOptions& out) {
  if (args.min_val) {
    auto sv = args.min_val->GetValue<std::string>();
    out.range.min.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.min_type =
      args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
  if (args.max_val) {
    auto sv = args.max_val->GetValue<std::string>();
    out.range.max.assign(irs::ViewCast<irs::byte_type>(std::string_view{sv}));
    out.range.max_type =
      args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
  }
}

// Parses an `any_of` / `all_of` call into (args, synthesised, min_match).
// `synthesised` owns BoundConstantExpression wrappers when the list was
// constant-folded; the caller must keep it alive for the duration of `args`
// use. `min_match` is empty for `all_of` and for `any_of` when not provided.
Result ExtractAnyAllOfArgs(
  const duckdb::BoundFunctionExpression& func, bool is_any,
  std::vector<const duckdb::Expression*>& args,
  std::vector<duckdb::unique_ptr<duckdb::Expression>>& synthesised,
  std::optional<size_t>& min_match) {
  if (func.children.empty() || func.children.size() > 2) {
    return {ERROR_BAD_PARAMETER,
            is_any ? "any_of takes ([list]) or ([list], min_match)"
                   : "all_of takes ([list])"};
  }
  if (!is_any && func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "all_of takes a single list argument"};
  }

  // DuckDB constant-folds `['a', 'b']` into a BOUND_CONSTANT holding a
  // LIST Value rather than a `list_value` function call. Support both
  // shapes: synthesised BoundConstantExpression wrappers per child Value
  // in the folded case, raw child expression pointers otherwise.
  const auto& list_expr = *func.children[0];
  if (list_expr.return_type.id() != duckdb::LogicalTypeId::LIST) {
    return {ERROR_BAD_PARAMETER, "any_of/all_of first argument must be a list"};
  }
  if (list_expr.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val = list_expr.Cast<duckdb::BoundConstantExpression>().value;
    if (val.IsNull()) {
      return {ERROR_BAD_PARAMETER, "list arg must not be NULL"};
    }
    for (const auto& child_val : duckdb::ListValue::GetChildren(val)) {
      synthesised.push_back(
        duckdb::make_uniq<duckdb::BoundConstantExpression>(child_val));
      args.push_back(synthesised.back().get());
    }
  } else if (list_expr.expression_class ==
             duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& list_fn = list_expr.Cast<duckdb::BoundFunctionExpression>();
    if (list_fn.function.name != "list_value") {
      return {ERROR_BAD_PARAMETER,
              "list arg must be a literal list (got: ", list_fn.function.name,
              ")"};
    }
    for (const auto& e : list_fn.children) {
      args.push_back(e.get());
    }
  } else {
    return {ERROR_BAD_PARAMETER, "list arg must be a literal list"};
  }

  if (func.children.size() == 2) {
    int64_t m;
    if (auto r = GetIntArg(*func.children[1], "any_of min_match", m); !r.ok()) {
      return r;
    }
    if (m < 1) {
      return {ERROR_BAD_PARAMETER, "any_of min_match must be >= 1, got ", m};
    }
    min_match = static_cast<size_t>(m);
  }

  if (args.empty()) {
    return {ERROR_BAD_PARAMETER, is_any
                                   ? "any_of requires at least one argument"
                                   : "all_of requires at least one argument"};
  }
  if (min_match.has_value() && *min_match > args.size()) {
    return {ERROR_BAD_PARAMETER, "any_of min_match (",
            *min_match,          ") exceeds number of arguments (",
            args.size(),         ")"};
  }
  return {};
}

// Emits the flattened phrase sequence as an irs::ByPhrase under `parent`.
Result EmitPhraseSeq(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const PhraseSeq& seq) {
  if (seq.parts.empty()) {
    return {ERROR_BAD_PARAMETER, "## phrase has no parts"};
  }
  if (seq.pending.has_value()) {
    return {ERROR_BAD_PARAMETER,
            "## trailing gap must be followed by a phrase part"};
  }
  if (column_info.logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "## field is not VARCHAR"};
  }
  if ((column_info.tokenizer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "## field should have Positions and Frequency features enabled"};
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(parent)
                             : AddFilter<irs::ByPhrase>(parent);
  *phrase.mutable_field() = field_name;
  phrase.boost(ctx.boost);

  auto* options = phrase.mutable_options();

  // Emit one phrase part per input, using the gap offsets. First part
  // is always at offset 0 (iresearch normalises this internally).
  for (size_t i = 0; i < seq.parts.size(); ++i) {
    const auto& part_expr_ref = UnwrapTSQueryCast(*seq.parts[i]);
    const PhraseGap gap = i > 0 ? seq.gaps[i - 1] : PhraseGap{};

    // Emission dispatches on what the part expression is.
    std::string text;
    LevenshteinArgs lev_args;  // populated only for TSQueryOp::Fuzzy
    TSQueryOp leaf_op = TSQueryOp::Unknown;
    if (part_expr_ref.expression_class ==
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      // Bare string (via implicit cast to TSQUERY) -> term part.
      const auto& val =
        part_expr_ref.Cast<duckdb::BoundConstantExpression>().value;
      if (val.IsNull() || val.type().id() != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_BAD_PARAMETER, "## part must be a VARCHAR constant"};
      }
      text = val.GetValue<std::string>();
      leaf_op = TSQueryOp::Term;
    } else if (part_expr_ref.expression_class ==
               duckdb::ExpressionClass::BOUND_FUNCTION) {
      const auto& f = part_expr_ref.Cast<duckdb::BoundFunctionExpression>();
      leaf_op = ClassifyTSQueryFunction(f.function.name);
      if (leaf_op == TSQueryOp::Term || leaf_op == TSQueryOp::Like ||
          leaf_op == TSQueryOp::Prefix) {
        if (f.children.size() != 1) {
          return {ERROR_BAD_PARAMETER, "## ", f.function.name,
                  " phrase part expects 1 argument, got ", f.children.size()};
        }
        if (auto r = GetVarcharArg(*f.children[0], "## phrase part text", text);
            !r.ok()) {
          return r;
        }
      } else if (leaf_op == TSQueryOp::Fuzzy) {
        auto args_or = ParseLevenshteinArgs(f);
        if (!args_or) {
          return std::move(args_or.error());
        }
        lev_args = std::move(*args_or);
      } else if (leaf_op == TSQueryOp::Phrase) {
        // Nested PHRASE('x y z') -> tokenise via column analyzer and
        // emit one term part per token. The FIRST token uses the
        // incoming gap (omin, omax); subsequent tokens are strictly
        // adjacent. Shared with BuildFtsPhrase via EmitPhraseTokens.
        if (f.children.empty() || f.children.size() > 2) {
          return {ERROR_BAD_PARAMETER,
                  "## PHRASE phrase part expects 1 or 2 arguments "
                  "(text[, slop]), got ",
                  f.children.size()};
        }
        std::string phrase_text;
        if (auto r =
              GetVarcharArg(*f.children[0], "## PHRASE text", phrase_text);
            !r.ok()) {
          return r;
        }
        if (auto r =
              EmitPhraseTokens(*options, ctx, column_info, phrase_text, gap);
            !r.ok()) {
          return r;
        }
        continue;  // EmitPhraseTokens pushed the parts; skip scalar dispatch
      } else if (leaf_op == TSQueryOp::AnyOf) {
        // ANY_OF as a phrase part -> ByTermsOptions slot with the
        // listed terms as alternatives at this phrase position.
        // Only `ANY_OF([list])` and `ANY_OF([list], 1)` are accepted:
        // iresearch's phrase filter ignores min_match for a
        // ByTermsOptions slot (a single position holds at most one
        // token, so min_match > 1 is unsatisfiable). ALL_OF is rejected
        // for the same reason -- both would silently degrade to
        // "match any one of these terms here" at runtime.
        std::vector<const duckdb::Expression*> sub_args;
        std::vector<duckdb::unique_ptr<duckdb::Expression>> sub_synth;
        std::optional<size_t> sub_min_match;
        if (auto r = ExtractAnyAllOfArgs(f, /*is_any=*/true, sub_args,
                                         sub_synth, sub_min_match);
            !r.ok()) {
          return r;
        }
        if (sub_min_match.has_value() && *sub_min_match != 1) {
          throw duckdb::InvalidInputException(
            "## ANY_OF phrase part requires min_match=1 (got %d); a "
            "phrase position can match only one token",
            static_cast<int>(*sub_min_match));
        }
        auto& terms_opts =
          options->push_back<irs::ByTermsOptions>(gap.offs_min, gap.offs_max);
        terms_opts.min_match = 1;
        for (const auto* arg : sub_args) {
          std::string term_text;
          if (auto r = GetVarcharArg(UnwrapTSQueryCast(*arg),
                                     "## ANY_OF phrase part term", term_text);
              !r.ok()) {
            return r;
          }
          terms_opts.terms.emplace(
            irs::ViewCast<irs::byte_type>(std::string_view{term_text}));
        }
        continue;  // pushed ByTermsOptions; skip scalar dispatch
      } else if (leaf_op == TSQueryOp::AllOf) {
        throw duckdb::InvalidInputException(
          "## ALL_OF phrase part is not supported (a phrase position "
          "can match only one token; use ANY_OF instead)");
      } else if (leaf_op == TSQueryOp::InRange) {
        // IN_RANGE as a phrase part -> ByRangeOptions slot. Only the
        // VARCHAR variant is meaningful here: phrases live on the
        // analyzed text field, so numeric / boolean ranges (which would
        // target separate fields) make no sense at a phrase position.
        auto ir_or = ParseInRangeArgs(f);
        if (!ir_or) {
          return std::move(ir_or.error());
        }
        const auto& ir_args = *ir_or;
        if ((ir_args.min_val &&
             ir_args.min_val->type().id() != duckdb::LogicalTypeId::VARCHAR) ||
            (ir_args.max_val &&
             ir_args.max_val->type().id() != duckdb::LogicalTypeId::VARCHAR)) {
          throw duckdb::InvalidInputException(
            "## IN_RANGE phrase part requires VARCHAR bounds");
        }
        FillByRangeOptionsVarchar(
          ir_args,
          options->push_back<irs::ByRangeOptions>(gap.offs_min, gap.offs_max));
        continue;  // pushed ByRangeOptions; skip scalar dispatch
      } else {
        return {ERROR_NOT_IMPLEMENTED,
                "## part type not supported yet: ", f.function.name};
      }
    } else {
      return {ERROR_NOT_IMPLEMENTED, "## part expression class: ",
              static_cast<int>(part_expr_ref.expression_class)};
    }

    // The push_back overload with (offs_min, offs_max) is the interval
    // form; (offs) is shorthand for {offs+1, offs+1}. We always use the
    // interval form to keep offs semantics consistent between exact and
    // range gaps. For i == 0 (first part), iresearch normalises offsets
    // to (0, 0) internally -- we pass (0, 0) explicitly for clarity.
    switch (leaf_op) {
      case TSQueryOp::Term:
        options->push_back<irs::ByTermOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      case TSQueryOp::Prefix:
        options->push_back<irs::ByPrefixOptions>(gap.offs_min, gap.offs_max)
          .term.assign(irs::ViewCast<irs::byte_type>(std::string_view{text}));
        break;
      case TSQueryOp::Like:
        FillByWildcardOptions(
          text, options->push_back<irs::ByWildcardOptions>(gap.offs_min,
                                                           gap.offs_max));
        break;
      case TSQueryOp::Fuzzy:
        FillByEditDistanceOptions(
          lev_args, options->push_back<irs::ByEditDistanceOptions>(
                      gap.offs_min, gap.offs_max));
        break;
      default:
        return {ERROR_NOT_IMPLEMENTED,
                "## part-kind dispatch reached unreachable branch"};
    }
  }
  return {};
}

// ---------------------------------------------------------------------------
// websearch_to_tsquery parser
//
// PG-compatible web-search syntax over a single VARCHAR input:
//   - Quoted substrings ("...") are phrases (ordered token sequence).
//   - Bare words are conjuncted (implicit AND) at the top level.
//   - The keyword `OR` (case-insensitive) between two adjacent tokens
//     forms a disjunction; OR binds tighter than implicit AND.
//   - `-` prefix on an atom means NOT (exclusion).
//   - Parens are NOT honored (per PG docs).
//
// Implementation is two-pass: a hand-rolled lexer (LexWebsearch) produces
// a flat token list, then GroupWebsearch collects OR-chained atoms into
// groups. ParseWebsearchQuery emits the resulting iresearch filter tree
// (AND of groups, each group an atom or OR of atoms), reusing
// BuildFtsPhrase / BuildFtsTokens for the leaves so tokenisation goes
// through the column's ambient analyzer consistently with the rest of
// the TSQUERY surface.
// ---------------------------------------------------------------------------

enum class WsTokKind { Word, Phrase, Or };

struct WsToken {
  WsTokKind kind;
  std::string text;
  bool negated = false;
};

std::vector<WsToken> LexWebsearch(std::string_view text) {
  std::vector<WsToken> out;
  size_t i = 0;
  const size_t n = text.size();
  while (i < n) {
    while (i < n && std::isspace(static_cast<unsigned char>(text[i]))) {
      ++i;
    }
    if (i >= n) {
      break;
    }

    bool neg = false;
    if (text[i] == '-') {
      neg = true;
      ++i;
      if (i >= n || std::isspace(static_cast<unsigned char>(text[i]))) {
        // lone '-' -- ignore
        continue;
      }
    }

    if (text[i] == '"') {
      ++i;
      size_t start = i;
      while (i < n && text[i] != '"') {
        ++i;
      }
      out.push_back(
        {WsTokKind::Phrase, std::string{text.substr(start, i - start)}, neg});
      if (i < n) {
        ++i;  // consume closing quote
      }
      continue;
    }

    size_t start = i;
    while (i < n && !std::isspace(static_cast<unsigned char>(text[i])) &&
           text[i] != '"') {
      ++i;
    }
    std::string word{text.substr(start, i - start)};
    // `OR` keyword: case-insensitive, exactly 2 chars, and only when it
    // isn't negated (the user wrote `-OR` or similar).
    if (!neg && word.size() == 2 && (word[0] == 'o' || word[0] == 'O') &&
        (word[1] == 'r' || word[1] == 'R')) {
      out.push_back({WsTokKind::Or, {}, false});
    } else {
      out.push_back({WsTokKind::Word, std::move(word), neg});
    }
  }
  return out;
}

// Groups OR-chained atoms. Stray OR tokens (at start / end / between
// other ORs) are silently dropped.
std::vector<std::vector<WsToken>> GroupWebsearch(
  const std::vector<WsToken>& tokens) {
  std::vector<std::vector<WsToken>> groups;
  size_t i = 0;
  while (i < tokens.size()) {
    if (tokens[i].kind == WsTokKind::Or) {
      ++i;
      continue;
    }
    std::vector<WsToken> group;
    group.push_back(tokens[i]);
    ++i;
    while (i + 1 < tokens.size() && tokens[i].kind == WsTokKind::Or &&
           tokens[i + 1].kind != WsTokKind::Or) {
      group.push_back(tokens[i + 1]);
      i += 2;
    }
    groups.push_back(std::move(group));
  }
  return groups;
}

Result ParseWebsearchQuery(std::string_view text,
                           const SearchColumnInfo& column_info,
                           const FilterContext& ctx,
                           irs::BooleanFilter& parent);

Result BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Expression& expr);

Result ParseWebsearchQuery(std::string_view text,
                           const SearchColumnInfo& column_info,
                           const FilterContext& ctx,
                           irs::BooleanFilter& parent) {
  const auto groups = GroupWebsearch(LexWebsearch(text));
  if (groups.empty()) {
    AddFilter<irs::Empty>(parent);
    return {};
  }

  auto emit_atom = [&](const WsToken& tok, irs::BooleanFilter& into,
                       const FilterContext& c) -> Result {
    auto ac = c;
    ac.negated = c.negated ^ tok.negated;
    if (tok.kind == WsTokKind::Phrase) {
      return BuildFtsPhrase(into, ac, column_info, tok.text);
    }
    return BuildFtsTokens(into, ac, column_info, tok.text,
                          /*require_all=*/false);
  };

  auto emit_group = [&](const std::vector<WsToken>& group,
                        irs::BooleanFilter& into,
                        const FilterContext& c) -> Result {
    if (group.size() == 1) {
      return emit_atom(group[0], into, c);
    }
    auto& or_group =
      c.negated ? Negate<irs::Or>(into) : AddFilter<irs::Or>(into);
    or_group.boost(c.boost);
    auto inner = c;
    inner.negated = false;
    inner.boost = irs::kNoBoost;
    for (const auto& tok : group) {
      if (auto r = emit_atom(tok, or_group, inner); !r.ok()) {
        return r;
      }
    }
    return {};
  };

  if (groups.size() == 1) {
    return emit_group(groups[0], parent, ctx);
  }

  auto& and_group =
    ctx.negated ? Negate<irs::And>(parent) : AddFilter<irs::And>(parent);
  and_group.boost(ctx.boost);
  auto inner = ctx;
  inner.negated = false;
  inner.boost = irs::kNoBoost;
  for (const auto& group : groups) {
    if (auto r = emit_group(group, and_group, inner); !r.ok()) {
      return r;
    }
  }
  return {};
}

// TERM(text): emit an irs::ByTerm with the raw text (no tokenisation).
Result FromTerm(irs::BooleanFilter& parent, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "TERM expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "TERM text", text); !r.ok()) {
    return r;
  }
  return BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
}

// LIKE(pattern): emit an irs::ByWildcard with SQL-LIKE wildcard
// semantics (escape `%`/`_` etc.).
Result FromLike(irs::BooleanFilter& parent, const FilterContext& ctx,
                const SearchColumnInfo& column_info,
                const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "LIKE expects 1 argument (pattern), got ",
            func.children.size()};
  }
  std::string pat;
  if (auto r = GetVarcharArg(*func.children[0], "LIKE pattern", pat); !r.ok()) {
    return r;
  }
  return BuildFtsLike(parent, ctx, column_info, pat);
}

// PREFIX(text): emit an irs::ByPrefix
Result FromPrefix(irs::BooleanFilter& parent, const FilterContext& ctx,
                  const SearchColumnInfo& column_info,
                  const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "PREFIX expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string prefix;
  if (auto r = GetVarcharArg(*func.children[0], "PREFIX text", prefix);
      !r.ok()) {
    return r;
  }
  return BuildFtsPrefix(parent, ctx, column_info, prefix);
}

// phraseto_tsquery(text): identical to a single-text PHRASE call --
// just delegate to FromPhrase, whose variadic-text loop handles the
// 1-arg case correctly.
Result FromPhraseToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                           const SearchColumnInfo& column_info,
                           const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "phraseto_tsquery expects 1 argument (text), got ",
            func.children.size()};
  }
  return FromPhrase(parent, ctx, column_info, func);
}

// TOKENIZE(text [, analyzer]): 1-arg uses the ambient (column)
// analyzer with OR semantics; 2-arg accepts only `'identity'` for v1
// (bypasses tokenisation, emits a raw ByTerm). Catalog-registered
// analyzers require snapshot access not yet plumbed through the
// filter builder.
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
  if (analyzer_name == "identity") {
    return BuildFtsTerm(parent, ctx, column_info, duckdb::Value(text));
  }
  return {ERROR_NOT_IMPLEMENTED,
          "TOKENIZE(text, analyzer): only 'identity' is supported for v1"
          " (got: ",
          analyzer_name,
          "). Use TOKENIZE(text) for the ambient (column) analyzer."};
}

// plainto_tsquery(text): tokenise via the column analyzer and AND all
// resulting tokens (PG semantics; every token must match).
Result FromPlainToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                          const SearchColumnInfo& column_info,
                          const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "plainto_tsquery expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "plainto_tsquery text", text);
      !r.ok()) {
    return r;
  }
  return BuildFtsTokens(parent, ctx, column_info, text, /*require_all=*/true);
}

// websearch_to_tsquery(text): PG-style web-search syntax (quoted
// phrases, OR keyword, leading `-` for NOT). Delegates to the
// websearch parser in this file.
Result FromWebsearchToTsquery(irs::BooleanFilter& parent,
                              const FilterContext& ctx,
                              const SearchColumnInfo& column_info,
                              const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "websearch_to_tsquery expects 1 argument (text), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r =
        GetVarcharArg(*func.children[0], "websearch_to_tsquery text", text);
      !r.ok()) {
    return r;
  }
  return ParseWebsearchQuery(text, column_info, ctx, parent);
}

// tsquery_phrase(q1, q2 [, distance]): function form of `##`. Wraps
// the args into a synthetic PhraseSeq (q1, optional gap, q2) and
// emits via the shared phrase-seq emitter.
Result FromTsqueryPhrase(irs::BooleanFilter& parent, const FilterContext& ctx,
                         const SearchColumnInfo& column_info,
                         const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() < 2 || func.children.size() > 3) {
    return {ERROR_BAD_PARAMETER,
            "tsquery_phrase expects 2 or 3 arguments "
            "(q1, q2[, distance]), got ",
            func.children.size()};
  }
  PhraseSeq seq;
  if (auto r = FlattenPhraseSeq(*func.children[0], seq); !r.ok()) {
    return r;
  }
  if (func.children.size() == 3) {
    auto gap_or = ParsePhraseSeqGap(*func.children[2]);
    if (!gap_or) {
      return std::move(gap_or.error());
    }
    seq.pending = *gap_or;
  }
  if (auto r = AttachPart(seq, *func.children[1]); !r.ok()) {
    return r;
  }
  return EmitPhraseSeq(parent, ctx, column_info, seq);
}

// to_tsquery(<lucene-string>): delegate to the iresearch Lucene parser
// (full Lucene grammar: TERM, PHRASE, AND/OR/NOT, +/-, prefix/wildcard/
// regex, ranges, boost, fuzzy, slop). Writes filters into a freshly-
// added MixedBooleanFilter using the column's analyzer for phrase
// tokenisation.
Result FromToTsquery(irs::BooleanFilter& parent, const FilterContext& ctx,
                     const SearchColumnInfo& column_info,
                     const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER,
            "to_tsquery expects 1 argument (lucene-string), got ",
            func.children.size()};
  }
  std::string text;
  if (auto r = GetVarcharArg(*func.children[0], "to_tsquery text", text);
      !r.ok()) {
    return r;
  }
  std::string field_name;
  MakeFieldName(column_info, field_name);
  search::mangling::MangleString(field_name);
  auto& mixed = ctx.negated ? Negate<irs::MixedBooleanFilter>(parent)
                            : AddFilter<irs::MixedBooleanFilter>(parent);
  mixed.boost(ctx.boost);
  sdb::ParserContext parser_ctx{mixed, field_name,
                                *ActiveTokenizer(ctx, column_info)};
  if (auto r = sdb::ParseQuery(parser_ctx, text); !r.ok()) {
    return {ERROR_BAD_PARAMETER, "to_tsquery parse error: ", r.errorMessage()};
  }
  return {};
}

// TSQUERY `&&` / `||` -- 2-operand conjunction or disjunction. `is_and`
// selects irs::And vs irs::Or. Sub-context resets boost (so a
// containing ^ doesn't double-apply) and negation (the parent already
// applied any negation when adding the And/Or to the parent).
Result FromTSQueryConjunction(irs::BooleanFilter& parent,
                              const FilterContext& ctx,
                              const SearchColumnInfo& column_info,
                              const duckdb::BoundFunctionExpression& func,
                              bool is_and) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "TSQUERY ", is_and ? "&&" : "||",
            " expects 2 operands, got ", func.children.size()};
  }
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
    if (auto r = BuildTSQuery(*group, sub_ctx, column_info, *child); !r.ok()) {
      return r;
    }
  }
  return {};
}

// TSQUERY `!!` -- prefix NOT. Flips ctx.negated and recurses; no new
// filter node is added at this level (the inner expression's emitter
// will wrap itself in irs::Not when ctx.negated is true).
Result FromTSQueryNot(irs::BooleanFilter& parent, const FilterContext& ctx,
                      const SearchColumnInfo& column_info,
                      const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 1) {
    return {ERROR_BAD_PARAMETER, "TSQUERY !! expects 1 operand, got ",
            func.children.size()};
  }
  auto neg = ctx;
  neg.negated = !ctx.negated;
  return BuildTSQuery(parent, neg, column_info, *func.children[0]);
}

// TSQUERY `^` -- boost. Multiplies the inherited ctx.boost by the
// factor (defaulting to factor when ctx.boost is the unset sentinel)
// and recurses into the inner expression.
Result FromTSQueryBoost(irs::BooleanFilter& parent, const FilterContext& ctx,
                        const SearchColumnInfo& column_info,
                        const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER,
            "TSQUERY ^ expects 2 operands (query ^ factor), got ",
            func.children.size()};
  }
  double factor_d;
  if (auto r = GetDoubleArg(*func.children[1], "boost factor", factor_d);
      !r.ok()) {
    return r;
  }
  const auto factor = static_cast<irs::score_t>(factor_d);
  if (factor < 0.0f) {
    return {ERROR_BAD_PARAMETER, "boost factor must be >= 0, got ", factor};
  }
  auto boosted = ctx;
  boosted.boost = (ctx.boost == irs::kNoBoost) ? factor : ctx.boost * factor;
  return BuildTSQuery(parent, boosted, column_info, *func.children[0]);
}

// TSQUERY `##` -- phrase sequence. Flattens the (possibly nested) ##
// tree into a (parts, gaps) sequence then emits an irs::ByPhrase via
// the shared phrase-seq emitter.
Result FromTSQueryPhraseSeq(irs::BooleanFilter& parent,
                            const FilterContext& ctx,
                            const SearchColumnInfo& column_info,
                            const duckdb::BoundFunctionExpression& func) {
  PhraseSeq seq;
  if (auto r = FlattenPhraseSeq(func, seq); !r.ok()) {
    return r;
  }
  return EmitPhraseSeq(parent, ctx, column_info, seq);
}

// ANY_OF / ALL_OF emitter. Both share the list-extraction + recursion
// shape; `is_any` selects between irs::Or (with optional min_match) and
// irs::And. Forms:
//   ANY_OF([list])
//   ANY_OF([list], min_match)
//   ALL_OF([list])
// Variadic / non-list forms are intentionally not supported (DuckDB's
// STRING_LITERAL->INTEGER cost-table preference would absorb 2-arg
// variadic bare-string calls into a (LIST, INTEGER) overload).
Result FromAnyAllOf(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::BoundFunctionExpression& func, bool is_any) {
  std::vector<const duckdb::Expression*> args;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> synthesised;
  std::optional<size_t> min_match;
  if (auto r = ExtractAnyAllOfArgs(func, is_any, args, synthesised, min_match);
      !r.ok()) {
    return r;
  }

  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  sub_ctx.negated = false;

  irs::BooleanFilter* group;
  if (is_any) {
    auto& or_group =
      ctx.negated ? Negate<irs::Or>(parent) : AddFilter<irs::Or>(parent);
    if (min_match && *min_match > 1) {
      or_group.min_match_count(*min_match);
    }
    group = &or_group;
  } else {
    group =
      ctx.negated ? &Negate<irs::And>(parent) : &AddFilter<irs::And>(parent);
  }
  group->boost(ctx.boost);
  for (const auto* arg : args) {
    if (auto r = BuildTSQuery(*group, sub_ctx, column_info, *arg); !r.ok()) {
      return r;
    }
  }
  return {};
}

// IN_RANGE(min, max, min_incl, max_incl) -- TSQUERY range constructor.
// Mirrors SQL BETWEEN with explicit inclusivity. Either bound may be
// NULL (unbounded that side). VARCHAR / BOOLEAN columns emit irs::ByRange,
// numeric columns emit irs::ByGranularRange.

Result FromInRange(irs::BooleanFilter& parent, const FilterContext& ctx,
                   const SearchColumnInfo& column_info,
                   const duckdb::BoundFunctionExpression& func) {
  auto args_or = ParseInRangeArgs(func);
  if (!args_or) {
    return std::move(args_or.error());
  }
  const auto& args = *args_or;

  // Both bounds NULL -> unbounded on both sides; matches every doc.
  if (!args.min_val && !args.max_val) {
    if (ctx.negated) {
      AddFilter<irs::Empty>(parent);
    } else {
      AddFilter<irs::All>(parent).boost(ctx.boost);
    }
    return {};
  }

  const auto col_type = column_info.logical_type.id();
  const auto* val_sample = args.min_val ? args.min_val : args.max_val;
  const auto val_type = val_sample->type().id();

  // Validate value type matches column type family.
  auto type_mismatch = [&]() {
    throw duckdb::InvalidInputException(
      "IN_RANGE bound type %s is incompatible with column type %s",
      val_sample->type().ToString(), column_info.logical_type.ToString());
  };
  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    if (val_type != duckdb::LogicalTypeId::VARCHAR) {
      type_mismatch();
    }
    if (column_info.tokenizer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      throw duckdb::InvalidInputException(
        "IN_RANGE on VARCHAR field requires identity-analyzed column");
    }
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    if (val_type != duckdb::LogicalTypeId::BOOLEAN) {
      type_mismatch();
    }
  } else if (IsNumericTypeId(col_type)) {
    if (!IsInRangeNumericValueType(val_type)) {
      type_mismatch();
    }
  } else {
    throw duckdb::InvalidInputException("IN_RANGE: unsupported column type %s",
                                        column_info.logical_type.ToString());
  }

  std::string field_name;
  MakeFieldName(column_info, field_name);
  if (auto r = MangleForType(col_type, field_name); !r.ok()) {
    return r;
  }

  if (col_type == duckdb::LogicalTypeId::VARCHAR) {
    auto& rf = ctx.negated ? Negate<irs::ByRange>(parent)
                           : AddFilter<irs::ByRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    FillByRangeOptionsVarchar(args, *rf.mutable_options());
  } else if (col_type == duckdb::LogicalTypeId::BOOLEAN) {
    auto& rf = ctx.negated ? Negate<irs::ByRange>(parent)
                           : AddFilter<irs::ByRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    auto& rng = rf.mutable_options()->range;
    if (args.min_val) {
      rng.min.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args.min_val->GetValue<bool>())));
      rng.min_type =
        args.min_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
    if (args.max_val) {
      rng.max.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(args.max_val->GetValue<bool>())));
      rng.max_type =
        args.max_incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    }
  } else {
    // Numeric. Cast each bound to the column's logical type before
    // tokenising so the indexed and queried representations match.
    auto& rf = ctx.negated ? Negate<irs::ByGranularRange>(parent)
                           : AddFilter<irs::ByGranularRange>(parent);
    *rf.mutable_field() = std::move(field_name);
    rf.boost(ctx.boost);
    auto& rng = rf.mutable_options()->range;
    auto emit_bound = [&](const duckdb::Value& v,
                          irs::ByGranularRangeOptions::terms& boundary,
                          irs::BoundType& bt, bool incl) {
      auto cast = v.DefaultCastAs(column_info.logical_type);
      irs::NumericTokenizer stream;
      ResetNumericStream(stream, col_type, cast);
      irs::SetGranularTerm(boundary, stream);
      bt = incl ? irs::BoundType::Inclusive : irs::BoundType::Exclusive;
    };
    if (args.min_val) {
      emit_bound(*args.min_val, rng.min, rng.min_type, args.min_incl);
    }
    if (args.max_val) {
      emit_bound(*args.max_val, rng.max, rng.max_type, args.max_incl);
    }
  }
  return {};
}

Result BuildTSQuery(irs::BooleanFilter& parent, const FilterContext& ctx,
                    const SearchColumnInfo& column_info,
                    const duckdb::Expression& expr) {
  // `<expr>::tokenizer('<analyzer>')` annotates a TSQUERY type with an
  // ExtensionTypeInfo modifier carrying the tokenizer name. The
  // modifier can appear on either a BOUND_CAST wrapper (cast not
  // folded) or on a BOUND_CONSTANT's value type (DuckDB constant-folds
  // `'foo'::tokenizer('name')` into a constant whose type carries the
  // modifier).
  //
  // 'identity' is special-cased to mean "no tokenisation, raw ByTerm".
  // Any other name was resolved at SQL bind time by the
  // tokenizer(<name>) parameterized-type bind function: it looked up
  // the named catalog tokenizer and stashed the live analyzer pointer
  // in the type's ExtensionTypeInfo. We just read the pointer here,
  // override ctx.tokenizer for the inner subtree, and recurse.
  {
    TokenizerModifier mod;
    const duckdb::Expression* inner_expr = nullptr;
    const duckdb::Value* inner_val = nullptr;
    if (expr.expression_class == duckdb::ExpressionClass::BOUND_CAST) {
      const auto& cast_expr = expr.Cast<duckdb::BoundCastExpression>();
      mod = TryGetTokenizerModifier(cast_expr.return_type);
      if (!mod.name.empty() && cast_expr.child) {
        inner_expr = cast_expr.child.get();
        const auto& inner = UnwrapTSQueryCast(*inner_expr);
        inner_val = TryGetConstant(inner);
      }
    } else if (expr.expression_class ==
               duckdb::ExpressionClass::BOUND_CONSTANT) {
      const auto& cv = expr.Cast<duckdb::BoundConstantExpression>().value;
      mod = TryGetTokenizerModifier(cv.type());
      if (!mod.name.empty()) {
        inner_val = &cv;
      }
    }
    if (!mod.name.empty()) {
      if (mod.name == "identity") {
        if (!inner_val || inner_val->IsNull() ||
            inner_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
          return {ERROR_NOT_IMPLEMENTED,
                  "::tokenizer('identity') currently supports only bare "
                  "VARCHAR/TSQUERY constants as the inner expression"};
        }
        return BuildFtsTerm(parent, ctx, column_info, *inner_val);
      }
      if (!mod.tokenizer) {
        return {ERROR_BAD_PARAMETER, "::tokenizer('", std::string(mod.name),
                "'): tokenizer not found in catalog"};
      }
      auto sub_ctx = ctx;
      sub_ctx.tokenizer = mod.tokenizer;
      // Folded-constant case: tokenise the bare value via the override
      // tokenizer (cannot recurse on the constant -- its type still
      // carries the modifier and would re-enter this branch).
      if (inner_val) {
        if (inner_val->IsNull() ||
            inner_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
          return {ERROR_BAD_PARAMETER,
                  "::tokenizer(<name>): inner value must be VARCHAR"};
        }
        return BuildFtsTokens(parent, sub_ctx, column_info,
                              inner_val->GetValue<std::string>(),
                              /*require_all=*/false);
      }
      // Cast-wrapped case: recurse on the unwrapped inner so a PHRASE
      // / LIKE / nested expression rebuilds under the override
      // tokenizer.
      return BuildTSQuery(parent, sub_ctx, column_info, *inner_expr);
    }
  }

  const auto& unwrapped = UnwrapTSQueryCast(expr);

  // Bare string (promoted via VARCHAR -> TSQUERY cast) -> tokenise via
  // the ambient (column) analyzer. Multi-token input composes with OR
  // (min_match=1) per the plan's "col @@ 'Quick Fox' ≡ ANY_OF(tokens)"
  // rule. Non-VARCHAR / analyzer-less paths fall back to raw ByTerm.
  if (unwrapped.expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
    const auto& val = unwrapped.Cast<duckdb::BoundConstantExpression>().value;
    if (val.IsNull()) {
      AddFilter<irs::Empty>(parent);
      return {};
    }
    if (val.type().id() == duckdb::LogicalTypeId::VARCHAR) {
      return BuildFtsTokens(parent, ctx, column_info,
                            val.GetValue<std::string>(),
                            /*require_all=*/false);
    }
    return BuildFtsTerm(parent, ctx, column_info, val);
  }

  if (unwrapped.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return {ERROR_NOT_IMPLEMENTED, "Unsupported TSQUERY expression class: ",
            static_cast<int>(unwrapped.expression_class)};
  }
  const auto& func = unwrapped.Cast<duckdb::BoundFunctionExpression>();
  const auto op = ClassifyTSQueryFunction(func.function.name);

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
      return FromPhraseToTsquery(parent, ctx, column_info, func);
    case TSQueryOp::AnyOf:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/true);
    case TSQueryOp::AllOf:
      return FromAnyAllOf(parent, ctx, column_info, func, /*is_any=*/false);
    case TSQueryOp::InRange:
      return FromInRange(parent, ctx, column_info, func);
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
      return {ERROR_NOT_IMPLEMENTED,
              "Not a TSQUERY-producing function: ", func.function.name};
  }
  SDB_UNREACHABLE();
}

Result FromTSQueryMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                        const duckdb::BoundFunctionExpression& func) {
  if (func.children.size() != 2) {
    return {ERROR_BAD_PARAMETER, "@@ has ", func.children.size(),
            " inputs but 2 expected"};
  }
  // @@ is commutative: either side can be the column. Try LHS first,
  // then the cast-stripped RHS. Matches PG `doc @@ q` / `q @@ doc`.
  const auto* left_col = TryGetColumnRef(UnwrapTSQueryCast(*func.children[0]));
  const auto* right_col = TryGetColumnRef(UnwrapTSQueryCast(*func.children[1]));
  const duckdb::BoundColumnRefExpression* col_ref = nullptr;
  const duckdb::Expression* tsquery_expr = nullptr;
  if (left_col && right_col) {
    // Both sides resolve to column refs -- only error if BOTH are
    // indexed inverted columns; otherwise prefer the indexed side.
    const auto* left_info = FindColumnInfo(ctx, *left_col);
    const auto* right_info = FindColumnInfo(ctx, *right_col);
    if (left_info && right_info) {
      return {ERROR_BAD_PARAMETER,
              "@@ has column references on both sides; disambiguate by "
              "wrapping the non-column side as a TSQUERY (e.g. ::TSQUERY "
              "cast or PHRASE/LIKE/PREFIX/LEVENSHTEIN constructor)"};
    }
    col_ref = left_info ? left_col : right_col;
    tsquery_expr = left_info ? func.children[1].get() : func.children[0].get();
  } else if (left_col) {
    col_ref = left_col;
    tsquery_expr = func.children[1].get();
  } else if (right_col) {
    col_ref = right_col;
    tsquery_expr = func.children[0].get();
  } else {
    return {ERROR_BAD_PARAMETER, "@@ must have a column reference on one side"};
  }
  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "@@ column not found in inverted index"};
  }
  return BuildTSQuery(filter, ctx, *column_info, *tsquery_expr);
}

Result FromFunctionExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.function.name;

  if (name == kTSQueryMatch) {
    return FromTSQueryMatch(filter, ctx, func);
  }
  if (name == kBoost) {
    return FromBoost(filter, ctx, func);
  }
  if (name == kTermEq) {
    if (func.children.size() != 2) {
      return {ERROR_BAD_PARAMETER, "TERM_EQ has ", func.children.size(),
              " inputs but 2 expected"};
    }
    return FromBinaryEq<false>(filter, ctx, *func.children[0],
                               *func.children[1], false);
  }
  if (name == kTermIn) {
    return FromTermIn(filter, ctx, func);
  }
  if (name == kTermLike) {
    if (func.children.size() < 2) {
      return {ERROR_BAD_PARAMETER, "TERM_LIKE has ", func.children.size(),
              " inputs but at least 2 expected"};
    }
    return FromLike<false>(filter, ctx, *func.children[0], *func.children[1]);
  }

  // sdb_term_lt / sdb_term_lte / sdb_term_gt / sdb_term_gte
  if (name == kTermLt) {
    if (func.children.size() != 2) {
      return {ERROR_BAD_PARAMETER, "TERM_LT has ", func.children.size(),
              " inputs but 2 expected"};
    }
    return FromComparison<false>(filter, ctx, *func.children[0],
                                 *func.children[1], ComparisonOp::Lt);
  }
  if (name == kTermLe) {
    if (func.children.size() != 2) {
      return {ERROR_BAD_PARAMETER, "TERM_LTE has ", func.children.size(),
              " inputs but 2 expected"};
    }
    return FromComparison<false>(filter, ctx, *func.children[0],
                                 *func.children[1], ComparisonOp::Le);
  }
  if (name == kTermGt) {
    if (func.children.size() != 2) {
      return {ERROR_BAD_PARAMETER, "TERM_GT has ", func.children.size(),
              " inputs but 2 expected"};
    }
    return FromComparison<false>(filter, ctx, *func.children[0],
                                 *func.children[1], ComparisonOp::Gt);
  }
  if (name == kTermGe) {
    if (func.children.size() != 2) {
      return {ERROR_BAD_PARAMETER, "TERM_GTE has ", func.children.size(),
              " inputs but 2 expected"};
    }
    return FromComparison<false>(filter, ctx, *func.children[0],
                                 *func.children[1], ComparisonOp::Ge);
  }

  // DuckDB turns LIKE into a BoundFunctionExpression with function.name
  // "~~" or "like_escape".  Handle it as generic LIKE.
  if (name == "~~" || name == "like_escape") {
    if (func.children.size() < 2) {
      return {ERROR_BAD_PARAMETER, "LIKE has ", func.children.size(),
              " inputs but at least 2 expected"};
    }
    char escape_char = '\\';
    if (name == "like_escape" && func.children.size() >= 3) {
      const auto* esc_val = TryGetConstant(*func.children[2]);
      if (!esc_val || esc_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
        return {ERROR_BAD_PARAMETER, "LIKE ESCAPE must be a VARCHAR constant"};
      }
      auto esc_str = esc_val->GetValue<std::string>();
      if (esc_str.size() != 1) {
        return {ERROR_BAD_PARAMETER, "LIKE ESCAPE must be a single character"};
      }
      escape_char = esc_str[0];
    }
    return FromLike<true>(filter, ctx, *func.children[0], *func.children[1],
                          escape_char);
  }

  return {ERROR_NOT_IMPLEMENTED, "Unsupported function: ", name};
}

Result FromComparisonExpression(irs::BooleanFilter& filter,
                                const FilterContext& ctx,
                                const duckdb::BoundComparisonExpression& cmp) {
  switch (cmp.type) {
    case duckdb::ExpressionType::COMPARE_EQUAL:
      return FromBinaryEq<true>(filter, ctx, *cmp.left, *cmp.right, false);
    case duckdb::ExpressionType::COMPARE_NOTEQUAL:
      return FromBinaryEq<true>(filter, ctx, *cmp.left, *cmp.right, true);
    case duckdb::ExpressionType::COMPARE_LESSTHAN:
    case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
    case duckdb::ExpressionType::COMPARE_GREATERTHAN:
    case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO: {
      auto op = GetComparisonOp(cmp.type);
      return FromComparison<true>(filter, ctx, *cmp.left, *cmp.right, op);
    }
    default:
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported comparison type: ", static_cast<int>(cmp.type)};
  }
}

Result FromOperatorExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundOperatorExpression& op_expr) {
  switch (op_expr.type) {
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
              "Unsupported operator type: ", static_cast<int>(op_expr.type)};
  }
}

}  // namespace

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr) {
  switch (expr.expression_class) {
    case duckdb::ExpressionClass::BOUND_CONJUNCTION: {
      const auto& conj = expr.Cast<duckdb::BoundConjunctionExpression>();
      if (conj.type == duckdb::ExpressionType::CONJUNCTION_AND) {
        return MakeGroup<irs::And>(filter, ctx, conj);
      }
      if (conj.type == duckdb::ExpressionType::CONJUNCTION_OR) {
        return MakeGroup<irs::Or>(filter, ctx, conj);
      }
      return {ERROR_NOT_IMPLEMENTED,
              "Unsupported conjunction type: ", static_cast<int>(conj.type)};
    }
    case duckdb::ExpressionClass::BOUND_COMPARISON:
      return FromComparisonExpression(
        filter, ctx, expr.Cast<duckdb::BoundComparisonExpression>());
    case duckdb::ExpressionClass::BOUND_OPERATOR:
      return FromOperatorExpression(
        filter, ctx, expr.Cast<duckdb::BoundOperatorExpression>());
    case duckdb::ExpressionClass::BOUND_FUNCTION:
      return FromFunctionExpression(
        filter, ctx, expr.Cast<duckdb::BoundFunctionExpression>());
    case duckdb::ExpressionClass::BOUND_BETWEEN:
      return FromBetween(filter, ctx,
                         expr.Cast<duckdb::BoundBetweenExpression>());
    default:
      return {ERROR_NOT_IMPLEMENTED, "Unsupported expression class: ",
              static_cast<int>(expr.expression_class)};
  }
}

Result ExprToFilter(irs::BooleanFilter& filter, const duckdb::Expression& expr,
                    const ColumnGetter& column_getter,
                    containers::FlatHashMap<catalog::Column::Id,
                                            SearchColumnInfo>& column_cache) {
  FilterContext ctx{
    .negated = false,
    .column_getter = column_getter,
    .column_cache = column_cache,
  };
  return FromExpression(filter, ctx, expr);
}

Result MakeSearchFilter(
  irs::And& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter) {
  containers::FlatHashMap<catalog::Column::Id, SearchColumnInfo> column_cache;
  for (const auto& expr : conjuncts) {
    auto res = ExprToFilter(root, *expr, column_getter, column_cache);
    if (!res.ok()) {
      return res;
    }
  }
  return {};
}

}  // namespace sdb::connector
