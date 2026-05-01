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

#include <duckdb/planner/expression/bound_between_expression.hpp>
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
#include <iresearch/search/ngram_similarity_filter.hpp>
#include <iresearch/search/ngram_similarity_query.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/phrase_query.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/wildcard_utils.hpp>

#include "basics/string_utils.h"
#include "catalog/mangling.h"
#include "functions/search.h"
#include "functions/string.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb_filter.hpp"

namespace sdb::connector {

// Context for filter conversion
struct FilterContext {
  bool negated = false;
  irs::score_t boost = irs::kNoBoost;
  const ColumnGetter& column_getter;
  containers::FlatHashMap<catalog::Column::Id, SearchColumnInfo>& column_cache;
};

Result FromExpression(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::Expression& expr);

namespace {

// ---------------------------------------------------------------------------
// Column resolution helpers
// ---------------------------------------------------------------------------

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
               cache_it->second.analyzer.analyzer);
    return &cache_it->second;
  }

  auto column_id = info->column_id;
  return &ctx.column_cache.emplace(column_id, std::move(info.value()))
            .first->second;
}

void MakeFieldName(const SearchColumnInfo& column, std::string& field_name) {
  basics::StrResize(field_name, sizeof(column.column_id));
  absl::big_endian::Store(field_name.data(), column.column_id);
}

// ---------------------------------------------------------------------------
// Mangling dispatch
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Value -> iresearch term helpers
// ---------------------------------------------------------------------------

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

// Sets up a ByTerm filter for a single constant value against a column.
Result SetupTermFilter(irs::ByTerm& filter, std::string& field_name,
                       const SearchColumnInfo& column_info,
                       const duckdb::Value& value) {
  SDB_ASSERT(!value.IsNull(),
             "UNKNOWN and Nulls should be handled as part of IS NULL operator. "
             "For regular filter it should be just irs::Empty!");

  auto type_id = column_info.logical_type.id();

  auto res = MangleForType(type_id, field_name);
  if (res.fail()) {
    return res;
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

// ---------------------------------------------------------------------------
// Filter tree helpers (AddFilter / Negate)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Comparison op mapping
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// AND / OR groups with De Morgan short-circuit
// ---------------------------------------------------------------------------

bool IsComparisonExpr(const duckdb::Expression& expr) {
  return expr.expression_class == duckdb::ExpressionClass::BOUND_COMPARISON &&
         GetComparisonOp(expr.type) != ComparisonOp::None;
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

// ---------------------------------------------------------------------------
// IS NULL / IS NOT NULL
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Equality (generic + sdb_term_eq)
// ---------------------------------------------------------------------------

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
        column_info->analyzer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use TERM_EQ "
              "function."};
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

// ---------------------------------------------------------------------------
// Range comparison (generic + sdb_term_*)
// ---------------------------------------------------------------------------

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
        column_info->analyzer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {
        ERROR_BAD_PARAMETER,
        "Field is not indexed by identity analyzer. Use corresponding TERM_XX "
        "comparison function."};
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

  auto res = MangleForType(type_id, field_name);
  if (res.fail()) {
    return res;
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

// ---------------------------------------------------------------------------
// BETWEEN
// ---------------------------------------------------------------------------

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

    auto res = FromComparison<true>(group, sub_ctx, *between.input,
                                    *between.lower, lower_op);
    if (res.fail()) {
      return res;
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

  auto res = FromComparison<true>(group, sub_ctx, *between.input,
                                  *between.lower, lower_op);
  if (res.fail()) {
    return res;
  }
  return FromComparison<true>(group, sub_ctx, *between.input, *between.upper,
                              upper_op);
}

// ---------------------------------------------------------------------------
// IN / NOT IN
// ---------------------------------------------------------------------------

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
        column_info->analyzer.analyzer->type() !=
          irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use TERM_IN."};
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
  auto res = MangleForType(type_id, field_name);
  if (res.fail()) {
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
              "Unsupported type for IN filter: ", static_cast<int>(type_id)};
    }
  }
  return {};
}

// ---------------------------------------------------------------------------
// LIKE (generic + sdb_term_like)
// ---------------------------------------------------------------------------

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
    if (column_info->analyzer.analyzer->type() !=
        irs::Type<irs::StringTokenizer>::id()) {
      return {ERROR_BAD_PARAMETER,
              "Field is not indexed by identity analyzer. Use TERM_LIKE."};
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

// ---------------------------------------------------------------------------
// sdb_phrase
// ---------------------------------------------------------------------------

std::optional<size_t> ReadGapValue(const duckdb::Value& value) {
  if (value.IsNull()) {
    return std::nullopt;
  }
  int64_t raw;
  switch (value.type().id()) {
    case duckdb::LogicalTypeId::TINYINT:
      raw = value.GetValue<int8_t>();
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      raw = value.GetValue<int16_t>();
      break;
    case duckdb::LogicalTypeId::INTEGER:
      raw = value.GetValue<int32_t>();
      break;
    case duckdb::LogicalTypeId::BIGINT:
      raw = value.GetValue<int64_t>();
      break;
    default:
      return std::nullopt;
  }
  if (raw < 0) {
    return std::nullopt;
  }
  return static_cast<size_t>(raw);
}

Result FromPhrase(irs::BooleanFilter& filter, const FilterContext& ctx,
                  const duckdb::BoundFunctionExpression& func) {
  // PHRASE is registered with 2 fixed VARCHAR args (plus variadic ANY
  // tail), so DuckDB's function resolver rejects anything shorter at
  // bind time before we get here.
  SDB_ASSERT(func.children.size() >= 2);
  const auto* col_ref = TryGetColumnRef(*func.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE first argument must be a column reference"};
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info) {
    return {ERROR_BAD_PARAMETER, "Column is not indexed"};
  }
  if (column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "PHRASE field is not VARCHAR"};
  }

  if ((column_info->analyzer.features &
       irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) !=
      irs::PhraseQuery<irs::FixedPhraseState>::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "PHRASE field should have Positions and Frequency features "
            "enabled"};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& phrase = ctx.negated ? Negate<irs::ByPhrase>(filter)
                             : AddFilter<irs::ByPhrase>(filter);
  phrase.boost(ctx.boost);
  *phrase.mutable_field() = field_name;
  auto* opts = phrase.mutable_options();
  const irs::TermAttr* token =
    irs::get<irs::TermAttr>(*column_info->analyzer.analyzer);

  bool has_gap = false;
  size_t gap_min = 0;
  size_t gap_max = 0;

  // A non-constant argument is an index-only restriction -- a future
  // full-scan PHRASE executor could handle it -- so we return Result
  // and let the caller roll back any partially-built phrase filter so
  // this predicate falls through to regular execution. All OTHER
  // violations below are gap-grammar errors: the PHRASE call is
  // malformed and no executor can satisfy it, so they throw with a
  // specific message rather than letting SearchStubFn surface its
  // generic "outside inverted index context" error.
  for (size_t i = 1; i < func.children.size(); ++i) {
    const auto* const_val = TryGetConstant(*func.children[i]);
    if (!const_val) {
      return {ERROR_BAD_PARAMETER, "PHRASE argument ", i,
              " must be a constant"};
    }
    switch (const_val->type().id()) {
      case duckdb::LogicalTypeId::VARCHAR: {
        auto text = const_val->GetValue<std::string>();
        column_info->analyzer.analyzer->reset(std::string_view{text});
        while (column_info->analyzer.analyzer->next()) {
          if (has_gap) {
            // First token of a new text pattern: apply pending gap.
            // push_back(offs_min, offs_max) has no implicit +1 unlike
            // push_back(offs), so add 1 to convert "N words between" to
            // "N+1 position offset".
            opts->push_back<irs::ByTermOptions>(gap_min + 1, gap_max + 1)
              .term.assign(token->value);
          } else {
            // No pending gap: first term or adjacent token within same
            // pattern.
            opts->push_back<irs::ByTermOptions>().term.assign(token->value);
          }
          has_gap = false;
        }
      } break;
      case duckdb::LogicalTypeId::LIST:
      case duckdb::LogicalTypeId::ARRAY: {
        if (opts->empty()) {
          THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                          ERR_MSG("PHRASE gap at argument ", i,
                                  " must be preceded by a text pattern"));
        }
        if (has_gap) {
          THROW_SQL_ERROR(
            ERR_CODE(ERROR_BAD_PARAMETER),
            ERR_MSG("PHRASE has consecutive gaps at argument ", i));
        }
        const auto& elements =
          const_val->type().id() == duckdb::LogicalTypeId::ARRAY
            ? duckdb::ArrayValue::GetChildren(*const_val)
            : duckdb::ListValue::GetChildren(*const_val);
        if (elements.size() != 2) {
          THROW_SQL_ERROR(
            ERR_CODE(ERROR_BAD_PARAMETER),
            ERR_MSG("PHRASE gap array at argument ", i,
                    " must have exactly 2 elements [min, max], got ",
                    elements.size()));
        }
        const auto min_val = ReadGapValue(elements[0]);
        const auto max_val = ReadGapValue(elements[1]);
        if (!min_val || !max_val) {
          THROW_SQL_ERROR(ERR_CODE(ERROR_BAD_PARAMETER),
                          ERR_MSG("PHRASE gap array at argument ", i,
                                  " elements must be non-negative integers"));
        }
        if (*min_val > *max_val) {
          THROW_SQL_ERROR(
            ERR_CODE(ERROR_BAD_PARAMETER),
            ERR_MSG("PHRASE gap array at argument ", i, " min (", *min_val,
                    ") must not exceed max (", *max_val, ")"));
        }
        gap_min = *min_val;
        gap_max = *max_val;
        has_gap = true;
      } break;
      default: {
        const auto gap = ReadGapValue(*const_val);
        if (!gap) {
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
        if (has_gap) {
          THROW_SQL_ERROR(
            ERR_CODE(ERROR_BAD_PARAMETER),
            ERR_MSG("PHRASE has consecutive gaps at argument ", i));
        }
        gap_min = gap_max = gap.value();
        has_gap = true;
      } break;
    }
  }

  if (has_gap) {
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

// ---------------------------------------------------------------------------
// sdb_boost
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// sdb_ngram_match
// ---------------------------------------------------------------------------

Result FromNgramMatch(irs::BooleanFilter& filter, const FilterContext& ctx,
                      const duckdb::BoundFunctionExpression& func) {
  const auto num_inputs = func.children.size();
  if (num_inputs < 2 || num_inputs > 3) {
    return {ERROR_BAD_PARAMETER, "NGRAM_MATCH has ", num_inputs,
            " inputs but 2 or 3 expected"};
  }

  const auto* col_ref = TryGetColumnRef(*func.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  // target (string)
  const auto* target_val = TryGetConstant(*func.children[1]);
  if (!target_val) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate target value as constant"};
  }
  if (target_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate target as VARCHAR"};
  }

  // threshold (optional, default 0.7)
  float threshold = 0.7f;
  if (num_inputs >= 3) {
    const auto* threshold_val = TryGetConstant(*func.children[2]);
    if (!threshold_val) {
      return {ERROR_BAD_PARAMETER,
              "Failed to evaluate threshold value as constant"};
    }
    threshold = static_cast<float>(threshold_val->GetValue<double>());
    if (threshold < 0.f || threshold > 1.f) {
      return {ERROR_BAD_PARAMETER,
              "NGRAM_MATCH threshold must be between 0.0 and 1.0"};
    }
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info ||
      column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "NGRAM_MATCH field is not VARCHAR"};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);

  if ((column_info->analyzer.features &
       irs::NGramSimilarityQuery::kRequiredFeatures) !=
      irs::NGramSimilarityQuery::kRequiredFeatures) {
    return {ERROR_BAD_PARAMETER,
            "NGRAM_MATCH field should have Positions and Frequency features "
            "enabled"};
  }

  auto& ngram_filter = ctx.negated ? Negate<irs::ByNGramSimilarity>(filter)
                                   : AddFilter<irs::ByNGramSimilarity>(filter);
  auto target = target_val->GetValue<std::string>();
  column_info->analyzer.analyzer->reset(std::string_view{target});
  const irs::TermAttr* token =
    irs::get<irs::TermAttr>(*column_info->analyzer.analyzer);
  search::mangling::MangleString(field_name);
  *ngram_filter.mutable_field() = field_name;
  ngram_filter.boost(ctx.boost);
  ngram_filter.mutable_options()->threshold = threshold;
  while (column_info->analyzer.analyzer->next()) {
    ngram_filter.mutable_options()->ngrams.emplace_back(token->value);
  }
  return {};
}

// ---------------------------------------------------------------------------
// sdb_levenshtein_match
// ---------------------------------------------------------------------------

Result FromLevenshteinMatch(irs::BooleanFilter& filter,
                            const FilterContext& ctx,
                            const duckdb::BoundFunctionExpression& func) {
  const auto num_inputs = func.children.size();
  if (num_inputs < 3 || num_inputs > 6) {
    return {ERROR_BAD_PARAMETER, "LEVENSHTEIN_MATCH has ", num_inputs,
            " inputs but 3 to 6 expected"};
  }

  const auto* col_ref = TryGetColumnRef(*func.children[0]);
  if (!col_ref) {
    return {ERROR_BAD_PARAMETER, "Input is not a column reference"};
  }

  // target (string)
  const auto* target_val = TryGetConstant(*func.children[1]);
  if (!target_val) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate target value as constant"};
  }
  if (target_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate target as VARCHAR"};
  }

  // distance (0-4 without transpositions, 0-3 with)
  const auto* distance_val = TryGetConstant(*func.children[2]);
  if (!distance_val) {
    return {ERROR_BAD_PARAMETER,
            "Failed to evaluate distance value as constant"};
  }
  auto distance = distance_val->GetValue<int64_t>();
  if (distance < 0 || distance > 4) {
    return {ERROR_BAD_PARAMETER,
            "LEVENSHTEIN_MATCH distance must be between 0 and 4, got ",
            distance};
  }

  // transpositions (bool, optional, default true = Damerau-Levenshtein)
  bool with_transpositions = true;
  if (num_inputs >= 4) {
    const auto* transpositions_val = TryGetConstant(*func.children[3]);
    if (!transpositions_val) {
      return {ERROR_BAD_PARAMETER,
              "Failed to evaluate transpositions value as constant"};
    }
    with_transpositions = transpositions_val->GetValue<bool>();
  }

  if (with_transpositions && distance > 3) {
    return {ERROR_BAD_PARAMETER,
            "LEVENSHTEIN_MATCH distance must be between 0 and 3 when "
            "transpositions is true, got ",
            distance};
  }

  // maxTerms (number, optional, default 64)
  size_t max_terms = 64;
  if (num_inputs >= 5) {
    const auto* max_terms_val = TryGetConstant(*func.children[4]);
    if (!max_terms_val) {
      return {ERROR_BAD_PARAMETER,
              "Failed to evaluate maxTerms value as constant"};
    }
    auto mt = max_terms_val->GetValue<int64_t>();
    if (mt < 0) {
      return {ERROR_BAD_PARAMETER, "LEVENSHTEIN_MATCH maxTerms must be >= 0"};
    }
    max_terms = static_cast<size_t>(mt);
  }

  // prefix (string, optional, default "")
  std::string prefix;
  if (num_inputs >= 6) {
    const auto* prefix_val = TryGetConstant(*func.children[5]);
    if (!prefix_val) {
      return {ERROR_BAD_PARAMETER,
              "Failed to evaluate prefix value as constant"};
    }
    if (prefix_val->type().id() != duckdb::LogicalTypeId::VARCHAR) {
      return {ERROR_BAD_PARAMETER, "Failed to evaluate prefix as VARCHAR"};
    }
    prefix = prefix_val->GetValue<std::string>();
  }

  const auto* column_info = FindColumnInfo(ctx, *col_ref);
  if (!column_info ||
      column_info->logical_type.id() != duckdb::LogicalTypeId::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LEVENSHTEIN_MATCH field is not VARCHAR"};
  }

  std::string field_name;
  MakeFieldName(*column_info, field_name);
  search::mangling::MangleString(field_name);

  auto& edit_filter = ctx.negated ? Negate<irs::ByEditDistance>(filter)
                                  : AddFilter<irs::ByEditDistance>(filter);
  edit_filter.boost(ctx.boost);
  *edit_filter.mutable_field() = field_name;
  auto* opts = edit_filter.mutable_options();
  auto target = target_val->GetValue<std::string>();
  opts->term.assign(irs::ViewCast<irs::byte_type>(std::string_view{target}));
  opts->max_distance = static_cast<uint8_t>(distance);
  opts->with_transpositions = with_transpositions;
  opts->max_terms = max_terms;
  if (!prefix.empty()) {
    opts->prefix.assign(
      irs::ViewCast<irs::byte_type>(std::string_view{prefix}));
  }
  return {};
}

// ---------------------------------------------------------------------------
// sdb_term_in (function-based IN)
// ---------------------------------------------------------------------------

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
  if (res.fail()) {
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
// BoundFunctionExpression dispatcher (sdb_* functions)
// ---------------------------------------------------------------------------

Result FromFunctionExpression(irs::BooleanFilter& filter,
                              const FilterContext& ctx,
                              const duckdb::BoundFunctionExpression& func) {
  const auto& name = func.function.name;

  if (name == kPhrase) {
    return FromPhrase(filter, ctx, func);
  }
  if (name == kBoost) {
    return FromBoost(filter, ctx, func);
  }
  if (name == kNgramMatch) {
    return FromNgramMatch(filter, ctx, func);
  }
  if (name == kLevenshteinMatch) {
    return FromLevenshteinMatch(filter, ctx, func);
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

// ---------------------------------------------------------------------------
// BoundComparisonExpression dispatcher
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// BoundOperatorExpression dispatcher (NOT, IS NULL, IN, etc.)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Top-level expression dispatcher
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ExprToFilter: wraps FromExpression with error boundary
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

Result MakeSearchFilter(
  irs::And& root,
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts,
  const ColumnGetter& column_getter) {
  containers::FlatHashMap<catalog::Column::Id, SearchColumnInfo> column_cache;
  for (const auto& expr : conjuncts) {
    auto res = ExprToFilter(root, *expr, column_getter, column_cache);
    if (res.fail()) {
      return res;
    }
  }
  return {};
}

void MakeFieldName(catalog::Column::Id column_id, std::string& field_name) {
  basics::StrResize(field_name, sizeof(column_id));
  absl::big_endian::Store(field_name.data(), column_id);
}

}  // namespace sdb::connector
