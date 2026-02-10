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
// NOLINTBEGIN
// Need this header to stay first to avoid conflict in DCHECK macros
#include "serenedb_connector.hpp"
// NOLINTEND
#include <velox/expression/ExprConstants.h>

#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/granular_range_filter.hpp>
#include <iresearch/search/range_filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/terms_filter.hpp>
#include <iresearch/search/wildcard_filter.hpp>
#include <iresearch/utils/wildcard_utils.hpp>

#include "basics/exceptions.h"
#include "catalog/mangling.h"
#include "iresearch/search/term_filter.hpp"
#include "velox/core/Expressions.h"
#include "velox/expression/Expr.h"

namespace sdb::connector::search {

// Context for filter conversion
struct VeloxFilterContext {
  bool negated = false;
  irs::score_t boost = irs::kNoBoost;
  const folly::F14FastMap<std::string, const axiom::connector::Column*>&
    columns_map;
  velox::core::ExpressionEvaluator& evaluator;

  // reusable vector for constants evaluation
  mutable velox::VectorPtr evaluation_result;
  mutable velox::RowVectorPtr evaluator_input;
};

Result FromVeloxExpression(irs::BooleanFilter& filter,
                           const VeloxFilterContext& ctx,
                           const velox::core::TypedExprPtr& expr);

namespace {

std::optional<velox::Variant> EvaluateConstant(
  const velox::core::TypedExprPtr& expr, const VeloxFilterContext& ctx) {
  auto compiled_expr = ctx.evaluator.compile(expr);
  SDB_ASSERT(compiled_expr->exprs().size() == 1);
  if (!compiled_expr->exprs()[0]->isConstantExpr()) {
    return std::nullopt;
  }
  velox::SelectivityVector rows(1);
  ctx.evaluator.evaluate(compiled_expr.get(), rows, *ctx.evaluator_input,
                         ctx.evaluation_result);
  return ctx.evaluation_result->variantAt(0);
}

velox::TypeKind ExtractFieldName(const VeloxFilterContext& ctx,
                                 const velox::core::FieldAccessTypedExpr& expr,
                                 std::string& field_name) {
  auto it = ctx.columns_map.find(expr.name());
  SDB_ENSURE(it != ctx.columns_map.end(), ERROR_BAD_PARAMETER, "Column ",
             expr.name(), " was not found");
  auto column_id = static_cast<const SereneDBColumn*>(it->second)->Id();
  basics::StrResize(field_name, sizeof(column_id));
  absl::big_endian::Store(field_name.data(), column_id);
  return static_cast<const SereneDBColumn*>(it->second)->type()->kind();
}

template<typename Func, typename... Args>
Result DispatchValue(velox::TypeKind kind, Func&& func, Args&&... args) {
  irs::bstring term_value;
  switch (kind) {
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
      return std::forward<Func>(func).template operator()<int32_t>(
        std::forward<Args>(args)...);
    case velox::TypeKind::INTEGER:
      return std::forward<Func>(func).template
      operator()<velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType>(
        std::forward<Args>(args)...);
    case velox::TypeKind::BIGINT:
      return std::forward<Func>(func).template
      operator()<velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType>(
        std::forward<Args>(args)...);
    case velox::TypeKind::DOUBLE:
    case velox::TypeKind::REAL:
    case velox::TypeKind::HUGEINT:
      return std::forward<Func>(func).template operator()<double>(
        std::forward<Args>(args)...);
    case velox::TypeKind::VARCHAR:
      return std::forward<Func>(func).template operator()<velox::StringView>(
        std::forward<Args>(args)...);
    case velox::TypeKind::BOOLEAN:
      return std::forward<Func>(func).template operator()<bool>(
        std::forward<Args>(args)...);
    default:
      return {ERROR_NOT_IMPLEMENTED, "Unsupported kind ",
              velox::TypeKindName::toName(kind), " for filter building"};
  }
  SDB_UNREACHABLE();
}

template<typename T>
void DoMangle(std::string& field_name) {
  if constexpr (std::is_same_v<T, bool>) {
    sdb::search::mangling::MangleBool(field_name);
  } else if constexpr (std::is_same_v<T, velox::StringView>) {
    sdb::search::mangling::MangleString(field_name);
  } else if constexpr (std::is_floating_point_v<T> || std::is_integral_v<T>) {
    sdb::search::mangling::MangleNumeric(field_name);
  } else {
    SDB_UNREACHABLE();
  }
}

Result SetupTermFilter(irs::ByTerm& filter, std::string& field_name,
                       velox::TypeKind kind, const velox::Variant& value) {
  SDB_ASSERT(!value.isNull(),
             "UNKNOWN and Nulls should be handled as part of IS NULL operator. "
             "For regular filter it should be just irs::Empty!");
  SDB_ASSERT(value.kind() == kind,
             "Values should have same kind as field. Analyzer should put "
             "necessary casts.");
  auto process = []<typename T>(irs::ByTerm& filter, std::string& field_name,
                                const velox::Variant& value) -> Result {
    irs::bstring term_value;
    DoMangle<T>(field_name);
    if constexpr (std::is_same_v<T, velox::StringView>) {
      irs::StringTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(value.value<velox::StringView>());
      stream.next();
      filter.mutable_options()->term.assign(token->value);
    } else if constexpr (std::is_same_v<T, bool>) {
      filter.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(
        irs::BooleanTokenizer::value(value.value<bool>())));
    } else {
      irs::NumericTokenizer stream;
      const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
      stream.reset(value.value<T>());
      stream.next();
      filter.mutable_options()->term.assign(token->value);
    }
    *filter.mutable_field() = field_name;
    return {};
  };
  return DispatchValue(kind, process, filter, field_name, value);
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

enum class ComparisonOp { KNone, KLt, KLe, KGt, KGe };

ComparisonOp GetComparisonOp(const std::string& name) {
  if (name == "lte" || name.ends_with("_lte")) {
    return ComparisonOp::KLe;
  } else if (name == "lt" || name.ends_with("_lt")) {
    return ComparisonOp::KLt;
  } else if (name == "gte" || name.ends_with("_gte")) {
    return ComparisonOp::KGe;
  } else if (name == "gt" || name.ends_with("_gt")) {
    return ComparisonOp::KGt;
  }
  return ComparisonOp::KNone;
}

ComparisonOp InvertComparisonOp(ComparisonOp op) {
  switch (op) {
    case ComparisonOp::KLe:
      return ComparisonOp::KGt;
    case ComparisonOp::KGe:
      return ComparisonOp::KLt;
    case ComparisonOp::KGt:
      return ComparisonOp::KLe;
    case ComparisonOp::KLt:
      return ComparisonOp::KGe;
    case ComparisonOp::KNone:
      return ComparisonOp::KNone;
  }
}

template<typename Filter, typename Source>
Result MakeGroup(Source& parent, const VeloxFilterContext& ctx,
                 const velox::core::CallTypedExpr* call) {
  auto sub_ctx = ctx;
  sub_ctx.boost = irs::kNoBoost;
  irs::BooleanFilter* group_root;
  if (ctx.negated && absl::c_all_of(call->inputs(), [](const auto& input) {
        auto* call =
          dynamic_cast<const velox::core::CallTypedExpr*>(input.get());
        if (!call) {
          return false;
        }
        return GetComparisonOp(call->name()) != ComparisonOp::KNone;
      })) {
    // DeMorgan`s law could be used if we negate group of comparisons. As
    // comparisons consume negation by invertion we can reduce number of NOT
    // filters!
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
  for (const auto& input : call->inputs()) {
    auto result = FromVeloxExpression(*group_root, sub_ctx, input);
    if (!result.ok()) {
      return result;
    }
  }
  return {};
}

bool IsNotExpr(const velox::core::TypedExprPtr& expr,
               const velox::core::CallTypedExpr* call,
               velox::core::ExpressionEvaluator& evaluator) {
  if (!call->name().ends_with("not")) {
    return false;
  }
  auto exprs = evaluator.compile(expr);
  if (exprs->size() != 1)
    return false;

  auto& compiled = exprs->expr(0);
  return compiled->vectorFunction() &&
         compiled->vectorFunction()->getCanonicalName() ==
           velox::exec::FunctionCanonicalName::kNot;
}

// Helper: Check if it's an equality/inequality operator
bool IsEqualityOp(const std::string& name, bool& not_equal) {
  if (name == "eq" || name.ends_with("_eq")) {
    not_equal = false;
    return true;
  }

  if (name == "neq" || name.ends_with("_neq")) {
    not_equal = true;
    return true;
  }

  return false;
}

bool IsIn(std::string_view name) { return name == "in"; }

bool IsNullEq(std::string_view name, bool& negated) {
  if (name == "isnull" || name.ends_with("_isnull")) {
    negated = false;
    return true;
  }
  if (name == "isnotnull" || name.ends_with("_isnotnull")) {
    negated = true;
    return true;
  }
  return false;
}

bool IsLike(std::string_view name) {
  return name == "like" || name.ends_with("_like");
}

Result FromVeloxBinaryEq(irs::BooleanFilter& filter,
                         const VeloxFilterContext& ctx,
                         const velox::core::CallTypedExpr* call,
                         bool not_equal) {
  if (call->inputs().size() != 2) {
    return {ERROR_NOT_IMPLEMENTED, "Equality has ", call->inputs().size(),
            " inputs but 2 expected"};
  }
  if (!call->inputs()[0]->isFieldAccessKind()) {
    return {ERROR_BAD_PARAMETER, "Left input is not field access"};
  }

  auto* left_field = static_cast<const velox::core::FieldAccessTypedExpr*>(
    call->inputs()[0].get());
  auto value = EvaluateConstant(call->inputs()[1], ctx);

  if (!value.has_value()) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate right value as constant"};
  }

  if (value.value().isNull()) {
    // foo == NULL is always false and foo != NULL is false too.
    // so we do not check negated in context.
    AddFilter<irs::Empty>(filter);
    return {};
  }

  auto& term_filter = (ctx.negated != not_equal)
                        ? Negate<irs::ByTerm>(filter)
                        : AddFilter<irs::ByTerm>(filter);

  // Set the field name
  std::string field_name;
  const auto kind = ExtractFieldName(ctx, *left_field, field_name);
  term_filter.boost(ctx.boost);
  return SetupTermFilter(term_filter, field_name, kind, value.value());
}

// Convert range comparisons to IResearch range filters
Result FromVeloxComparison(irs::BooleanFilter& filter,
                           const VeloxFilterContext& ctx,
                           const velox::core::CallTypedExpr* call,
                           ComparisonOp op) {
  if (call->inputs().size() != 2) {
    return {ERROR_NOT_IMPLEMENTED, "Comparison has ", call->inputs().size(),
            " inputs but 2 expected"};
  }

  auto field_input = call->inputs()[0];
  auto value_input = call->inputs()[1];

  // looks like we do't need normalization. Value is always second argument

  if (ctx.negated) {
    op = InvertComparisonOp(op);
  }

  // TODO(Dronplane): handle case when field access is wrapped in cast
  // e.g. b:INTEGER  < 2.5:DOUBLE will be Cast(b, DOUBLE) < 2.5
  // current implementation will just fail below.
  if (!field_input->isFieldAccessKind()) {
    return {ERROR_BAD_PARAMETER, "Input is not field access"};
  }

  auto* left_field =
    static_cast<const velox::core::FieldAccessTypedExpr*>(field_input.get());
  auto value = EvaluateConstant(value_input, ctx);

  if (!value.has_value()) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate right value as constant"};
  }

  if (value.value().isNull()) {
    // foo <=> NULL is always false
    AddFilter<irs::Empty>(filter);
    return {};
  }

  // Set the field name
  std::string field_name;
  const auto kind = ExtractFieldName(ctx, *left_field, field_name);

  auto setup_base_filter = [&](auto& filter,
                               std::string&& field_name) -> decltype(auto) {
    *filter.mutable_field() = std::move(field_name);
    filter.boost(ctx.boost);
    switch (op) {
      case ComparisonOp::KLt:
        filter.mutable_options()->range.max_type = irs::BoundType::Exclusive;
        return (filter.mutable_options()->range.max);
      case ComparisonOp::KLe:
        filter.mutable_options()->range.max_type = irs::BoundType::Inclusive;
        return (filter.mutable_options()->range.max);
      case ComparisonOp::KGt:
        filter.mutable_options()->range.min_type = irs::BoundType::Exclusive;
        return (filter.mutable_options()->range.min);
      case ComparisonOp::KGe:
        filter.mutable_options()->range.min_type = irs::BoundType::Inclusive;
        return (filter.mutable_options()->range.min);
      default:
        SDB_ASSERT(false, "Not all comparison operations implemented");
    }
    SDB_UNREACHABLE();
  };
  SDB_ASSERT(value->kind() == kind,
             "Values should have same kind as field. Analyzer should put "
             "necessary casts.");
  return DispatchValue(
    kind,
    [&]<typename T>(const velox::Variant& converted_value) -> Result {
      DoMangle<T>(field_name);
      if constexpr (std::is_same_v<T, velox::StringView>) {
        auto& range_filter = AddFilter<irs::ByRange>(filter);
        irs::StringTokenizer stream;
        const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
        stream.reset(converted_value.value<velox::StringView>());
        stream.next();
        setup_base_filter(range_filter, std::move(field_name))
          .assign(token->value);
      } else if constexpr (std::is_same_v<T, bool>) {
        auto& range_filter = AddFilter<irs::ByRange>(filter);
        setup_base_filter(range_filter, std::move(field_name))
          .assign(irs::ViewCast<irs::byte_type>(
            irs::BooleanTokenizer::value(converted_value.value<bool>())));
      } else {
        auto& range_filter = AddFilter<irs::ByGranularRange>(filter);
        irs::NumericTokenizer stream;
        stream.reset(converted_value.value<T>());
        irs::SetGranularTerm(
          setup_base_filter(range_filter, std::move(field_name)), stream);
      }
      return {};
    },
    value.value());
}

Result FromVeloxIn(irs::BooleanFilter& filter, const VeloxFilterContext& ctx,
                   const velox::core::CallTypedExpr* call) {
  if (call->inputs().size() < 2) {
    return {ERROR_NOT_IMPLEMENTED, "IN has ", call->inputs().size(),
            " inputs but at least 2 expected"};
  }

  auto field_input = call->inputs()[0];
  auto value_input = call->inputs()[1];

  if (!field_input->isFieldAccessKind()) {
    return {ERROR_BAD_PARAMETER, "Input is not field access"};
  }

  std::vector<velox::Variant> values_list;

  auto* field_typed =
    static_cast<const velox::core::FieldAccessTypedExpr*>(field_input.get());
  auto value = EvaluateConstant(value_input, ctx);
  if (!value.has_value()) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate value as constant"};
  }
  if (call->inputs().size() == 2) {
    // Case with second argument as ARRAY of values or single value.
    if (value->kind() != velox::TypeKind::ARRAY) {
      values_list.push_back(std::move(value.value()));
    }
  } else {
    // Values are just inputs after field access
    values_list.push_back(std::move(value.value()));
    for (size_t i = 2; i < call->inputs().size(); ++i) {
      auto value = EvaluateConstant(call->inputs()[i], ctx);
      if (!value.has_value()) {
        return {ERROR_BAD_PARAMETER, "Failed to evaluate value as constant"};
      }
      values_list.push_back(std::move(value.value()));
    }
  }

  // Empty IN is syntax error.
  SDB_ASSERT(!values_list.empty() || !value.value().array().empty());

  std::string field_name;
  const auto kind = ExtractFieldName(ctx, *field_typed, field_name);

  auto& terms_filter = ctx.negated ? Negate<irs::ByTerms>(filter)
                                   : AddFilter<irs::ByTerms>(filter);
  return DispatchValue(
    kind,
    []<typename T>(auto& terms_filter, auto& value_array, auto& ctx,
                   auto& field_name, velox::TypeKind kind) -> Result {
      DoMangle<T>(field_name);
      terms_filter.boost(ctx.boost);
      *terms_filter.mutable_field() = field_name;
      auto& opts = *terms_filter.mutable_options();
      for (const auto& value : value_array) {
        if constexpr (std::is_same_v<T, velox::StringView>) {
          irs::StringTokenizer stream;
          const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
          stream.reset(value.template value<velox::StringView>());
          stream.next();
          opts.terms.emplace(token->value);
        } else if constexpr (std::is_same_v<T, bool>) {
          opts.terms.emplace(irs::ViewCast<irs::byte_type>(
            irs::BooleanTokenizer::value(value.template value<bool>())));
        } else {
          irs::NumericTokenizer stream;
          const irs::TermAttr* token = irs::get<irs::TermAttr>(stream);
          stream.reset(value.template value<T>());
          stream.next();
          opts.terms.emplace(token->value);
        }
      }
      return {};
    },
    terms_filter, values_list.empty() ? value.value().array() : values_list,
    ctx, field_name, kind);

  return {};
}

Result FromVeloxIsNull(irs::BooleanFilter& filter,
                       const VeloxFilterContext& ctx,
                       const velox::core::CallTypedExpr* call)

{
  if (call->inputs().size() != 1) {
    return {ERROR_NOT_IMPLEMENTED, "IS NULL has ", call->inputs().size(),
            " inputs but 1 expected"};
  }
  if (!call->inputs()[0]->isFieldAccessKind()) {
    return {ERROR_BAD_PARAMETER, "Input is not field access"};
  }

  auto* left_field = static_cast<const velox::core::FieldAccessTypedExpr*>(
    call->inputs()[0].get());

  std::string field_name;
  ExtractFieldName(ctx, *left_field, field_name);
  sdb::search::mangling::MangleNull(field_name);
  auto& term_filter =
    ctx.negated ? Negate<irs::ByTerm>(filter) : AddFilter<irs::ByTerm>(filter);
  term_filter.boost(ctx.boost);
  *term_filter.mutable_field() = field_name;
  term_filter.mutable_options()->term.assign(
    irs::ViewCast<irs::byte_type>(irs::NullTokenizer::value_null()));
  return {};
}

Result FromVeloxLike(irs::BooleanFilter& filter, const VeloxFilterContext& ctx,
                     const velox::core::CallTypedExpr* call) {
  if (call->inputs().size() != 3 && call->inputs().size() != 2) {
    return {ERROR_NOT_IMPLEMENTED, "LIKE has ", call->inputs().size(),
            " inputs but 2 OR 3 expected"};
  }
  if (!call->inputs()[0]->isFieldAccessKind()) {
    return {ERROR_BAD_PARAMETER, "Input is not field access"};
  }

  auto value = EvaluateConstant(call->inputs()[1], ctx);
  if (!value.has_value()) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate value as constant"};
  }

  if (value->kind() != velox::TypeKind::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "Failed to evaluate value as VARCHAR"};
  }

  // We do not need to process custom escape - it is already done by analyzer

  auto* field_typed = static_cast<const velox::core::FieldAccessTypedExpr*>(
    call->inputs()[0].get());

  std::string field_name;
  auto kind = ExtractFieldName(ctx, *field_typed, field_name);
  if (kind != velox::TypeKind::VARCHAR) {
    return {ERROR_BAD_PARAMETER, "LIKE field is not VARCHAR"};
  }
  sdb::search::mangling::MangleString(field_name);
  auto& wild_filter = ctx.negated ? Negate<irs::ByWildcard>(filter)
                                  : AddFilter<irs::ByWildcard>(filter);
  wild_filter.boost(ctx.boost);
  *wild_filter.mutable_field() = field_name;
  wild_filter.mutable_options()->term.assign(irs::ViewCast<irs::byte_type>(
    static_cast<std::string_view>(value.value().value<velox::StringView>())));
  return {};
}

}  // namespace

// Recursive conversion: Handle complex expressions (AND, OR, NOT)
Result FromVeloxExpression(irs::BooleanFilter& filter,
                           const VeloxFilterContext& ctx,
                           const velox::core::TypedExprPtr& expr) {
  auto* call = dynamic_cast<const velox::core::CallTypedExpr*>(expr.get());
  if (!call) {
    return {ERROR_NOT_IMPLEMENTED, "Expression is not a call"};
  }

  // Handle NOT
  if (IsNotExpr(expr, call, ctx.evaluator)) {
    auto negated_ctx = ctx;
    negated_ctx.negated = !ctx.negated;
    SDB_ASSERT(call->inputs().size() == 1);
    return FromVeloxExpression(filter, negated_ctx, call->inputs()[0]);
  }

  bool negated;
  if (IsNullEq(call->name(), negated)) {
    VeloxFilterContext sub_ctx = ctx;
    if (negated) {
      sub_ctx.negated = !ctx.negated;
    }
    return FromVeloxIsNull(filter, sub_ctx, call);
  }

  // Handle AND
  if (call->name() == velox::expression::kAnd) {
    return MakeGroup<irs::And>(filter, ctx, call);
  }

  // Handle OR
  if (call->name() == velox::expression::kOr) {
    return MakeGroup<irs::Or>(filter, ctx, call);
  }

  // Try equality/inequality

  if (IsEqualityOp(call->name(), negated)) {
    return FromVeloxBinaryEq(filter, ctx, call, negated);
  }

  // ByRange openended.
  // BETWEEN is currently executed as conjunction of comparisons so it also goes
  // here.
  auto comparison_op = GetComparisonOp(call->name());
  if (comparison_op != ComparisonOp::KNone) {
    return FromVeloxComparison(filter, ctx, call, comparison_op);
  }

  // ByTerms
  if (IsIn(call->name())) {
    return FromVeloxIn(filter, ctx, call);
  }

  if (IsLike(call->name())) {
    return FromVeloxLike(filter, ctx, call);
  }

  return {ERROR_NOT_IMPLEMENTED, "Unsupported operator: ", call->name()};
}

Result ExprToFilter(
  irs::BooleanFilter& filter, velox::core::ExpressionEvaluator& evaluator,
  const velox::core::TypedExprPtr& expr,
  const folly::F14FastMap<std::string, const axiom::connector::Column*>&
    columns_map) {
  VeloxFilterContext ctx{.negated = false,
                         .columns_map = columns_map,
                         .evaluator = evaluator,
                         .evaluator_input = std::make_shared<velox::RowVector>(
                           evaluator.pool(), velox::ROW({}, {}), nullptr, 1,
                           std::vector<velox::VectorPtr>{})};
  try {
    return FromVeloxExpression(filter, ctx, expr);
  } catch (const velox::VeloxException& ex) {
    // intercept velox error (most likely cast errors). And let's give a try
    // without search.
    SDB_TRACE("e1f19", Logger::SEARCH,
              "Failed to build filter with velox error:", ex.what());
    return {ERROR_BAD_PARAMETER,
            "Failed to build filter with velox error:", ex.what()};
  }
}

}  // namespace sdb::connector::search
