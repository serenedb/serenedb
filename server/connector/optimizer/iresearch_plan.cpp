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

#include "connector/optimizer/iresearch_plan.h"

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>

#include <array>
#include <cmath>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/optimizer/column_lifetime_analyzer.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_parameter_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression/bound_window_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/ivf/ivf_reader.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/boolean_rules.hpp>
#include <iresearch/search/queries/hnsw_query.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/ts_query_codec.h"
#include "connector/functions/vector.h"
#include "connector/index_expression.hpp"
#include "connector/optimizer/iresearch_plan_common.hpp"
#include "connector/optimizer/ts_dict_plan.hpp"
#include "connector/scan/deferred_check.h"
#include "connector/scan/scan_bind.h"
#include "connector/search_filter_builder.hpp"
#include "connector/term_dict.h"
#include "pg/connection_context.h"
#include "pg/types/logical.h"
#include "query/config.h"
#include "search/scorer_options.h"
#include "search/store.h"

namespace sdb::optimizer {

std::optional<duckdb::TableIndex> SingleReferencedTableIndex(
  const duckdb::Expression& expr) {
  duckdb::unordered_set<duckdb::TableIndex> bindings;
  duckdb::LogicalJoin::GetExpressionBindings(expr, bindings);
  if (bindings.size() != 1) {
    return std::nullopt;
  }
  return *bindings.begin();
}

namespace {

connector::ColumnId ColumnIdByName(const connector::ScanBindData& bind_data,
                                   std::string_view name) {
  if (bind_data.view) {
    const auto& names = bind_data.view->column_names;
    const auto it = absl::c_find(names, name);
    return it == names.end()
             ? connector::kInvalidColumnId
             : static_cast<connector::ColumnId>(it - names.begin());
  }
  const auto& columns = bind_data.relation.table_entry->GetColumns();
  const duckdb::Identifier key{name};
  return columns.ColumnExists(key)
           ? connector::TableColumnId(columns.GetColumn(key))
           : connector::kInvalidColumnId;
}

bool ColumnFilterCanTake(const duckdb::Expression& conjunct) {
  return duckdb::BoundComparisonExpression::IsComparison(conjunct) ||
         conjunct.GetExpressionType() ==
           duckdb::ExpressionType::COMPARE_BETWEEN;
}

struct DeferredClaimBuilder {
  connector::DeferredClaim claim;
  std::map<connector::BindingKey, connector::DeferredColumn> columns;
  const BindingColumnId* column_id = nullptr;
  bool used_expr_getter = false;
  bool declined_parameter = false;

  connector::ColumnGetter Recording(const connector::ColumnGetter& getter) {
    return [this, &getter](const duckdb::BoundColumnRefExpression& ref)
             -> std::optional<connector::SearchColumnInfo> {
      auto info = getter(ref);
      if (info) {
        columns.insert_or_assign(
          connector::MakeBindingKey(ref.Binding()),
          connector::DeferredColumn{.column = (*column_id)(ref),
                                    .column_stored = info->column_stored});
      }
      return info;
    };
  }

  connector::ExpressionGetter Recording(
    const connector::ExpressionGetter& getter) {
    return [this, &getter](const duckdb::Expression& expr)
             -> std::optional<connector::SearchColumnInfo> {
      auto info = getter(expr);
      if (info) {
        used_expr_getter = true;
      }
      return info;
    };
  }
};

bool ClaimDeferredRange(const duckdb::unique_ptr<duckdb::Expression>& conjunct,
                        const duckdb::unique_ptr<duckdb::Expression>& shaped,
                        const connector::ColumnGetter& column_getter,
                        const connector::ExpressionGetter& expression_getter,
                        duckdb::ClientContext& context,
                        DeferredClaimBuilder& deferred) {
  if (deferred.used_expr_getter || !ColumnFilterCanTake(*shaped)) {
    return false;
  }
  std::optional<duckdb::ColumnBinding> column;
  bool one = true;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    *shaped, [&](const duckdb::BoundColumnRefExpression& ref) {
      if (!column) {
        column = ref.Binding();
      } else if (*column != ref.Binding()) {
        one = false;
      }
    });
  if (!column || !one) {
    return false;
  }
  connector::FilterScorers scorers;
  if (!connector::MakeConjunctFilter(shaped, column_getter, context,
                                     expression_getter, &scorers,
                                     connector::WideRanges::Build) ||
      deferred.used_expr_getter || !scorers.empty()) {
    return false;
  }
  const auto it = deferred.columns.find(connector::MakeBindingKey(*column));
  if (it == deferred.columns.end() || !it->second.column_stored) {
    return false;
  }
  deferred.claim.ranges.push_back(conjunct->Copy());
  return true;
}

bool ParameterShapesConjunction(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    for (const auto& operand :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      if (!operand->HasParameter()) {
        continue;
      }
      bool reads_column = false;
      duckdb::ExpressionIterator::VisitExpression<
        duckdb::BoundColumnRefExpression>(
        *operand,
        [&](const duckdb::BoundColumnRefExpression&) { reads_column = true; });
      if (!reads_column) {
        return true;
      }
    }
  }
  bool shapes = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::Expression& child) {
      shapes = shapes || ParameterShapesConjunction(child);
    });
  return shapes;
}

bool TryClaimIResearchConjunctImpl(
  irs::BooleanFilter& root,
  const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context, connector::FilterScorers* scorers,
  DeferredClaimBuilder* deferred, connector::WideRanges range_mode) {
  if (conjunct->HasParameter()) {
    if (!deferred) {
      return false;
    }
    if (ParameterShapesConjunction(*conjunct)) {
      deferred->declined_parameter = true;
      return false;
    }
    auto substituted =
      SubstituteParameters(conjunct->Copy(), /*with_values=*/false);
    if (!substituted) {
      deferred->declined_parameter = true;
      return false;
    }
    try {
      auto shaped = NormalizeClaimShape(context, std::move(substituted));
      connector::FilterScorers shaped_scorers;
      const auto column_getter = deferred->Recording(getter);
      const auto expression_getter = deferred->Recording(expr_getter);
      if (!connector::MakeConjunctFilter(
            shaped, column_getter, context, expression_getter, &shaped_scorers,
            ColumnFilterCanTake(*shaped) &&
                range_mode != connector::WideRanges::Build
              ? connector::WideRanges::DeclineAll
              : connector::WideRanges::Build) ||
          deferred->used_expr_getter) {
        if (ClaimDeferredRange(conjunct, shaped, column_getter,
                               expression_getter, context, *deferred)) {
          return true;
        }
        deferred->declined_parameter = true;
        return false;
      }
    } catch (const irs::SqlException&) {
      deferred->declined_parameter = true;
      return false;
    }
    deferred->claim.conjuncts.push_back(conjunct->Copy());
    return true;
  }
  // A declined conjunct is rolled back by dropping the node it built into:
  // a term clause is sorted into its bucket rather than appended, so there
  // is no suffix of the root to cut back to. The flatten rule folds the node
  // away once the whole request is claimed.
  const connector::ColumnGetter* column_getter = &getter;
  const connector::ExpressionGetter* expression_getter = &expr_getter;
  connector::ColumnGetter recording_columns;
  connector::ExpressionGetter recording_expressions;
  if (deferred) {
    recording_columns = deferred->Recording(getter);
    recording_expressions = deferred->Recording(expr_getter);
    column_getter = &recording_columns;
    expression_getter = &recording_expressions;
  }
  auto node = connector::MakeConjunctFilter(
    conjunct, *column_getter, context, *expression_getter, scorers,
    ColumnFilterCanTake(*conjunct) ? range_mode : connector::WideRanges::Build);
  if (!node) {
    return false;
  }
  // Nested, the wrapper hides the root's shape: one holding only negated
  // clauses reads as a node owing itself an include side.
  if (node->Transparent()) {
    node->SpliceInto(root);
  } else {
    root.Add(std::move(node), irs::Occur::Must);
  }
  return true;
}

}  // namespace

bool TryClaimIResearchConjunct(
  irs::BooleanFilter& root,
  const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context, connector::FilterScorers* scorers) {
  return TryClaimIResearchConjunctImpl(root, conjunct, getter, expr_getter,
                                       context, scorers, nullptr,
                                       connector::WideRanges::Build);
}

namespace {

irs::field_id SingleColumnKey(const connector::ScanBindData& scan,
                              connector::ColumnId col_id) {
  if (!scan.relation.IsSearchTable()) {
    return irs::field_limits::invalid();
  }
  for (const auto& key : scan.relation.ScannedIndex().constraint_keys) {
    if (catalog::IsSingleColumnKey(key) && key.columns.front() == col_id) {
      return key.field;
    }
  }
  return irs::field_limits::invalid();
}

std::optional<connector::SearchColumnInfo> TermColumnInfo(
  const connector::ScanBindData& scan, const catalog::IndexTokenizers& dicts,
  connector::ColumnId col_id) {
  auto type = scan.ColumnTypeById(col_id);
  if (type.id() == duckdb::LogicalTypeId::INVALID) {
    return std::nullopt;
  }
  const auto& config = *scan.relation.inverted_config;
  const auto term = config.TermField(col_id);
  const auto* info = config.LookupField(term).entry;
  const bool term_dict = info && info->IsTermDict();
  const auto key = term_dict && connector::term_dict::IsIndexable(type)
                     ? irs::field_limits::invalid()
                     : SingleColumnKey(scan, col_id);
  if (!term_dict) {
    if (!irs::field_limits::valid(key)) {
      return std::nullopt;
    }
    return connector::SearchColumnInfo{
      .nullable = true,
      .key_field = key,
      .logical_type = std::move(type),
      .tokenizer = {.analyzer = catalog::Tokenizer::TokenizerWrapper{
                      new irs::KeywordTokenizer(), {}}}};
  }
  auto column_info =
    MakeSearchColumnInfo(term, info, std::move(type), dicts.Acquire(term));
  if (scan.relation.Stores(col_id)) {
    column_info.text = col_id;
  }
  column_info.key_field = key;
  return column_info;
}

std::optional<connector::SearchColumnInfo> ExpressionColumnInfo(
  const catalog::InvertedIndexConfig& config,
  const catalog::IndexTokenizers& dicts, const catalog::InvertedIndexKey* term,
  irs::field_id text) {
  if (!term) {
    return std::nullopt;
  }
  auto column_info =
    MakeSearchColumnInfo(term->field_id, config.FindEntry(term->field_id),
                         term->type, dicts.Acquire(term->field_id));
  column_info.text = text;
  return column_info;
}

std::optional<connector::SearchColumnInfo> ColumnInfoOf(
  const connector::ScanBindData& scan, const catalog::IndexTokenizers& dicts,
  connector::ColumnId col_id) {
  if (!scan.relation.StoresExpression(col_id)) {
    return TermColumnInfo(scan, dicts, col_id);
  }
  const auto& config = *scan.relation.inverted_config;
  return ExpressionColumnInfo(
    config, dicts,
    config.FindExpressionKeys(config.FindKey(col_id)->normalized_expression)
      .term,
    col_id);
}

}  // namespace

bool WithSearchGetters(PlannedScan& scan, duckdb::ClientContext& context,
                       absl::FunctionRef<bool(const SearchGetters&)> fn) {
  const auto& get = scan.Get();
  const auto& bind_data = scan.Bind();
  const auto& config = *bind_data.relation.inverted_config;
  const auto dicts = bind_data.relation.ResolveTokenizers(context);
  const auto table_index = get.table_index;
  const bool table_backed = !bind_data.IsViewBacked();
  const auto& reader = *bind_data.Read(context).reader;

  irs::containers::FlatHashSet<irs::field_id> analyzed_fields;
  irs::containers::FlatHashMap<connector::ColumnId, bool> not_null_cache;

  const auto column_not_null = [&](connector::ColumnId col_id) {
    const auto [it, inserted] = not_null_cache.try_emplace(col_id, false);
    if (inserted) {
      it->second = bind_data.IsColumnNotNull(col_id);
    }
    return it->second;
  };

  const auto finish_info =
    [&](std::optional<connector::SearchColumnInfo> column_info,
        std::optional<connector::ColumnId> column) {
      if (!column_info) {
        return column_info;
      }
      column_info->column_stored = bind_data.relation.IsSearchTable();
      if (column && column_info->column_stored) {
        column_info->stats = reader.GetColumnStats(*column);
      }
      if (column && table_backed && column_not_null(*column)) {
        column_info->nullable = false;
      }
      if (column_info->tokenizer.analyzer->type() !=
          irs::Type<irs::KeywordTokenizer>::id()) {
        analyzed_fields.insert(column_info->field_id);
      }
      return column_info;
    };

  const auto index_field = [&](connector::ColumnId col_id)
    -> std::optional<connector::SearchColumnInfo> {
    if (col_id == connector::kInvalidColumnId) {
      return std::nullopt;
    }
    if (bind_data.relation.StoresExpression(col_id)) {
      return finish_info(ColumnInfoOf(bind_data, dicts, col_id), std::nullopt);
    }
    return finish_info(TermColumnInfo(bind_data, dicts, col_id), col_id);
  };

  connector::FieldSetGetter field_set = [&](std::string_view name) {
    return index_field(ColumnIdByName(bind_data, name));
  };

  connector::ColumnGetter getter =
    [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<connector::SearchColumnInfo> {
    if (PrimaryColumn(ref.Binding(), get) ==
        connector::kColumnIdentifierTableOid) {
      return connector::SearchColumnInfo{
        .layout = irs::DictLayout::Numeric,
        .logical_type = duckdb::LogicalType::BIGINT,
        .index_fields = &field_set};
    }
    return index_field(scan.IdOf(ref.Binding()));
  };

  connector::ExpressionGetter expr_getter = [&](const duckdb::Expression& expr)
    -> std::optional<connector::SearchColumnInfo> {
    if (!config.HasExpressionKeys() ||
        SingleReferencedTableIndex(expr) != table_index) {
      return std::nullopt;
    }
    const auto keys = config.FindExpressionKeys(connector::NormalizedExpression(
      expr, bind_data.RelationId(), scan.Ids(), context, &config));
    return finish_info(
      ExpressionColumnInfo(
        config, dicts, keys.term,
        keys.stored ? keys.stored->field_id : irs::field_limits::invalid()),
      std::nullopt);
  };

  const BindingColumnId column_id =
    [&](const duckdb::BoundColumnRefExpression& ref) -> connector::ColumnId {
    return scan.IdOf(ref.Binding());
  };

  return fn(SearchGetters{getter, expr_getter, analyzed_fields, column_id});
}

namespace {

bool TryFoldExpression(duckdb::ClientContext& context, duckdb::Expression& expr,
                       duckdb::Value& out) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
    out = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
    return !out.IsNull();
  }
  if (!expr.IsFoldable()) {
    return false;
  }
  if (!duckdb::ExpressionExecutor::TryEvaluateFoldable(
        context, expr, out, duckdb::NumericLimits<duckdb::idx_t>::Maximum())) {
    return false;
  }
  return !out.IsNull();
}

bool TryFoldQueryVector(duckdb::ClientContext& context,
                        duckdb::Expression& expr, size_t dim,
                        std::vector<float>& out) {
  duckdb::Value folded;
  if (!TryFoldExpression(context, expr, folded)) {
    return false;
  }
  const auto& type = folded.type();
  const bool array = type.id() == duckdb::LogicalTypeId::ARRAY;
  const bool floats = (array || type.id() == duckdb::LogicalTypeId::LIST) &&
                      (array ? duckdb::ArrayType::GetChildType(type)
                             : duckdb::ListType::GetChildType(type))
                          .id() == duckdb::LogicalTypeId::FLOAT;
  duckdb::optional<duckdb::Value> casted;
  if (!floats) {
    casted = folded.DefaultTryCastAs(
      duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, dim));
    if (!casted || casted->IsNull()) {
      return false;
    }
  }
  const auto& children = casted  ? duckdb::ArrayValue::GetChildren(*casted)
                         : array ? duckdb::ArrayValue::GetChildren(folded)
                                 : duckdb::ListValue::GetChildren(folded);
  if (children.size() != dim) {
    return false;
  }
  out.reserve(dim);
  for (const auto& child : children) {
    if (child.IsNull()) {
      return false;
    }
    out.push_back(child.GetValue<float>());
  }
  return true;
}

irs::field_id ResolveAnnTargetFieldId(const duckdb::Expression& col_arg,
                                      PlannedScan& scan,
                                      duckdb::ClientContext& client_context) {
  const auto table_index = scan.Get().table_index;
  const auto& bind_data = scan.Bind();
  if (col_arg.GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = col_arg.Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Binding().table_index == table_index) {
      if (const auto id = scan.IdOf(ref.Binding());
          id != connector::kInvalidColumnId) {
        return id;
      }
    }
  }
  if (!bind_data.relation.inverted_config->HasExpressionKeys() ||
      SingleReferencedTableIndex(col_arg) != table_index) {
    return irs::field_limits::invalid();
  }
  return bind_data.relation.inverted_config->FindFieldIdByExpression(
    connector::NormalizedExpression(col_arg, bind_data.RelationId(), scan.Ids(),
                                    client_context));
}

bool IsScorerFunctionName(std::string_view name) {
  using S = catalog::ScorerOptions;
  static const irs::containers::FlatHashSet<std::string_view> kScorerNames{
    S::Bm25::Owner::type_name(),           S::Tfidf::Owner::type_name(),
    S::LmJm::Owner::type_name(),           S::LmDirichlet::Owner::type_name(),
    S::IndriDirichlet::Owner::type_name(), S::Dfi::Owner::type_name(),
    S::RawBoost::Owner::type_name(),       S::RawTf::Owner::type_name(),
    S::RawDL::Owner::type_name(),          S::Idf::Owner::type_name(),
    S::Constant::Owner::type_name(),
  };
  return kScorerNames.contains(name);
}

struct AnnFunctionInfo {
  irs::VectorMetric metric;
  duckdb::OrderType order;
  bool is_norm;
  connector::ScoreEmit score_emit;
};

std::optional<AnnFunctionInfo> GetAnnFunctionInfo(
  const duckdb::BoundFunctionExpression& func) {
  using enum irs::VectorMetric;
  using enum connector::ScoreEmit;
  constexpr auto kAsc = duckdb::OrderType::ASCENDING;
  constexpr auto kDesc = duckdb::OrderType::DESCENDING;
  constexpr AnnFunctionInfo kL2Info{L2Sqr, kAsc, false, SqrtNeg};
  constexpr AnnFunctionInfo kL2SqrInfo{L2Sqr, kAsc, false, ZeroMinus};
  constexpr AnnFunctionInfo kL1Info{L1, kAsc, false, ZeroMinus};
  constexpr AnnFunctionInfo kCosineInfo{Cosine, kAsc, false, OneMinus};
  constexpr AnnFunctionInfo kCosineSimilarityInfo{Cosine, kDesc, false,
                                                  Identity};
  constexpr AnnFunctionInfo kIPInfo{InnerProduct, kDesc, false, Identity};
  constexpr AnnFunctionInfo kNegativeIPInfo{InnerProduct, kAsc, false, Negate};
  constexpr AnnFunctionInfo kL1NormInfo{L1, kAsc, true, ZeroMinus};
  constexpr AnnFunctionInfo kL2NormInfo{L2Sqr, kAsc, true, SqrtNeg};
  static const irs::containers::FlatHashMap<std::string_view, AnnFunctionInfo>
    kFunctions{
      {connector::kL2Distance, kL2Info},
      {connector::kL2DistanceOp, kL2Info},
      {"list_distance", kL2Info},
      {"array_distance", kL2Info},
      {connector::kL2SqrDistance, kL2SqrInfo},
      {connector::kL1Distance, kL1Info},
      {connector::kL1DistanceOp, kL1Info},
      {connector::kCosineDistance, kCosineInfo},
      {connector::kCosineDistanceOp, kCosineInfo},
      {"list_cosine_distance", kCosineInfo},
      {"array_cosine_distance", kCosineInfo},
      {connector::kCosineSimilarity, kCosineSimilarityInfo},
      {"list_cosine_similarity", kCosineSimilarityInfo},
      {"array_cosine_similarity", kCosineSimilarityInfo},
      {connector::kIP, kIPInfo},
      {"list_inner_product", kIPInfo},
      {"list_dot_product", kIPInfo},
      {"array_inner_product", kIPInfo},
      {"array_dot_product", kIPInfo},
      {connector::kNegativeIP, kNegativeIPInfo},
      {connector::kNegativeIPDistanceOp, kNegativeIPInfo},
      {"list_negative_inner_product", kNegativeIPInfo},
      {"list_negative_dot_product", kNegativeIPInfo},
      {"array_negative_inner_product", kNegativeIPInfo},
      {"array_negative_dot_product", kNegativeIPInfo},
      {connector::kL1Norm, kL1NormInfo},
      {connector::kL2Norm, kL2NormInfo},
      {"vector_norm", kL2NormInfo},
    };
  const auto it =
    kFunctions.find(func.Function().GetName().GetIdentifierName());
  if (it == kFunctions.end()) {
    return std::nullopt;
  }
  for (const auto& type : func.Function().GetArguments()) {
    if (type.id() != duckdb::LogicalTypeId::LIST &&
        type.id() != duckdb::LogicalTypeId::ARRAY) {
      return std::nullopt;
    }
  }
  return it->second;
}

bool IsScoreColumn(const ScanColumn& column) {
  return column &&
         column.scan->IdOf(column.binding) == connector::kInvertedIndexScoreId;
}

bool StrictOverNull(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION ||
      expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_BETWEEN ||
      expr.Cast<duckdb::BoundFunctionExpression>()
          .Function()
          .GetNullHandling() !=
        duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING) {
    return false;
  }
  return absl::c_any_of(
    expr.Cast<duckdb::BoundFunctionExpression>().GetChildren(),
    [](const auto& child) {
      return child->GetExpressionClass() ==
               duckdb::ExpressionClass::BOUND_CONSTANT &&
             child->template Cast<duckdb::BoundConstantExpression>()
               .GetValue()
               .IsNull();
    });
}

bool TrySetScorer(std::optional<catalog::ScorerOptions>& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name) {
  auto extracted = search::ExtractScorerFromBound(func, name);
  if (!extracted) {
    return false;
  }
  if (!scorer) {
    scorer = std::move(*extracted);
    return true;
  }
  if (*scorer == *extracted) {
    return true;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("Only one scorer function is allowed per inverted index"),
    ERR_HINT("Use UNION to combine different score functions for the same "
             "inverted index"));
}

struct CallRewrite {
  duckdb::ClientContext& context;
  duckdb::Binder& binder;
  PlanIndex index;
  irs::containers::FlatHashSet<const PlannedScan*> text_scans;
};

const duckdb::BoundColumnRefExpression* AnchorOf(
  const duckdb::BoundFunctionExpression& func) {
  const auto& children = func.GetChildren();
  if (children.empty() || children[0]->GetExpressionClass() !=
                            duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  return &children[0]->Cast<duckdb::BoundColumnRefExpression>();
}

duckdb::unique_ptr<duckdb::Expression> MakeVirtualRef(
  PlanIndex& index, PlannedScan& scan, duckdb::TableIndex anchor,
  connector::ColumnId id, const duckdb::LogicalType& type,
  std::string_view name) {
  const auto get_column = scan.Ensure(id, type, name);
  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{name}, type,
    index.Expose(anchor, scan, get_column, name, type));
}

duckdb::unique_ptr<duckdb::Expression> MakeScoreRef(PlanIndex& index,
                                                    PlannedScan& scan,
                                                    duckdb::TableIndex anchor) {
  return MakeVirtualRef(index, scan, anchor, connector::kInvertedIndexScoreId,
                        duckdb::LogicalType::FLOAT, connector::kScoreName);
}

duckdb::unique_ptr<duckdb::Expression> PushdownScorerCall(
  duckdb::BoundFunctionExpression& func, PlanIndex& index) {
  const auto* anchor = AnchorOf(func);
  if (!anchor) {
    return nullptr;
  }
  auto* scan = index.Scan(anchor->Binding().table_index);
  if (!scan) {
    return nullptr;
  }
  auto& ss = scan->Bind();
  if (ss.score.vector) {
    return nullptr;
  }
  const auto& name = func.Function().GetName().GetIdentifierName();
  if (!TrySetScorer(ss.score.text, func, name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(name, "() arguments must be constants"));
  }
  auto ref = MakeScoreRef(index, *scan, anchor->Binding().table_index);
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

duckdb::unique_ptr<duckdb::Expression> PushdownExplainCall(
  duckdb::BoundFunctionExpression& func, PlanIndex& index) {
  const auto* anchor = AnchorOf(func);
  if (!anchor) {
    return nullptr;
  }
  auto* scan = index.Scan(anchor->Binding().table_index);
  if (!scan) {
    return nullptr;
  }
  auto ref =
    MakeVirtualRef(index, *scan, anchor->Binding().table_index,
                   connector::kInvertedIndexExplainId,
                   duckdb::LogicalType::VARIANT(), connector::kExplainName);
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

const duckdb::Expression& PeelArrayToListCast(const duckdb::Expression& expr) {
  if (!duckdb::BoundCastExpression::IsCast(expr)) {
    return expr;
  }
  const auto& child = duckdb::BoundCastExpression::Child(
    expr.Cast<duckdb::BoundFunctionExpression>());
  const auto& from = child.GetReturnType();
  const auto& to = expr.GetReturnType();
  if (from.id() != duckdb::LogicalTypeId::ARRAY ||
      to.id() != duckdb::LogicalTypeId::LIST ||
      duckdb::ArrayType::GetChildType(from) !=
        duckdb::ListType::GetChildType(to)) {
    return expr;
  }
  return child;
}

duckdb::unique_ptr<duckdb::Expression> PushdownDistanceCall(
  duckdb::BoundFunctionExpression& func, const AnnFunctionInfo& info,
  CallRewrite& rewrite) {
  auto& context = rewrite.context;
  const auto [col_arg, value_arg] =
    [&] -> std::pair<duckdb::Expression*, duckdb::Expression*> {
    if (info.is_norm) {
      if (func.GetChildren().empty() || func.GetChildren()[0]->IsScalar()) {
        return {nullptr, nullptr};
      }
      return {func.GetChildren()[0].get(), nullptr};
    }
    if (func.GetChildren().size() != 2) {
      return {nullptr, nullptr};
    }
    auto& lhs = func.GetChildren()[0];
    auto& rhs = func.GetChildren()[1];
    if (!lhs->IsScalar() && rhs->IsScalar()) {
      return {lhs.get(), rhs.get()};
    }
    if (lhs->IsScalar() && !rhs->IsScalar()) {
      return {rhs.get(), lhs.get()};
    }
    return {nullptr, nullptr};
  }();
  if (!col_arg) {
    return nullptr;
  }
  const auto& column = PeelArrayToListCast(*col_arg);

  const auto anchor_ti = SingleReferencedTableIndex(column);
  if (!anchor_ti) {
    return nullptr;
  }

  auto* scan = rewrite.index.Scan(*anchor_ti);
  if (!scan) {
    return nullptr;
  }
  auto& ss = scan->Bind();
  if (ss.score.text || ss.offsets.Active() ||
      rewrite.text_scans.contains(scan)) {
    return nullptr;
  }

  const auto call_field_id = ResolveAnnTargetFieldId(column, *scan, context);
  if (!irs::field_limits::valid(call_field_id)) {
    return nullptr;
  }
  const auto* ann_info =
    ss.relation.inverted_config->FindAnn(call_field_id, info.metric);
  if (!ann_info) {
    return nullptr;
  }

  std::vector<float> call_qvec;
  std::shared_ptr<const duckdb::Expression> call_qexpr;
  if (info.is_norm) {
    call_qvec.assign(ann_info->d, 0.0f);
  } else if (!TryFoldQueryVector(context, *value_arg, ann_info->d, call_qvec)) {
    if (!value_arg->HasParameter()) {
      return nullptr;
    }
    call_qexpr = value_arg->Copy();
  }

  if (!ss.score.vector) {
    ss.score.vector = connector::VectorScorerOptions{
      .field_id = call_field_id,
      .query_vector = std::move(call_qvec),
      .query_expr = std::move(call_qexpr),
      .dims = static_cast<uint32_t>(ann_info->d),
      .metric = info.metric,
      .score_emit = info.score_emit,
      .natural_order = info.order,
      .ann_id = ann_info->index_id,
      .quant = ann_info->quant.kind,
      .nb_bits = ann_info->quant.nb_bits,
      .kind = ann_info->kind,
      .ef_construction = ann_info->ef_construction,
      .posting_size = ann_info->posting_size,
    };
    connector::RefreshVectorKnobs(*ss.score.vector, context);
    ss.score.order = info.order;
  } else {
    const auto& vs = *ss.score.vector;
    const bool same_vector =
      call_qexpr ? (vs.query_expr && vs.query_expr->Equals(*call_qexpr))
                 : (!vs.query_expr && vs.query_vector == call_qvec);
    if (vs.field_id != call_field_id || vs.metric != info.metric ||
        vs.score_emit != info.score_emit || !same_vector) {
      return nullptr;
    }
  }

  auto ref = MakeScoreRef(rewrite.index, *scan, *anchor_ti);
  const auto& want_type = func.GetReturnType();
  if (want_type.id() != duckdb::LogicalTypeId::FLOAT) {
    ref = duckdb::BoundCastExpression::AddCastToType(context, std::move(ref),
                                                     want_type);
  }
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

duckdb::unique_ptr<duckdb::Expression> PushdownOffsetsCall(
  duckdb::BoundFunctionExpression& func, PlanIndex& index,
  duckdb::ClientContext& context) {
  if (func.GetChildren().size() != 1 && func.GetChildren().size() != 2) {
    return nullptr;
  }
  if (func.GetChildren()[0]->GetExpressionClass() !=
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() first argument must be a column reference"));
  }
  const auto& col_ref =
    func.GetChildren()[0]->Cast<duckdb::BoundColumnRefExpression>();

  const auto limit = [&] -> size_t {
    constexpr size_t kDefaultOffsetsLimit = 1 << 12;
    if (func.GetChildren().size() != 2) {
      return kDefaultOffsetsLimit;
    }
    auto& arg1 = *func.GetChildren()[1];
    if (arg1.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_offsets() second argument must be an integer literal"));
    }
    const auto raw = arg1.Cast<duckdb::BoundConstantExpression>()
                       .GetValue()
                       .GetValue<int32_t>();
    if (raw < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_offsets() limit must be greater than zero or 0 for no "
                "limit"));
    }
    return raw == 0 ? std::numeric_limits<size_t>::max() : raw;
  }();

  const auto resolved = index.Column(col_ref.Binding());
  if (!resolved) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_ref.GetAlias().GetIdentifierName(),
              ") requires an inverted index scan in the same sub-query"));
  }
  auto& scan = *resolved.scan;
  auto& search_scan = scan.Bind();

  const auto col_name = [&] -> std::string_view {
    if (const auto& cids = scan.Get().GetColumnIds();
        resolved.binding.column_index.GetIndex() < cids.size()) {
      return scan.Get()
        .GetColumnName(cids[resolved.binding.column_index.GetIndex()])
        .GetIdentifierName();
    }
    return col_ref.GetAlias().GetIdentifierName();
  };

  const auto target_col_id = scan.IdOf(resolved.binding);
  if (target_col_id == connector::kInvalidColumnId) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in table"));
  }

  const auto& config = search_scan.relation.inverted_config;
  const auto* col_info = config->FindColumnInfo(target_col_id);
  if (!col_info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in index"));
  }
  const bool is_text = col_info->HasTextDictionary();
  const bool offs_stored =
    col_info->features.HasFeatures(irs::IndexFeatures::Offs);
  const auto read_field = config->TermField(target_col_id);

  if (is_text && !offs_stored) {
    auto bind = duckdb::make_uniq<connector::OffsetsBindData>();
    bind->config = config;
    bind->tokenizers = search_scan.relation.ResolveTokenizers(context);
    bind->column_id = read_field;
    bind->limit = limit;
    search_scan.offsets.requests.push_back(
      {.column_id = read_field, .display_id = target_col_id, .limit = limit});
    func.BindInfoMutable() = std::move(bind);
    func.FunctionMutable().SetFunctionCallback(connector::OffsetsScalarFn);
    auto body_expr = std::move(func.GetChildrenMutable()[0]);
    func.GetChildrenMutable().clear();
    func.GetChildrenMutable().emplace_back(
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value{std::string{}}));
    func.GetChildrenMutable().emplace_back(std::move(body_expr));
    return nullptr;
  }

  duckdb::idx_t get_column = duckdb::DConstants::INVALID_INDEX;
  const auto existing = absl::c_find_if(
    search_scan.offsets.requests,
    [&](const auto& req) { return req.column_id == read_field; });
  if (existing != search_scan.offsets.requests.end()) {
    if (existing->limit != limit) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_offsets() called multiple times for field '",
                              col_name(), "' with different limits"));
    }
    get_column = scan.GetColumnOf(existing->bind_column);
    SDB_ASSERT(get_column != duckdb::DConstants::INVALID_INDEX);
  }

  const auto col_type = connector::MakeOffsetsType();
  const auto offsets_col_name =
    connector::MakeOffsetsName(search_scan.DisplayColumnName(target_col_id));
  if (get_column == duckdb::DConstants::INVALID_INDEX) {
    const auto appended = scan.Append(connector::kInvertedIndexOffsetsId,
                                      col_type, offsets_col_name);
    get_column = appended.get_column;
    search_scan.offsets.requests.push_back({
      .column_id = read_field,
      .display_id = target_col_id,
      .limit = limit,
      .bind_column = appended.bind_column,
    });
  }
  auto out = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{offsets_col_name}, col_type,
    index.Expose(col_ref.Binding().table_index, scan, get_column,
                 offsets_col_name, col_type));
  out->SetAlias(func.GetAlias());
  return out;
}

bool PushdownStoredExpression(duckdb::unique_ptr<duckdb::Expression>& expr,
                              const duckdb::LogicalOperator& op,
                              PlanIndex& index,
                              duckdb::ClientContext& context) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF ||
      expr->IsFoldable() || !expr->IsConsistent() || expr->HasParameter() ||
      !connector::ComputedFromRowAlone(*expr)) {
    return false;
  }
  const auto table_index = SingleReferencedTableIndex(*expr);
  if (!table_index) {
    return false;
  }
  auto* scan = index.Scan(*table_index);
  if (!scan || scan->Get().table_index != *table_index) {
    return false;
  }
  auto& bind_data = scan->Bind();
  const auto& config = *bind_data.relation.inverted_config;
  if (bind_data.ts_dict.Active() || !config.StoresExpressions() ||
      index.NullPadded(op, *scan)) {
    return false;
  }
  const auto* stored = config
                         .FindExpressionKeys(connector::NormalizedExpression(
                           *expr, bind_data.RelationId(), scan->Ids(), context))
                         .stored;
  if (!stored || stored->type != expr->GetReturnType()) {
    return false;
  }
  const auto get_column =
    scan->Ensure(stored->field_id, stored->type, stored->expression_text);
  auto out = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{stored->expression_text}, stored->type,
    duckdb::ColumnBinding{*table_index, duckdb::ProjectionIndex{get_column}});
  out->SetAlias(expr->GetAlias());
  expr = std::move(out);
  return true;
}

void CollectTextCalls(const duckdb::Expression& expr, CallRewrite& rewrite) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.Function().GetName().GetIdentifierName();
    if (const auto* anchor = AnchorOf(func)) {
      if (IsScorerFunctionName(name)) {
        if (auto* scan = rewrite.index.Scan(anchor->Binding().table_index)) {
          rewrite.text_scans.insert(scan);
        }
      } else if (name == connector::kOffsets) {
        if (const auto column = rewrite.index.Column(anchor->Binding())) {
          rewrite.text_scans.insert(column.scan);
        }
      }
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr,
    [&](const duckdb::Expression& child) { CollectTextCalls(child, rewrite); });
}

void CollectTextCalls(duckdb::LogicalOperator& op, CallRewrite& rewrite) {
  for (auto& child : op.children) {
    CollectTextCalls(*child, rewrite);
  }
  switch (op.type) {
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (const auto& e : op.expressions) {
        CollectTextCalls(*e, rewrite);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (const auto& o : op.Cast<duckdb::LogicalOrder>().orders) {
        CollectTextCalls(*o.expression, rewrite);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      for (const auto& g : op.Cast<duckdb::LogicalAggregate>().groups) {
        CollectTextCalls(*g, rewrite);
      }
      for (const auto& e : op.expressions) {
        CollectTextCalls(*e, rewrite);
      }
      break;
    default:
      break;
  }
}

void RewriteCallInExpr(duckdb::unique_ptr<duckdb::Expression>& expr,
                       const duckdb::LogicalOperator& op,
                       CallRewrite& rewrite) {
  if (!expr) {
    return;
  }
  auto& index = rewrite.index;
  if (index.StoresExpressions() &&
      PushdownStoredExpression(expr, op, index, rewrite.context)) {
    return;
  }
  const auto cls = expr->GetExpressionClass();
  if (cls == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.Function().GetName().GetIdentifierName();
    if (const bool explain = name == connector::kExplain;
        explain || IsScorerFunctionName(name)) {
      if (StrictOverNull(func)) {
        expr = duckdb::make_uniq<duckdb::BoundConstantExpression>(
          duckdb::Value{func.GetReturnType()});
        return;
      }
      if (auto repl = explain ? PushdownExplainCall(func, index)
                              : PushdownScorerCall(func, index)) {
        expr = std::move(repl);
        return;
      }
      // Native scans cannot materialize the tableoid argument, so the
      // runtime stub would never be reached; raise its error here.
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG(name,
                              "() requires an inverted index scan in the same "
                              "sub-query"));
    } else if (name == connector::kOffsets) {
      if (auto repl = PushdownOffsetsCall(func, index, rewrite.context)) {
        expr = std::move(repl);
        return;
      }
    } else if (auto info = GetAnnFunctionInfo(func)) {
      if (auto repl = PushdownDistanceCall(func, *info, rewrite)) {
        expr = std::move(repl);
        return;
      }
    }
  } else if (cls == duckdb::ExpressionClass::BOUND_WINDOW) {
    const auto& window = expr->Cast<duckdb::BoundWindowExpression>();
    const auto& aggregate = window.AggregateFunction();
    if (aggregate && connector::IsTsDictFunctionName(
                       aggregate->GetName().GetIdentifierName())) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG(aggregate->GetName().GetIdentifierName(),
                "() cannot be used as a window function"),
        ERR_HINT("use it as a plain aggregate over an inverted index scan"));
    }
  } else if (cls == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    if (index.AnyScan() && !ref.GetAlias().empty() &&
        IsScoreColumn(index.Column(ref.Binding()))) {
      ref.SetAlias({});
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      RewriteCallInExpr(child, op, rewrite);
    });
}

void ReuseExistingScoreColumn(duckdb::Expression& order_expr,
                              PlanIndex& index) {
  if (order_expr.GetExpressionType() !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return;
  }
  auto& ref = order_expr.Cast<duckdb::BoundColumnRefExpression>();
  const auto* proj = index.Projection(ref.Binding().table_index);
  if (!proj) {
    return;
  }
  const auto idx = ref.Binding().column_index.GetIndex();
  if (idx >= proj->expressions.size() ||
      proj->expressions[idx]->GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return;
  }
  const auto binding =
    proj->expressions[idx]->Cast<duckdb::BoundColumnRefExpression>().Binding();
  if (!IsScoreColumn(index.Column(binding))) {
    return;
  }
  for (duckdb::idx_t j = 0; j < idx; ++j) {
    const auto& e = *proj->expressions[j];
    if (e.GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF &&
        e.Cast<duckdb::BoundColumnRefExpression>().Binding() == binding) {
      ref.BindingMutable().column_index = duckdb::ProjectionIndex{j};
      return;
    }
  }
}

void RewriteSearchCalls(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                        duckdb::unique_ptr<duckdb::LogicalOperator>& root,
                        CallRewrite& rewrite) {
  for (auto& child : plan->children) {
    RewriteSearchCalls(child, root, rewrite);
  }

  const auto& op = *plan;
  switch (plan->type) {
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (auto& e : plan->expressions) {
        RewriteCallInExpr(e, op, rewrite);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (auto& o : plan->Cast<duckdb::LogicalOrder>().orders) {
        RewriteCallInExpr(o.expression, op, rewrite);
        if (rewrite.index.AnyScan()) {
          ReuseExistingScoreColumn(*o.expression, rewrite.index);
        }
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      PushdownTsDictAggregates(plan, *root, rewrite.index, rewrite.binder,
                               rewrite.context);
      if (plan->type ==
          duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
        for (auto& g : plan->Cast<duckdb::LogicalAggregate>().groups) {
          RewriteCallInExpr(g, op, rewrite);
        }
        for (auto& e : plan->expressions) {
          RewriteCallInExpr(e, op, rewrite);
        }
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_UNNEST:
      CollapseTsDictUnnest(plan, rewrite.index);
      break;
    default:
      break;
  }
}

// Resolves the column reference on one side of a comparison, under an optional
// cast and an optional unary negation (`-(col)`). `negated` reports whether the
// negation was present -- the caller folds it into the comparison.
std::optional<duckdb::ColumnBinding> ScoreSideBinding(
  const duckdb::Expression* e, bool& negated) {
  negated = false;
  const auto strip_casts = [](const duckdb::Expression* x) {
    while (x && duckdb::BoundCastExpression::IsCast(*x)) {
      x = &duckdb::BoundCastExpression::Child(
        x->Cast<duckdb::BoundFunctionExpression>());
    }
    return x;
  };
  e = strip_casts(e);
  if (e && e->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& fn = e->Cast<duckdb::BoundFunctionExpression>();
    if (fn.Function().GetName() == "-" && fn.GetChildren().size() == 1) {
      negated = true;
      e = strip_casts(fn.GetChildren()[0].get());
    }
  }
  if (!e ||
      e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  return e->Cast<duckdb::BoundColumnRefExpression>().Binding();
}

bool TryClaimAnnRange(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  const PlannedScan& planned, duckdb::ClientContext& context) {
  auto& scan = planned.Bind();
  if (!scan.score.vector ||
      scan.score.vector->natural_order != duckdb::OrderType::ASCENDING ||
      scan.score.vector->radius != std::numeric_limits<float>::max()) {
    return false;
  }

  for (duckdb::idx_t i = 0; i < filters.size(); ++i) {
    auto& expr = *filters[i];
    if (!duckdb::BoundComparisonExpression::IsComparison(expr)) {
      continue;
    }
    auto& cmp = expr.Cast<duckdb::BoundFunctionExpression>();
    auto op = cmp.GetExpressionType();
    if (op != duckdb::ExpressionType::COMPARE_LESSTHAN &&
        op != duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO &&
        op != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
        op != duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
      continue;
    }
    auto& cmp_left = duckdb::BoundComparisonExpression::LeftMutable(cmp);
    auto& cmp_right = duckdb::BoundComparisonExpression::RightMutable(cmp);

    // Find the side that resolves to the score column (bare or `-(score)`); the
    // other side is the bound.
    const auto is_score = [&](const std::optional<duckdb::ColumnBinding>& b) {
      return b && planned.IdOf(*b) == connector::kInvertedIndexScoreId;
    };
    bool negated = false;
    bool score_on_left = true;
    duckdb::Expression* const_side = cmp_right.get();
    auto binding = ScoreSideBinding(cmp_left.get(), negated);
    if (!is_score(binding)) {
      score_on_left = false;
      const_side = cmp_left.get();
      binding = ScoreSideBinding(cmp_right.get(), negated);
      if (!is_score(binding)) {
        continue;
      }
    }

    duckdb::Value bound_value;
    if (!TryFoldExpression(context, *const_side, bound_value)) {
      continue;
    }
    const auto bound = [&] -> std::optional<float> {
      switch (bound_value.type().id()) {
        case duckdb::LogicalTypeId::FLOAT:
          return bound_value.GetValue<float>();
        case duckdb::LogicalTypeId::DOUBLE:
          return static_cast<float>(bound_value.GetValue<double>());
        default:
          return std::nullopt;
      }
    }();
    if (!bound || std::isnan(*bound)) {
      continue;
    }

    // Normalize to `score <op> radius` (score = the raw vector distance): put
    // the score on the left, then fold away a `-(score)` wrapper. A radius is
    // an upper bound on the distance; a lower bound stays a residual filter
    // (the scan evaluates it in user-facing space).
    float radius = *bound;
    if (!score_on_left) {
      op = duckdb::FlipComparisonExpression(op);
    }
    if (negated) {
      op = duckdb::FlipComparisonExpression(op);
      radius = -radius;
    }
    const bool inclusive =
      op == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
    if (op != duckdb::ExpressionType::COMPARE_LESSTHAN && !inclusive) {
      continue;
    }
    scan.score.vector->radius = radius;
    scan.score.vector->radius_inclusive = inclusive;
    filters.erase(filters.begin() + i);
    return true;
  }
  return false;
}

std::map<const duckdb::Expression*, connector::WideRanges> DecideColumnRanges(
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  const connector::ColumnGetter& getter, duckdb::ClientContext& context) {
  connector::ColumnIntervals<const duckdb::Expression*> intervals;
  for (const auto& filter : filters) {
    if (filter->HasParameter() || !ColumnFilterCanTake(*filter)) {
      continue;
    }
    const auto shaped = NormalizeClaimShape(context, filter->Copy());
    if (const auto bounds = connector::ColumnRangeOf(*shaped)) {
      connector::AddColumnRange(intervals, *bounds, getter, filter.get());
    }
  }
  std::map<const duckdb::Expression*, connector::WideRanges> modes;
  for (const auto& [key, interval] : intervals) {
    const auto fits = connector::IntervalFitsIndex(interval);
    if (!fits) {
      continue;
    }
    for (const auto* conjunct : interval.members) {
      modes.emplace(conjunct, *fits ? connector::WideRanges::Build
                                    : connector::WideRanges::DeclineAll);
    }
  }
  return modes;
}

bool ClaimSearchConjuncts(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  connector::ScanBindData& scan, const SearchGetters& getters,
  duckdb::ClientContext& context) {
  auto& [getter, expr_getter, analyzed_fields, column_id] = getters;

  auto root_and = std::make_unique<irs::BooleanFilter>();
  bool any_claimed = false;
  connector::FilterScorers filter_scorers;
  DeferredClaimBuilder deferred;
  deferred.column_id = &column_id;
  DeferredClaimBuilder* const deferred_ptr =
    scan.ts_dict.Active() || scan.offsets.Active() ? nullptr : &deferred;
  std::map<const duckdb::Expression*, connector::WideRanges> range_modes;
  if (scan.score.vector) {
    range_modes = DecideColumnRanges(filters, getter, context);
  }
  const auto default_mode = scan.score.vector
                              ? connector::WideRanges::DeclineWide
                              : connector::WideRanges::Build;
  const auto stored_row = [&](const duckdb::Expression& conjunct)
    -> std::optional<connector::RowFilter> {
    auto form = RowFormOf(conjunct);
    if (!form) {
      return std::nullopt;
    }
    connector::RowFilter row;
    for (const auto* ref : form->columns) {
      const auto info = getter(*ref);
      if (!info || !info->column_stored) {
        return std::nullopt;
      }
      row.columns.push_back(column_id(*ref));
      row.types.push_back(ref->GetReturnType());
    }
    row.filter =
      std::make_shared<duckdb::ExpressionFilter>(std::move(form->expr));
    return row;
  };
  struct Claimed {
    duckdb::unique_ptr<duckdb::Expression> conjunct;
    connector::WideRanges mode;
  };
  std::vector<Claimed> claimed;
  for (size_t i = 0; i < filters.size();) {
    const auto found = range_modes.find(filters[i].get());
    const auto mode = found != range_modes.end() ? found->second : default_mode;
    if (TryClaimIResearchConjunctImpl(*root_and, filters[i], getter,
                                      expr_getter, context, &filter_scorers,
                                      deferred_ptr, mode)) {
      any_claimed = true;
      if (!filters[i]->HasParameter()) {
        claimed.push_back({std::move(filters[i]), mode});
      }
      std::swap(filters[i], filters.back());
      filters.pop_back();
    } else {
      ++i;
    }
  }
  const bool deferring =
    !deferred.claim.conjuncts.empty() || !deferred.claim.ranges.empty();
  if (deferred.declined_parameter || (deferring && deferred.used_expr_getter)) {
    scan.plan_cache.declined_parameter = true;
  }
  if (!any_claimed) {
    return false;
  }
  if (deferring && !deferred.used_expr_getter) {
    for (auto& c : claimed) {
      deferred.claim.conjuncts.push_back(std::move(c.conjunct));
    }
    deferred.claim.columns = std::make_shared<
      const std::map<connector::BindingKey, connector::DeferredColumn>>(
      std::move(deferred.columns));
    deferred.claim.analyzed_fields.insert(analyzed_fields.begin(),
                                          analyzed_fields.end());
    scan.plan_cache.deferred = std::move(deferred.claim);
  }
  if (!deferring && (scan.KeepsColumnForms() || scan.ReadsRowsOnlySegments())) {
    std::vector<connector::RowFilter> duals;
    std::vector<Claimed> index_only;
    for (auto& c : claimed) {
      if (auto row = stored_row(*c.conjunct)) {
        duals.push_back(std::move(*row));
      } else {
        index_only.push_back(std::move(c));
      }
    }
    if (!duals.empty()) {
      auto rest = std::make_unique<irs::BooleanFilter>();
      connector::FilterScorers rest_scorers;
      for (const auto& c : index_only) {
        const bool reclaimed = TryClaimIResearchConjunctImpl(
          *rest, c.conjunct, getter, expr_getter, context, &rest_scorers,
          nullptr, c.mode);
        SDB_ENSURE(reclaimed);
      }
      if (!rest->Empty()) {
        irs::Filter::ptr filter = std::move(rest);
        connector::EnsureIncludeSides(*filter);
        irs::Optimize(filter,
                      {.scored = false, .analyzed_fields = analyzed_fields});
        scan.search.index_only = std::move(filter);
      }
      scan.search.duals = std::move(duals);
    }
  }

  const bool has_clauses = !root_and->Empty();
  irs::Filter::ptr root = std::move(root_and);
  connector::EnsureIncludeSides(*root);
  const auto scorer =
    scan.score.text ? search::MakeScorer(*scan.score.text) : nullptr;
  irs::OptimizeContext optimize{.scored = static_cast<bool>(scan.score.text),
                                .analyzed_fields = std::move(analyzed_fields),
                                .scorer = scorer.get()};
  irs::Deferral deferral{.reader = *scan.Read(context).reader};
  if (scan.DefersChecks()) {
    optimize.deferral = &deferral;
  }
  if (has_clauses) {
    irs::Optimize(root, optimize);
  }
  scan.search.filter = std::move(root);
  scan.search.deferred = connector::MakeChecks(std::move(deferral));

  scan.search.filter_scorers = std::move(filter_scorers);
  return true;
}

bool TryClaimSearchFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  PlannedScan& scan, duckdb::ClientContext& context) {
  return WithSearchGetters(scan, context, [&](const SearchGetters& getters) {
    return ClaimSearchConjuncts(filters, scan.Bind(), getters, context);
  });
}

void RewriteSearchCallsToColumnRefs(
  duckdb::OptimizerExtensionInput& input,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  CallRewrite rewrite{
    .context = input.context,
    .binder = input.optimizer.binder,
    .index = PlanIndex{*plan},
  };
  if (rewrite.index.AnyScan()) {
    CollectTextCalls(*plan, rewrite);
  }
  RewriteSearchCalls(plan, plan, rewrite);
}

bool HasRowOffsets(duckdb::LogicalOperator& op) {
  if (const auto* bind = SearchBindOf(op);
      bind && absl::c_any_of(bind->offsets.requests, [](const auto& request) {
        return request.bind_column == duckdb::DConstants::INVALID_INDEX;
      })) {
    return true;
  }
  return absl::c_any_of(
    op.children, [](const auto& child) { return HasRowOffsets(*child); });
}

void GiveOffsetsTheirFilter(duckdb::Expression& expr, PlanIndex& index) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
    if (func.Function().GetName().GetIdentifierName() == connector::kOffsets &&
        func.BindInfo() && func.GetChildren().size() == 2 &&
        func.GetChildren()[1]->GetExpressionClass() ==
          duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      auto& bind = func.BindInfoMutable()->Cast<connector::OffsetsBindData>();
      const auto column =
        index.Column(func.GetChildren()[1]
                       ->Cast<duckdb::BoundColumnRefExpression>()
                       .Binding());
      if (!bind.IsStandalone() && column) {
        bind.filter = column.scan->Bind().search.filter;
      }
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    expr,
    [&](duckdb::Expression& child) { GiveOffsetsTheirFilter(child, index); });
}

void GiveOffsetsTheirFilter(duckdb::LogicalOperator& op, PlanIndex& index) {
  for (auto& child : op.children) {
    GiveOffsetsTheirFilter(*child, index);
  }
  duckdb::LogicalOperatorVisitor::EnumerateExpressions(
    op, [&](duckdb::unique_ptr<duckdb::Expression>* expr) {
      GiveOffsetsTheirFilter(**expr, index);
    });
}

void FinishSearchScans(duckdb::OptimizerExtensionInput&,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  LimitTsDictEnumerations(*plan);
  if (HasRowOffsets(*plan)) {
    PlanIndex index{*plan};
    GiveOffsetsTheirFilter(*plan, index);
  }
}

bool StoredColumn(const duckdb::LogicalGet& get,
                  const connector::ScanBindData& bind_data,
                  const duckdb::ColumnBinding& binding) {
  const auto& column_ids = get.GetColumnIds();
  if (binding.table_index != get.table_index ||
      binding.column_index >= column_ids.size()) {
    return false;
  }
  const auto& column_index = column_ids[binding.column_index];
  if (column_index.HasChildren()) {
    return false;
  }
  const auto col_idx = column_index.GetPrimaryIndex();
  if (col_idx >= bind_data.columns.ids.size()) {
    return false;
  }
  const auto col_id = bind_data.columns.ids[col_idx];
  if (col_id > connector::kMaxRealColumnIdValue) {
    return false;
  }
  if (bind_data.relation.IsSearchTable()) {
    return true;
  }
  if (!bind_data.relation.IsInvertedIndex()) {
    return false;
  }
  const auto* info = bind_data.relation.ScannedIndex().FindColumnInfo(col_id);
  return info && info->IsStored();
}

bool CallsSearchStub(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION &&
      connector::IsSearchStub(expr.Cast<duckdb::BoundFunctionExpression>())) {
    return true;
  }
  return duckdb::ExpressionIterator::AnyChild(
    expr,
    [](const duckdb::Expression& child) { return CallsSearchStub(child); });
}

bool ColumnEvaluable(const duckdb::LogicalGet& get,
                     const connector::ScanBindData& bind_data,
                     const duckdb::Expression& expr) {
  bool stored = true;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    expr, [&](const duckdb::BoundColumnRefExpression& ref) {
      stored = stored && StoredColumn(get, bind_data, ref.Binding());
    });
  return stored && !CallsSearchStub(expr);
}

duckdb::vector<duckdb::ColumnBinding> DistinctColumns(
  const duckdb::Expression& expr) {
  duckdb::vector<duckdb::ColumnBinding> bindings;
  duckdb::ColumnLifetimeAnalyzer::ExtractColumnBindings(expr, bindings);
  duckdb::vector<duckdb::ColumnBinding> columns;
  for (const auto& b : bindings) {
    if (absl::c_find(columns, b) == columns.end()) {
      columns.push_back(b);
    }
  }
  return columns;
}

std::shared_ptr<const irs::Filter> BuildIndexLeaf(
  const duckdb::Expression& expr, const SearchGetters& getters,
  duckdb::ClientContext& context) {
  const auto copy = expr.Copy();
  connector::FilterScorers scorers;
  auto node = connector::MakeConjunctFilter(copy, getters.getter, context,
                                            getters.expr_getter, &scorers,
                                            connector::WideRanges::Build);
  if (!node || !scorers.empty()) {
    return nullptr;
  }
  irs::Filter::ptr root = std::move(node);
  connector::EnsureIncludeSides(*root);
  irs::Optimize(root,
                {.scored = false, .analyzed_fields = getters.analyzed_fields});
  return root;
}

duckdb::unique_ptr<duckdb::Expression> SplitRowFilter(
  const duckdb::Expression& expr, const duckdb::LogicalGet& get,
  const connector::ScanBindData& bind_data, const SearchGetters& getters,
  duckdb::ClientContext& context,
  std::vector<std::shared_ptr<const irs::Filter>>& leaves) {
  if (ColumnEvaluable(get, bind_data, expr)) {
    return expr.Copy();
  }
  if (auto leaf = BuildIndexLeaf(expr, getters, context)) {
    leaves.push_back(std::move(leaf));
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      duckdb::Identifier{expr.ToString()}, duckdb::LogicalType::BOOLEAN,
      leaves.size() - 1);
  }
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    return nullptr;
  }
  auto split = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
    expr.GetExpressionType());
  for (const auto& child :
       expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
    auto part =
      SplitRowFilter(*child, get, bind_data, getters, context, leaves);
    if (!part) {
      return nullptr;
    }
    split->GetChildrenMutable().push_back(std::move(part));
  }
  return split;
}

bool TakeRowFilter(const PlannedScan& scan, const SearchGetters& getters,
                   const duckdb::Expression& expr,
                   duckdb::ClientContext& context) {
  const auto& get = scan.Get();
  auto& bind_data = scan.Bind();
  std::vector<std::shared_ptr<const irs::Filter>> leaves;
  auto split = SplitRowFilter(expr, get, bind_data, getters, context, leaves);
  if (!split) {
    return false;
  }
  const auto columns = DistinctColumns(*split);
  if (columns.empty()) {
    return false;
  }
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundReferenceExpression>(
    split, [&](duckdb::BoundReferenceExpression& ref,
               duckdb::unique_ptr<duckdb::Expression>&) {
      ref.IndexMutable() += columns.size();
    });
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundColumnRefExpression>(
    split, [&](duckdb::BoundColumnRefExpression& ref,
               duckdb::unique_ptr<duckdb::Expression>& child) {
      const auto index = static_cast<duckdb::idx_t>(
        absl::c_find(columns, ref.Binding()) - columns.begin());
      child = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        ref.GetAlias(), ref.GetReturnType(), index);
    });
  connector::RowFilter row;
  for (const auto& binding : columns) {
    const auto col_idx =
      get.GetColumnIds()[binding.column_index].GetPrimaryIndex();
    row.columns.push_back(bind_data.columns.ids[col_idx]);
    row.types.push_back(bind_data.columns.types[col_idx]);
  }
  row.leaves = std::move(leaves);
  row.filter = std::make_shared<duckdb::ExpressionFilter>(std::move(split));
  bind_data.search.row_filters.push_back(std::move(row));
  return true;
}

bool TakesColumnConjunct(const PlannedScan& scan,
                         const duckdb::Expression& expr) {
  if (expr.IsVolatile() || expr.CanThrow() || expr.HasParameter() ||
      expr.HasSubquery()) {
    return false;
  }
  if (DistinctColumns(expr).size() != 1 ||
      !ColumnEvaluable(scan.Get(), scan.Bind(), expr)) {
    return true;
  }
  const auto type = expr.GetExpressionType();
  return type == duckdb::ExpressionType::CONJUNCTION_OR ||
         type == duckdb::ExpressionType::COMPARE_IN;
}

void TakeColumnConjuncts(
  PlannedScan& scan,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::ClientContext& context) {
  if (absl::c_none_of(filters, [&](const auto& e) {
        return TakesColumnConjunct(scan, *e);
      })) {
    return;
  }
  WithSearchGetters(scan, context, [&](const SearchGetters& getters) {
    for (size_t i = 0; i < filters.size();) {
      if (TakesColumnConjunct(scan, *filters[i]) &&
          TakeRowFilter(scan, getters, *filters[i], context)) {
        filters.erase(filters.begin() + i);
      } else {
        ++i;
      }
    }
    return true;
  });
}

}  // namespace

void IResearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (filters.empty()) {
    return;
  }
  auto& bind_data = bind_data_ptr->Cast<connector::ScanBindData>();
  SDB_ASSERT(&bind_data == get.bind_data.get());
  PlannedScan scan{get};
  if (bind_data.ts_dict.Active()) {
    ClaimTsDictFilter(filters, scan, context);
    return;
  }
  if (!bind_data.search.filter) {
    TryClaimAnnRange(filters, scan, context);
    if (!filters.empty()) {
      TryClaimSearchFilter(filters, scan, context);
    }
    if (bind_data.score.vector) {
      TakeColumnConjuncts(scan, filters, context);
    }
  }
  if (absl::c_any_of(filters,
                     [](const auto& e) { return e->HasParameter(); })) {
    bind_data.plan_cache.declined_parameter = true;
  }
}

namespace {

duckdb::Value ShapeValue(const duckdb::LogicalType& type) {
  if (type.IsNumeric()) {
    return duckdb::Value::Numeric(type, 1);
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return duckdb::Value::BOOLEAN(true);
    case duckdb::LogicalTypeId::VARCHAR:
      return duckdb::Value("a");
    case duckdb::LogicalTypeId::UUID:
      return duckdb::Value::UUID(duckdb::hugeint_t{1});
    case duckdb::LogicalTypeId::ENUM:
      if (duckdb::EnumType::GetSize(type) != 0) {
        return duckdb::Value::ENUM(0, type);
      }
      break;
    default:
      break;
  }
  for (const char* text : {"2000-01-01 00:00:00", "1"}) {
    if (auto out = duckdb::Value{text}.DefaultTryCastAs(type);
        out && !out->IsNull()) {
      return *out;
    }
  }
  return duckdb::Value{type};
}

bool IsPlainInteger(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::TINYINT:
    case duckdb::LogicalTypeId::SMALLINT:
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::UTINYINT:
    case duckdb::LogicalTypeId::USMALLINT:
    case duckdb::LogicalTypeId::UINTEGER:
    case duckdb::LogicalTypeId::UBIGINT:
      return true;
    default:
      return false;
  }
}

const duckdb::BoundColumnRefExpression* CastOfIntegerColumn(
  const duckdb::Expression& expr) {
  if (!duckdb::BoundCastExpression::IsCast(expr)) {
    return nullptr;
  }
  const auto& child = duckdb::BoundCastExpression::Child(
    expr.Cast<duckdb::BoundFunctionExpression>());
  if (child.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF ||
      !IsPlainInteger(child.GetReturnType()) ||
      !IsPlainInteger(expr.GetReturnType())) {
    return nullptr;
  }
  return &child.Cast<duckdb::BoundColumnRefExpression>();
}

duckdb::unique_ptr<duckdb::Expression> CompareIntegerColumn(
  const duckdb::BoundColumnRefExpression& col, duckdb::ExpressionType op,
  const duckdb::Value& value) {
  using duckdb::ExpressionType;
  const auto& type = col.GetReturnType();
  const auto compare = [&](ExpressionType with, duckdb::Value constant) {
    return duckdb::BoundComparisonExpression::Create(
      with, col.Copy(),
      duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(constant)));
  };
  if (value.IsNull()) {
    return compare(op, duckdb::Value{type});
  }
  if (auto fitted = value.DefaultTryCastAs(type); fitted && !fitted->IsNull()) {
    return compare(op, std::move(*fitted));
  }
  const bool above = value.GetValue<duckdb::hugeint_t>() > 0;
  const bool all = [&] {
    switch (op) {
      case ExpressionType::COMPARE_NOTEQUAL:
        return true;
      case ExpressionType::COMPARE_LESSTHAN:
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
        return above;
      case ExpressionType::COMPARE_GREATERTHAN:
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        return !above;
      default:
        return false;
    }
  }();
  if (all) {
    return compare(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                   duckdb::Value::MinimumValue(type));
  }
  return compare(ExpressionType::COMPARE_EQUAL, duckdb::Value{type});
}

duckdb::unique_ptr<duckdb::Expression> FoldColumnCast(
  duckdb::unique_ptr<duckdb::Expression> expr) {
  using duckdb::ExpressionType;
  if (duckdb::BoundComparisonExpression::IsComparison(*expr)) {
    const auto& cmp = expr->Cast<duckdb::BoundFunctionExpression>();
    auto op = cmp.GetExpressionType();
    switch (op) {
      case ExpressionType::COMPARE_EQUAL:
      case ExpressionType::COMPARE_NOTEQUAL:
      case ExpressionType::COMPARE_LESSTHAN:
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      case ExpressionType::COMPARE_GREATERTHAN:
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        break;
      default:
        return expr;
    }
    const auto& left = duckdb::BoundComparisonExpression::Left(cmp);
    const auto& right = duckdb::BoundComparisonExpression::Right(cmp);
    const auto* col = CastOfIntegerColumn(left);
    const auto* constant = &right;
    if (!col) {
      col = CastOfIntegerColumn(right);
      constant = &left;
      op = duckdb::FlipComparisonExpression(op);
    }
    if (!col || constant->GetExpressionClass() !=
                  duckdb::ExpressionClass::BOUND_CONSTANT) {
      return expr;
    }
    const auto& value =
      constant->Cast<duckdb::BoundConstantExpression>().GetValue();
    if (value.IsNull() || !IsPlainInteger(value.type())) {
      return expr;
    }
    return CompareIntegerColumn(*col, op, value);
  }
  if (expr->GetExpressionType() == ExpressionType::COMPARE_BETWEEN &&
      expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& between = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto* col =
      CastOfIntegerColumn(duckdb::BoundBetweenExpression::Input(between));
    const auto& lower = duckdb::BoundBetweenExpression::LowerBound(between);
    const auto& upper = duckdb::BoundBetweenExpression::UpperBound(between);
    if (!col ||
        lower.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT ||
        upper.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
      return expr;
    }
    const auto& lo = lower.Cast<duckdb::BoundConstantExpression>().GetValue();
    const auto& hi = upper.Cast<duckdb::BoundConstantExpression>().GetValue();
    if ((!lo.IsNull() && !IsPlainInteger(lo.type())) ||
        (!hi.IsNull() && !IsPlainInteger(hi.type()))) {
      return expr;
    }
    const auto& type = col->GetReturnType();
    auto lo_fitted = lo.DefaultTryCastAs(type);
    auto hi_fitted = hi.DefaultTryCastAs(type);
    if (lo_fitted && hi_fitted && !lo_fitted->IsNull() &&
        !hi_fitted->IsNull()) {
      return duckdb::BoundBetweenExpression::Create(
        col->Copy(),
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          std::move(*lo_fitted)),
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          std::move(*hi_fitted)),
        duckdb::BoundBetweenExpression::LowerInclusive(between),
        duckdb::BoundBetweenExpression::UpperInclusive(between));
    }
    return duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
      ExpressionType::CONJUNCTION_AND,
      CompareIntegerColumn(
        *col, duckdb::BoundBetweenExpression::LowerComparisonType(between), lo),
      CompareIntegerColumn(
        *col, duckdb::BoundBetweenExpression::UpperComparisonType(between),
        hi));
  }
  return expr;
}

void MinMatchRows(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  const auto count = args.size();
  const auto needed = args.data[0].Values<uint32_t>()[0].GetValue();
  struct Tally {
    uint32_t matched;
    uint32_t unknown;
  };
  std::array<Tally, STANDARD_VECTOR_SIZE> tallies;
  std::fill_n(tallies.begin(), count, Tally{});
  for (duckdb::idx_t i = 1; i < args.ColumnCount(); ++i) {
    for (const auto branch : args.data[i].Values<bool>()) {
      auto& tally = tallies[branch.GetIndex()];
      if (!branch.IsValid()) {
        ++tally.unknown;
      } else if (branch.GetValue()) {
        ++tally.matched;
      }
    }
  }
  auto writer = duckdb::FlatVector::Writer<bool>(result, count);
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto& tally = tallies[row];
    if (tally.matched >= needed) {
      writer.WriteValue(true);
    } else if (tally.matched + tally.unknown < needed) {
      writer.WriteValue(false);
    } else {
      writer.WriteNull();
    }
  }
}

duckdb::unique_ptr<duckdb::Expression> MinMatchRowsOf(
  uint32_t needed,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> branches) {
  duckdb::ScalarFunction fn(duckdb::Identifier{"sdb_min_match"},
                            {duckdb::LogicalType::UINTEGER},
                            duckdb::LogicalType::BOOLEAN, MinMatchRows, nullptr,
                            nullptr, nullptr, duckdb::LogicalType::BOOLEAN);
  fn.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> children;
  children.reserve(branches.size() + 1);
  children.push_back(duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::UINTEGER(needed)));
  for (auto& branch : branches) {
    children.push_back(std::move(branch));
  }
  return duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    duckdb::BoundScalarFunction(fn), std::move(children), nullptr);
}

using RowColumns =
  irs::containers::FlatHashMap<connector::BindingKey, duckdb::idx_t>;

duckdb::unique_ptr<duckdb::Expression> RowValueOf(
  duckdb::unique_ptr<duckdb::Expression> expr, const RowColumns& columns) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    return duckdb::make_uniq<duckdb::BoundReferenceExpression>(
      ref.GetAlias(), ref.GetReturnType(),
      columns.at(connector::MakeBindingKey(ref.Binding())));
  }
  if (duckdb::BoundCastExpression::IsCast(*expr)) {
    auto& cast = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto& target = cast.GetReturnType();
    auto& operand = duckdb::BoundCastExpression::ChildMutable(cast);
    if (connector::IsTSQueryStructType(target) ||
        connector::IsTSQueryStructType(operand->GetReturnType())) {
      auto value = RowValueOf(std::move(operand), columns);
      SDB_ASSERT(value->GetReturnType().id() == duckdb::LogicalTypeId::BOOLEAN);
      const auto needed = connector::TryGetMinMatchModifier(target);
      if (!needed) {
        SDB_ASSERT(target.id() == duckdb::LogicalTypeId::BOOLEAN ||
                   connector::HasGroupModifier(target));
        return value;
      }
      SDB_ASSERT(value->GetExpressionType() ==
                 duckdb::ExpressionType::CONJUNCTION_OR);
      return MinMatchRowsOf(
        *needed, std::move(value->Cast<duckdb::BoundConjunctionExpression>()
                             .GetChildrenMutable()));
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = RowValueOf(std::move(child), columns);
    });
  return expr;
}

}  // namespace

duckdb::unique_ptr<duckdb::Expression> NormalizeClaimShape(
  duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::Expression> expr) {
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = NormalizeClaimShape(context, std::move(child));
    });
  if (expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT &&
      expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_LAMBDA &&
      expr->IsFoldable()) {
    duckdb::Value folded;
    if (duckdb::ExpressionExecutor::TryEvaluateFoldable(
          context, *expr, folded,
          duckdb::NumericLimits<duckdb::idx_t>::Maximum())) {
      return duckdb::make_uniq<duckdb::BoundConstantExpression>(
        std::move(folded));
    }
  }
  if (StrictOverNull(*expr)) {
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(
      duckdb::Value{expr->GetReturnType()});
  }
  return FoldColumnCast(std::move(expr));
}

duckdb::unique_ptr<duckdb::Expression> SubstituteParameters(
  duckdb::unique_ptr<duckdb::Expression> expr, bool with_values) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_PARAMETER) {
    const auto& param = expr->Cast<duckdb::BoundParameterExpression>();
    auto value = with_values ? param.ParameterData()->GetValue()
                             : ShapeValue(param.GetReturnType());
    if (!with_values && value.IsNull()) {
      return nullptr;
    }
    if (value.type() != param.GetReturnType()) {
      value = value.DefaultCastAs(param.GetReturnType());
    }
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(value));
  }
  if (!with_values && duckdb::BoundCastExpression::IsCast(*expr)) {
    auto& cast = expr->Cast<duckdb::BoundFunctionExpression>();
    auto& child = duckdb::BoundCastExpression::ChildMutable(cast);
    if (child->GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_PARAMETER) {
      if (auto value = ShapeValue(duckdb::BoundCastExpression::TargetType(cast))
                         .DefaultTryCastAs(child->GetReturnType());
          value && !value->IsNull()) {
        child =
          duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(*value));
        return expr;
      }
    }
  }
  bool unshaped = false;
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      if (!unshaped) {
        child = SubstituteParameters(std::move(child), with_values);
        unshaped = !child;
      }
    });
  return unshaped ? nullptr : std::move(expr);
}

std::optional<RowForm> RowFormOf(const duckdb::Expression& conjunct) {
  if (conjunct.IsVolatile() || conjunct.HasParameter() ||
      conjunct.HasSubquery() || CallsSearchStub(conjunct)) {
    return std::nullopt;
  }
  RowForm form;
  RowColumns columns;
  duckdb::ExpressionIterator::VisitExpression<duckdb::BoundColumnRefExpression>(
    conjunct, [&](const duckdb::BoundColumnRefExpression& ref) {
      if (columns
            .try_emplace(connector::MakeBindingKey(ref.Binding()),
                         form.columns.size())
            .second) {
        form.columns.push_back(&ref);
      }
    });
  if (form.columns.empty()) {
    return std::nullopt;
  }
  form.expr = RowValueOf(conjunct.Copy(), columns);
  if (form.expr->CanThrow()) {
    return std::nullopt;
  }
  return form;
}

duckdb::unique_ptr<duckdb::Expression> ColumnFormOf(
  duckdb::unique_ptr<duckdb::Expression> conjunct) {
  duckdb::ExpressionIterator::VisitExpressionMutable<
    duckdb::BoundColumnRefExpression>(
    conjunct, [](duckdb::BoundColumnRefExpression& ref,
                 duckdb::unique_ptr<duckdb::Expression>& child) {
      child = duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        ref.GetAlias(), ref.GetReturnType(), 0ULL);
    });
  return conjunct;
}

std::optional<connector::SearchColumnInfo> ResolveSearchColumnById(
  duckdb::ClientContext& context, const connector::ScanBindData& scan,
  connector::ColumnId col_id, bool column_stored) {
  auto column_info =
    ColumnInfoOf(scan, scan.relation.ResolveTokenizers(context), col_id);
  if (!column_info) {
    return std::nullopt;
  }
  if (!scan.relation.StoresExpression(col_id) && !scan.IsViewBacked() &&
      scan.IsColumnNotNull(col_id)) {
    column_info->nullable = false;
  }
  column_info->column_stored = column_stored;
  return column_info;
}

void RegisterIResearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &RewriteSearchCallsToColumnRefs,
                 .anchor = duckdb::OptimizerType::FILTER_PUSHDOWN,
                 .where = duckdb::OptimizerHookPosition::Before,
               });
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &FinishSearchScans,
                 .anchor = duckdb::OptimizerType::FILTER_PUSHDOWN,
                 .where = duckdb::OptimizerHookPosition::After,
               });
}

}  // namespace sdb::optimizer
