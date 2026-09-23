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

#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/planner/expression/bound_between_expression.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_parameter_expression.hpp>
#include <duckdb/planner/expression/bound_window_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_join.hpp>
#include <duckdb/planner/operator/logical_order.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>
#include <iresearch/formats/ivf/ivf_reader.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/boolean_rules.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/inverted_index.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/scorer_options.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_offsets.h"
#include "connector/functions/vector.h"
#include "connector/index_expression.hpp"
#include "connector/optimizer/iresearch_plan_common.hpp"
#include "connector/optimizer/ts_dict_plan.hpp"
#include "connector/scan/scan_bind.h"
#include "connector/search_filter_builder.hpp"
#include "pg/connection_context.h"
#include "query/config.h"
#include "search/search_table.h"

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

catalog::ColumnId ResolveColumnId(duckdb::ColumnBinding binding,
                                  const connector::ScanBindData& bind_data,
                                  const duckdb::LogicalGet& get) {
  if (binding.table_index != get.table_index) {
    return catalog::kInvalidColumnId;
  }
  const auto col_idx = binding.column_index.GetIndex();
  const auto& column_ids = get.GetColumnIds();
  if (col_idx >= column_ids.size() || !column_ids[col_idx].HasPrimaryIndex()) {
    return catalog::kInvalidColumnId;
  }
  const auto phys = column_ids[col_idx].GetPrimaryIndex();
  if (phys >= bind_data.columns.ids.size()) {
    return catalog::kInvalidColumnId;
  }
  return bind_data.columns.ids[phys];
}

std::vector<catalog::ColumnId> BuildProjectedColumnIds(
  const duckdb::LogicalGet& get, const connector::ScanBindData& bind_data) {
  std::vector<catalog::ColumnId> projected_ids(get.GetColumnIds().size());
  for (duckdb::idx_t i = 0; i < projected_ids.size(); ++i) {
    projected_ids[i] = ResolveColumnId(
      {get.table_index, duckdb::ProjectionIndex{i}}, bind_data, get);
  }
  return projected_ids;
}

void ResolveSearchTableIndexes(connector::ScanBindData& bind_data,
                               duckdb::ClientContext& context) {
  if (bind_data.relation.IsIndexRelation() ||
      !bind_data.relation.IsSearchTable() ||
      !bind_data.relation.indexes.empty() || bind_data.IsViewBacked()) {
    return;
  }
  const auto* entry = dynamic_cast<const catalog::SereneDBTableEntry*>(
    bind_data.relation.table_entry.get());
  if (!entry || !entry->GetSearchData()) {
    return;
  }
  const auto& shard = *entry->GetSearchData();
  bind_data.relation.indexes = catalog::RelationInvertedIndexes(
    &context, shard.GetSchemaId(), shard.GetTableId());
}

std::shared_ptr<const catalog::InvertedIndex> TermDictIndexFor(
  const connector::ScanBindData& bind_data, catalog::ColumnId col_id) {
  if (bind_data.relation.IsIndexRelation()) {
    return std::static_pointer_cast<const catalog::InvertedIndex>(
      bind_data.relation.indexes.front());
  }
  for (const auto& index : bind_data.relation.indexes) {
    auto inverted =
      std::static_pointer_cast<const catalog::InvertedIndex>(index);
    const auto* info = inverted->FindColumnInfo(col_id);
    if (info && info->IsTermDict()) {
      return inverted;
    }
  }
  return nullptr;
}

irs::field_id ResolveAnnTargetFieldId(const duckdb::Expression& col_arg,
                                      const duckdb::LogicalGet& get,
                                      const connector::ScanBindData& bind_data,
                                      const catalog::InvertedIndex& index,
                                      duckdb::ClientContext& client_context) {
  if (col_arg.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF ||
      col_arg.GetExpressionClass() == duckdb::ExpressionClass::BOUND_REF) {
    if (const auto id =
          bind_data.ColumnIdByName(col_arg.GetName().GetIdentifierName());
        id != catalog::kInvalidColumnId) {
      return id;
    }
  }
  if (SingleReferencedTableIndex(col_arg) != get.table_index) {
    return irs::field_limits::invalid();
  }
  auto normalized = connector::NormalizeBoundExpression(
    col_arg, index.GetRelationId(), BuildProjectedColumnIds(get, bind_data),
    client_context);
  auto serialized = connector::SerializeBoundExpression(*normalized);
  return index.FindFieldIdBySerialized(serialized);
}

std::optional<FoundScan> AsSearchScan(duckdb::LogicalOperator& op) {
  if (op.type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return std::nullopt;
  }
  auto& get = op.Cast<duckdb::LogicalGet>();
  if (!connector::IsSereneDBScan(get)) {
    return std::nullopt;
  }
  auto& bd = get.bind_data->Cast<connector::ScanBindData>();
  return FoundScan{&get, &bd};
}

std::optional<FoundScan> FindIResearchScan(duckdb::LogicalOperator& op,
                                           duckdb::TableIndex target) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    if (op.Cast<duckdb::LogicalGet>().table_index != target) {
      return std::nullopt;
    }
    return AsSearchScan(op);
  }
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == target && proj.children.size() == 1) {
      if (auto result = AsSearchScan(*proj.children[0])) {
        return result;
      }
    }
  }
  for (auto& child : op.children) {
    if (auto result = FindIResearchScan(*child, target)) {
      return result;
    }
  }
  return std::nullopt;
}

duckdb::LogicalProjection* FindProjectionByTableIndex(
  duckdb::LogicalOperator& op, duckdb::TableIndex target) {
  if (op.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    auto& proj = op.Cast<duckdb::LogicalProjection>();
    if (proj.table_index == target) {
      return &proj;
    }
  }
  for (auto& child : op.children) {
    if (auto* result = FindProjectionByTableIndex(*child, target)) {
      return result;
    }
  }
  return nullptr;
}

ResolvedProjection WalkProjections(duckdb::LogicalOperator& root,
                                   duckdb::ColumnBinding binding) {
  while (auto* proj = FindProjectionByTableIndex(root, binding.table_index)) {
    const auto idx = binding.column_index.GetIndex();
    if (idx >= proj->expressions.size()) {
      break;
    }
    auto& forwarded = *proj->expressions[idx];
    if (forwarded.GetExpressionType() !=
        duckdb::ExpressionType::BOUND_COLUMN_REF) {
      return {binding, &forwarded};
    }
    binding = forwarded.Cast<duckdb::BoundColumnRefExpression>().Binding();
  }
  return {binding, nullptr};
}

duckdb::ColumnBinding ResolveBindingThroughProjections(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding) {
  return WalkProjections(root, binding).binding;
}

std::optional<FoundScanColumn> ResolveIResearchScanColumn(
  duckdb::LogicalOperator& root, duckdb::ColumnBinding binding) {
  const auto resolved = ResolveBindingThroughProjections(root, binding);
  auto found = FindIResearchScan(root, resolved.table_index);
  if (!found) {
    return std::nullopt;
  }
  return FoundScanColumn{*found, resolved};
}

duckdb::ColumnBinding ExposeGetColumnAt(duckdb::LogicalOperator& root,
                                        duckdb::TableIndex anchor_ti,
                                        const duckdb::LogicalGet& target_get,
                                        duckdb::idx_t get_col_idx,
                                        std::string_view col_name,
                                        const duckdb::LogicalType& col_type) {
  if (anchor_ti == target_get.table_index) {
    return {target_get.table_index, duckdb::ProjectionIndex{get_col_idx}};
  }
  auto* proj = FindProjectionByTableIndex(root, anchor_ti);
  SDB_ASSERT(proj);
  for (duckdb::idx_t i = 0; i < proj->expressions.size(); ++i) {
    auto& e = *proj->expressions[i];
    if (e.GetExpressionType() != duckdb::ExpressionType::BOUND_COLUMN_REF) {
      continue;
    }
    auto& ref = e.Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Binding().table_index == target_get.table_index &&
        ref.Binding().column_index.GetIndex() == get_col_idx) {
      return {proj->table_index, duckdb::ProjectionIndex{i}};
    }
  }
  proj->expressions.push_back(
    duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
      duckdb::Identifier{col_name}, col_type,
      duckdb::ColumnBinding{target_get.table_index,
                            duckdb::ProjectionIndex{get_col_idx}}));
  if (!proj->types.empty()) {
    proj->types.push_back(col_type);
  }
  return {proj->table_index,
          duckdb::ProjectionIndex{proj->expressions.size() - 1}};
}

duckdb::idx_t AppendVirtualGetColumn(connector::ScanBindData& bind_data,
                                     duckdb::LogicalGet& get,
                                     catalog::ColumnId virtual_id,
                                     const duckdb::LogicalType& col_type,
                                     std::string_view col_name) {
  const auto bind_idx = bind_data.columns.ids.size();
  bind_data.columns.ids.push_back(virtual_id);
  bind_data.columns.types.push_back(col_type);
  get.returned_types.push_back(col_type);
  get.names.emplace_back(col_name);
  const auto get_col_idx = get.GetColumnIds().size();
  const auto proj_idx = get.AddColumnId(bind_idx);
  if (!get.projection_ids.empty()) {
    get.projection_ids.push_back(proj_idx);
  }
  get.types.push_back(col_type);
  return get_col_idx;
}

// A parameterized conjunct is claimed on its shape: built once with NULLs in
// the parameters' places to prove the filter compiles, then rebuilt at every
// execution with the values (connector::BuildDeferredFilter). Its columns are
// resolved now and remembered, since the scan's binding is gone by then.
struct DeferredClaimBuilder {
  connector::DeferredClaim claim;
  std::map<std::pair<duckdb::idx_t, duckdb::idx_t>,
           connector::DeferredColumn>
    columns;
  const BindingColumnId* column_id = nullptr;
  bool used_expr_getter = false;
  bool any_parameter = false;
  bool declined_parameter = false;

  connector::ColumnGetter Recording(const connector::ColumnGetter& getter) {
    return [this, &getter](const duckdb::BoundColumnRefExpression& ref)
             -> std::optional<connector::SearchColumnInfo> {
      auto info = getter(ref);
      if (info) {
        columns.insert_or_assign(
          {ref.Binding().table_index.index,
           ref.Binding().column_index.GetIndex()},
          connector::DeferredColumn{
            .column = (*column_id)(ref), .column_stored = info->column_stored});
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

bool TryClaimIResearchConjunctImpl(
  irs::BooleanFilter& root,
  const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context, connector::FilterScorers* scorers,
  DeferredClaimBuilder* deferred) {
  // A conjunct with an unbound parameter appears in a prepared statement's
  // template plan. Where the scan can read the parameter at execution
  // (`deferred`), the conjunct is claimed on its shape and rebuilt then; where
  // it cannot, duckdb rebinds the statement with the values as constants
  // before every execution, so declining here costs nothing.
  if (conjunct->HasParameter()) {
    if (deferred == nullptr) {
      return false;
    }
    deferred->any_parameter = true;
    auto shaped = NormalizeClaimShape(
      context, SubstituteParameters(conjunct->Copy(), /*with_values=*/false));
    auto node = std::make_unique<irs::BooleanFilter>();
    std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&shaped, 1};
    connector::FilterScorers shaped_scorers;
    const auto column_getter = deferred->Recording(getter);
    const auto expression_getter = deferred->Recording(expr_getter);
    const auto claimed = connector::MakeSearchFilter(
      *node, single, column_getter, context, expression_getter,
      &shaped_scorers, connector::WideRanges::DeclineAll);
    const bool built = absl::c_any_of(
      irs::kAllOccur, [&](irs::Occur occur) { return node->Size(occur) != 0; });
    if (!claimed.ok() || !built || deferred->used_expr_getter ||
        !shaped_scorers.empty()) {
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
  auto node = std::make_unique<irs::BooleanFilter>();
  std::span<const duckdb::unique_ptr<duckdb::Expression>> single{&conjunct, 1};
  const auto claimed =
    connector::MakeSearchFilter(*node, single, getter, context, expr_getter,
                                scorers, connector::WideRanges::DeclineWide);
  const bool built = absl::c_any_of(
    irs::kAllOccur, [&](irs::Occur occur) { return node->Size(occur) != 0; });
  if (!claimed.ok() || !built) {
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

bool TryClaimIResearchConjunct(
  irs::BooleanFilter& root,
  const duckdb::unique_ptr<duckdb::Expression>& conjunct,
  const connector::ColumnGetter& getter,
  const connector::ExpressionGetter& expr_getter,
  duckdb::ClientContext& context, connector::FilterScorers* scorers) {
  return TryClaimIResearchConjunctImpl(root, conjunct, getter, expr_getter,
                                       context, scorers, nullptr);
}

bool WithSearchGetters(duckdb::LogicalGet& get,
                       connector::ScanBindData& bind_data,
                       std::span<const catalog::InvertedIndex* const> indexes,
                       duckdb::ClientContext& context,
                       absl::FunctionRef<bool(const SearchGetters&)> fn) {
  struct IndexTokenizers {
    const catalog::InvertedIndex* index;
    catalog::TokenizerMap dicts;
  };
  // Resolved once for the whole claim: every column of this scan reads its
  // dictionary out of these maps instead of the catalog.
  const auto resolved = indexes | std::views::transform([&](const auto* index) {
                          return IndexTokenizers{
                            index, catalog::ResolveTokenizers(context, *index)};
                        }) |
                        std::ranges::to<std::vector>();
  const auto projected_ids = BuildProjectedColumnIds(get, bind_data);
  const auto table_index = get.table_index;
  const bool table_backed = !bind_data.IsViewBacked();

  irs::containers::FlatHashSet<irs::field_id> analyzed_fields;
  irs::containers::FlatHashMap<irs::field_id, irs::field_id> null_markers;
  irs::containers::FlatHashMap<catalog::ColumnId, bool> not_null_cache;

  const auto column_not_null = [&](catalog::ColumnId col_id) {
    const auto [it, inserted] = not_null_cache.try_emplace(col_id, false);
    if (inserted) {
      it->second = bind_data.IsColumnNotNull(col_id);
    }
    return it->second;
  };

  const auto make_info =
    [&](const IndexTokenizers& resolved_index, irs::field_id field_id,
        const catalog::InvertedIndexEntryInfo* info, duckdb::LogicalType type,
        std::optional<catalog::ColumnId> column) {
      const auto& [index, dicts] = resolved_index;
      auto column_info =
        MakeSearchColumnInfo(field_id, info, std::move(type),
                             index->GetTokenizer(context, dicts, field_id));
      if (column && table_backed && column_not_null(*column)) {
        column_info.null_field_id = irs::field_limits::invalid();
      }
      if (irs::field_limits::valid(column_info.null_field_id)) {
        null_markers[column_info.null_field_id] = column_info.field_id;
      }
      if (column_info.tokenizer.analyzer->type() !=
          irs::Type<irs::KeywordTokenizer>::id()) {
        analyzed_fields.insert(field_id);
      }
      return column_info;
    };

  connector::ColumnGetter getter =
    [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<connector::SearchColumnInfo> {
    const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
    if (col_id == catalog::kInvalidColumnId) {
      return std::nullopt;
    }
    auto type = bind_data.ColumnTypeById(col_id);
    if (type.id() == duckdb::LogicalTypeId::INVALID) {
      return std::nullopt;
    }
    for (const auto& resolved_index : resolved) {
      const auto& index = *resolved_index.index;
      const auto* info = index.FindColumnInfo(col_id);
      if (info && info->IsTermDict()) {
        return make_info(resolved_index, index.TermFieldForColumn(col_id), info,
                         std::move(type), col_id);
      }
    }
    return std::nullopt;
  };

  connector::ExpressionGetter expr_getter = [&](const duckdb::Expression& expr)
    -> std::optional<connector::SearchColumnInfo> {
    if (SingleReferencedTableIndex(expr) != table_index) {
      return std::nullopt;
    }
    for (const auto& resolved_index : resolved) {
      const auto& index = *resolved_index.index;
      auto normalized = connector::NormalizeBoundExpression(
        expr, index.GetRelationId(), projected_ids, context);
      const auto field_id = index.FindFieldIdBySerialized(
        connector::SerializeBoundExpression(*normalized));
      const auto* expr_data = index.ExpressionByFieldId(field_id);
      if (!expr_data) {
        continue;
      }
      return make_info(resolved_index, field_id, index.FindEntry(field_id),
                       expr_data->return_type, std::nullopt);
    }
    return std::nullopt;
  };

  const BindingColumnId column_id =
    [&](const duckdb::BoundColumnRefExpression& ref) -> catalog::ColumnId {
    return ResolveColumnId(ref.Binding(), bind_data, get);
  };

  return fn(SearchGetters{getter, expr_getter, analyzed_fields, null_markers,
                          column_id});
}

void DecidePlanCache(
  connector::ScanBindData& scan,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& residual);

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
  if (!duckdb::ExpressionExecutor::TryEvaluateScalar(context, expr, out)) {
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
  duckdb::Value casted;
  const auto target =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, dim);
  if (!folded.DefaultTryCastAs(target, casted, nullptr) || casted.IsNull()) {
    return false;
  }
  out.reserve(dim);
  for (const auto& child : duckdb::ArrayValue::GetChildren(casted)) {
    if (child.IsNull()) {
      return false;
    }
    out.push_back(child.GetValue<float>());
  }
  return true;
}

irs::field_id ResolveAnnTargetFieldId(const duckdb::Expression& col_arg,
                                      const duckdb::LogicalGet& get,
                                      const connector::ScanBindData& bind_data,
                                      const catalog::InvertedIndex& index,
                                      duckdb::ClientContext& client_context) {
  if (col_arg.GetExpressionClass() ==
      duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    const auto& ref = col_arg.Cast<duckdb::BoundColumnRefExpression>();
    if (ref.Binding().table_index == get.table_index) {
      if (const auto id = ResolveColumnId(ref.Binding(), bind_data, get);
          id != catalog::kInvalidColumnId) {
        return id;
      }
    }
  }
  if (SingleReferencedTableIndex(col_arg) != get.table_index) {
    return irs::field_limits::invalid();
  }
  auto normalized = connector::NormalizeBoundExpression(
    col_arg, index.GetRelationId(), BuildProjectedColumnIds(get, bind_data),
    client_context);
  auto serialized = connector::SerializeBoundExpression(*normalized);
  return index.FindFieldIdBySerialized(serialized);
}

duckdb::idx_t AppendScoreColumn(connector::ScanBindData& bind_data,
                                duckdb::LogicalGet& get) {
  const auto& col_ids = get.GetColumnIds();
  for (duckdb::idx_t j = 0; j < col_ids.size(); ++j) {
    if (ResolveColumnId({get.table_index, duckdb::ProjectionIndex{j}},
                        bind_data, get) == catalog::kInvertedIndexScoreId) {
      return j;
    }
  }
  return AppendVirtualGetColumn(bind_data, get, catalog::kInvertedIndexScoreId,
                                duckdb::LogicalType::FLOAT,
                                catalog::kScoreName);
}

duckdb::unique_ptr<duckdb::Expression> MakeScoreRefExpression(
  duckdb::LogicalOperator& root, const FoundScan& found,
  duckdb::TableIndex anchor_ti) {
  const auto idx = AppendScoreColumn(*found.bind_data, *found.get);
  const auto binding =
    ExposeGetColumnAt(root, anchor_ti, *found.get, idx, catalog::kScoreName,
                      duckdb::LogicalType::FLOAT);
  return duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{catalog::kScoreName}, duckdb::LogicalType::FLOAT,
    binding);
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

bool ScanColumnIsScore(const FoundScanColumn& sc) {
  return ResolveColumnId(sc.binding, *sc.found.bind_data, *sc.found.get) ==
         catalog::kInvertedIndexScoreId;
}

bool BindingResolvesToScoreColumn(const duckdb::BoundColumnRefExpression& ref,
                                  duckdb::LogicalOperator& root) {
  if (ref.GetAlias().empty()) {
    return false;
  }
  const auto sc = ResolveIResearchScanColumn(root, ref.Binding());
  return sc && ScanColumnIsScore(*sc);
}

bool TrySetScorer(std::optional<catalog::ScorerOptions>& scorer,
                  const duckdb::BoundFunctionExpression& func,
                  std::string_view name) {
  auto extracted = catalog::ExtractScorerFromBound(func, name);
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

duckdb::unique_ptr<duckdb::Expression> PushdownScorerCall(
  duckdb::BoundFunctionExpression& func, duckdb::LogicalOperator& root) {
  if (func.GetChildren().empty() ||
      func.GetChildren()[0]->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return nullptr;
  }
  auto& anchor =
    func.GetChildren()[0]->Cast<duckdb::BoundColumnRefExpression>();
  auto found = FindIResearchScan(root, anchor.Binding().table_index);
  if (!found) {
    return nullptr;
  }
  auto& ss = *found->bind_data;
  if (ss.score.vector) {
    return nullptr;
  }
  if (!TrySetScorer(ss.score.text, func,
                    func.Function().GetName().GetIdentifierName())) {
    return nullptr;
  }
  auto ref = MakeScoreRefExpression(root, *found, anchor.Binding().table_index);
  if (!func.GetAlias().empty()) {
    ref->SetAlias(func.GetAlias());
  }
  return ref;
}

uint32_t ReadSearchNprobe(duckdb::ClientContext& context) {
  static constinit SettingRef gNprobe{"sdb_ivf_search_nprobe"};
  const auto n = gNprobe.SignedInt(context);
  return n < 0 ? 0 : static_cast<uint32_t>(n);
}

uint32_t ReadMinSearchFanout(duckdb::ClientContext& context) {
  static constinit SettingRef gFanout{"sdb_ivf_min_search_fanout"};
  const auto n = gFanout.SignedInt(context);
  return n < 0 ? 0 : static_cast<uint32_t>(n);
}

uint32_t ReadMaxSearchFanout(duckdb::ClientContext& context) {
  static constinit SettingRef gFanout{"sdb_ivf_max_search_fanout"};
  const auto n = gFanout.SignedInt(context);
  return n < 0 ? 0 : static_cast<uint32_t>(n);
}

uint32_t ReadHnswEfSearch(duckdb::ClientContext& context) {
  static constinit SettingRef gEfSearch{"sdb_hnsw_ef_search"};
  const auto n = gEfSearch.SignedInt(context);
  return n < 0 ? 0 : static_cast<uint32_t>(n);
}

duckdb::unique_ptr<duckdb::Expression> PushdownDistanceCall(
  duckdb::BoundFunctionExpression& func, const connector::AnnFunctionInfo& info,
  duckdb::LogicalOperator& root, duckdb::ClientContext& context) {
  const auto [col_arg, value_arg] =
    [&] -> std::pair<duckdb::Expression*, duckdb::Expression*> {
    // The query side references no column: a constant, or a prepared
    // statement's parameter (not foldable, read at execution).
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

  const auto anchor_ti = SingleReferencedTableIndex(*col_arg);
  if (!anchor_ti) {
    return nullptr;
  }

  auto found = FindIResearchScan(root, *anchor_ti);
  if (!found) {
    return nullptr;
  }
  auto& ss = *found->bind_data;
  if (ss.score.text || ss.offsets.Active()) {
    return nullptr;
  }

  // An index relation scans exactly one inverted index. A search table scanned
  // by name owns the same segments but carries no index in its bind data until
  // the table's inverted indexes are resolved from the catalog; the ANN column
  // then belongs to whichever of them indexes that field.
  const catalog::InvertedIndex* index = nullptr;
  auto call_field_id = irs::field_limits::invalid();
  if (ss.relation.IsIndexRelation()) {
    index = &ss.relation.ScannedIndex();
    call_field_id =
      ResolveAnnTargetFieldId(*col_arg, *found->get, ss, *index, context);
  } else {
    ResolveSearchTableIndexes(ss, context);
    for (const auto* candidate : ss.relation.InvertedIndexes()) {
      const auto fid =
        ResolveAnnTargetFieldId(*col_arg, *found->get, ss, *candidate, context);
      if (irs::field_limits::valid(fid) && candidate->GetAnnInfo(fid)) {
        index = candidate;
        call_field_id = fid;
        break;
      }
    }
  }
  if (index == nullptr || !irs::field_limits::valid(call_field_id)) {
    return nullptr;
  }
  auto ann_info = index->GetAnnInfo(call_field_id);
  if (!ann_info || ann_info->metric != info.metric) {
    return nullptr;
  }

  std::vector<float> call_qvec;
  std::shared_ptr<const duckdb::Expression> call_qexpr;
  if (info.is_norm) {
    call_qvec.assign(ann_info->d, 0.0f);
  } else if (!TryFoldQueryVector(context, *value_arg, ann_info->d, call_qvec)) {
    // A prepared statement's template plan: the vector is a parameter whose
    // value the execution supplies. Kept as the expression, evaluated at
    // scan init; the plan can then be cached across executions.
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
      .centroids_id = ann_info->centroids_id,
      .postings_id = ann_info->postings_id,
      .quant = ann_info->quant.kind,
      .quant_bits = ann_info->quant.nb_bits,
      .kind = ann_info->kind,
      .nprobe = ReadSearchNprobe(context),
      .min_search_fanout = ReadMinSearchFanout(context),
      .max_search_fanout = ReadMaxSearchFanout(context),
      .ef_search = ReadHnswEfSearch(context),
      .ef_construction = ann_info->ef_construction,
      .posting_size = ann_info->posting_size,
      .hnsw_filter_mode = connector::ReadHnswFilterMode(context),
      .exact = connector::ReadAnnExact(context),
    };
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

  auto ref = MakeScoreRefExpression(root, *found, *anchor_ti);
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
  duckdb::BoundFunctionExpression& func, duckdb::LogicalOperator& root,
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

  auto resolved = ResolveIResearchScanColumn(root, col_ref.Binding());
  if (!resolved) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(", col_ref.GetAlias().GetIdentifierName(),
              ") requires an inverted index scan in the same sub-query"));
  }
  auto& found = resolved->found;
  auto& search_scan = *found.bind_data;

  const auto col_name = [&] -> std::string_view {
    if (const auto& cids = found.get->GetColumnIds();
        resolved->binding.column_index.GetIndex() < cids.size()) {
      return found.get
        ->GetColumnName(cids[resolved->binding.column_index.GetIndex()])
        .GetIdentifierName();
    }
    return col_ref.GetAlias().GetIdentifierName();
  };

  const auto target_col_id =
    ResolveColumnId(resolved->binding, *found.bind_data, *found.get);
  if (target_col_id == catalog::kInvalidColumnId) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in table"));
  }

  ResolveSearchTableIndexes(*found.bind_data, context);
  const auto index = TermDictIndexFor(*found.bind_data, target_col_id);
  const auto* col_info = index ? index->FindColumnInfo(target_col_id) : nullptr;
  if (!col_info) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): column '", col_name(), "' not found in index"));
  }
  const bool is_text = col_info->text_dictionary.isSet();
  const bool offs_stored =
    col_info->features.HasFeatures(irs::IndexFeatures::Offs);
  const auto read_field =
    static_cast<catalog::ColumnId>(index->TermFieldForColumn(target_col_id));

  if (is_text && !offs_stored) {
    auto bind = duckdb::make_uniq<connector::OffsetsBindData>();
    bind->inverted_index = index;
    bind->column_id = read_field;
    bind->limit = limit;
    search_scan.offsets.requests.push_back({.column_id = read_field,
                                            .display_id = target_col_id,
                                            .limit = limit,
                                            .bind = bind.get()});
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

  duckdb::idx_t get_col_idx = duckdb::DConstants::INVALID_INDEX;
  const auto existing = absl::c_find_if(
    search_scan.offsets.requests,
    [&](const auto& req) { return req.column_id == read_field; });
  if (existing != search_scan.offsets.requests.end()) {
    if (existing->limit != limit) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_offsets() called multiple times for field '",
                              col_name(), "' with different limits"));
    }
    get_col_idx = existing->get_col_idx;
  }

  const auto col_type = catalog::MakeOffsetsType();
  const auto offsets_col_name = catalog::MakeOffsetsName(target_col_id);
  if (get_col_idx == duckdb::DConstants::INVALID_INDEX) {
    get_col_idx = AppendVirtualGetColumn(*found.bind_data, *found.get,
                                         catalog::kInvertedIndexOffsetsId,
                                         col_type, offsets_col_name);
    search_scan.offsets.requests.push_back({.column_id = read_field,
                                            .display_id = target_col_id,
                                            .limit = limit,
                                            .get_col_idx = get_col_idx});
  }
  const auto binding =
    ExposeGetColumnAt(root, col_ref.Binding().table_index, *found.get,
                      get_col_idx, offsets_col_name, col_type);
  auto out = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    duckdb::Identifier{offsets_col_name}, col_type, binding);
  out->SetAlias(func.GetAlias());
  return out;
}

void RewriteCallInExpr(duckdb::unique_ptr<duckdb::Expression>& expr,
                       duckdb::LogicalOperator& root,
                       duckdb::ClientContext& context) {
  if (!expr) {
    return;
  }
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    auto& func = expr->Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.Function().GetName().GetIdentifierName();
    if (IsScorerFunctionName(name)) {
      if (auto repl = PushdownScorerCall(func, root)) {
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
      if (auto repl = PushdownOffsetsCall(func, root, context)) {
        expr = std::move(repl);
        return;
      }
    } else if (auto info = connector::GetAnnFunctionInfo(func)) {
      if (auto repl = PushdownDistanceCall(func, *info, root, context)) {
        expr = std::move(repl);
        return;
      }
    }
  } else if (expr->GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_WINDOW) {
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
  } else if (expr->GetExpressionClass() ==
             duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    auto& ref = expr->Cast<duckdb::BoundColumnRefExpression>();
    if (BindingResolvesToScoreColumn(ref, root)) {
      ref.SetAlias({});
    }
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      RewriteCallInExpr(child, root, context);
    });
}

void ReuseExistingScoreColumn(duckdb::Expression& order_expr,
                              duckdb::LogicalOperator& root) {
  if (order_expr.GetExpressionType() !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return;
  }
  auto& ref = order_expr.Cast<duckdb::BoundColumnRefExpression>();
  auto* proj = FindProjectionByTableIndex(root, ref.Binding().table_index);
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
  auto sc = ResolveIResearchScanColumn(root, binding);
  if (!sc || !ScanColumnIsScore(*sc)) {
    return;
  }
  for (duckdb::idx_t j = 0; j < idx; ++j) {
    auto& e = *proj->expressions[j];
    if (e.GetExpressionType() == duckdb::ExpressionType::BOUND_COLUMN_REF &&
        e.Cast<duckdb::BoundColumnRefExpression>().Binding() == binding) {
      ref.BindingMutable().column_index = duckdb::ProjectionIndex{j};
      return;
    }
  }
}

void RewriteIResearchExpressions(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::LogicalOperator>& root,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan, duckdb::Binder& binder) {
  if (plan->type == duckdb::LogicalOperatorType::LOGICAL_DELETE ||
      plan->type == duckdb::LogicalOperatorType::LOGICAL_UPDATE ||
      plan->type == duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO) {
    return;
  }

  for (auto& child : plan->children) {
    RewriteIResearchExpressions(context, root, child, binder);
  }

  switch (plan->type) {
    case duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
    case duckdb::LogicalOperatorType::LOGICAL_FILTER:
    case duckdb::LogicalOperatorType::LOGICAL_WINDOW:
      for (auto& e : plan->expressions) {
        RewriteCallInExpr(e, *root, context);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_ORDER_BY:
      for (auto& o : plan->Cast<duckdb::LogicalOrder>().orders) {
        RewriteCallInExpr(o.expression, *root, context);
        ReuseExistingScoreColumn(*o.expression, *root);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_TOP_N:
      for (auto& o : plan->Cast<duckdb::LogicalTopN>().orders) {
        RewriteCallInExpr(o.expression, *root, context);
        ReuseExistingScoreColumn(*o.expression, *root);
      }
      break;
    case duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      PushdownTsDictAggregates(plan, *root, binder, context);
      break;
    case duckdb::LogicalOperatorType::LOGICAL_UNNEST:
      CollapseTsDictUnnest(plan);
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
    while (x &&
           x->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CAST) {
      x = &x->Cast<duckdb::BoundCastExpression>().Child();
    }
    return x;
  };
  e = strip_casts(e);
  if (e != nullptr &&
      e->GetExpressionClass() == duckdb::ExpressionClass::BOUND_FUNCTION) {
    const auto& fn = e->Cast<duckdb::BoundFunctionExpression>();
    if (fn.Function().GetName().GetIdentifierName() == "-" &&
        fn.GetChildren().size() == 1) {
      negated = true;
      e = strip_casts(fn.GetChildren()[0].get());
    }
  }
  if (e == nullptr ||
      e->GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return std::nullopt;
  }
  return e->Cast<duckdb::BoundColumnRefExpression>().Binding();
}

bool TryClaimAnnRange(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::ScanBindData& bind_data,
  duckdb::ClientContext& context) {
  auto& scan = bind_data;
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
      return b && ResolveColumnId(*b, bind_data, get) ==
                    catalog::kInvertedIndexScoreId;
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
    if (!bound) {
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

bool ClaimSearchConjuncts(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  connector::ScanBindData& bind_data, const SearchGetters& getters,
  duckdb::ClientContext& context) {
  auto& [getter, expr_getter, analyzed_fields, null_markers, column_id] =
    getters;
  (void)column_id;
  auto& scan = bind_data;

  auto root_and = std::make_unique<irs::BooleanFilter>();
  bool any_claimed = false;
  connector::FilterScorers filter_scorers;
  // Parameters are read at execution only by a vector-scored scan, the one
  // whose plan is worth caching; a text-scored one keeps duckdb's rebind.
  DeferredClaimBuilder deferred;
  deferred.column_id = &getters.column_id;
  DeferredClaimBuilder* const deferred_ptr =
    scan.score.vector && !scan.score.text ? &deferred : nullptr;
  std::vector<std::shared_ptr<const duckdb::Expression>> claimed_exprs;
  for (size_t i = 0; i < filters.size();) {
    if (TryClaimIResearchConjunctImpl(*root_and, filters[i], getter,
                                      expr_getter, context, &filter_scorers,
                                      deferred_ptr)) {
      any_claimed = true;
      if (!filters[i]->HasParameter()) {
        claimed_exprs.push_back(filters[i]->Copy());
      }
      std::swap(filters[i], filters.back());
      filters.pop_back();
    } else {
      ++i;
    }
  }
  if (deferred.declined_parameter) {
    scan.plan_cache.declined_parameter = true;
  }
  if (!any_claimed) {
    return false;
  }
  if (!deferred.claim.conjuncts.empty()) {
    // The whole WHERE is rebuilt at execution, the constant conjuncts with
    // it, so the executed filter is one boolean the optimizer has seen whole.
    for (auto& e : claimed_exprs) {
      deferred.claim.conjuncts.push_back(std::move(e));
    }
    deferred.claim.columns = std::make_shared<
      const std::map<std::pair<duckdb::idx_t, duckdb::idx_t>,
                     connector::DeferredColumn>>(
      std::move(deferred.columns));
    deferred.claim.analyzed_fields.insert(analyzed_fields.begin(),
                                          analyzed_fields.end());
    for (const auto& [marker, field] : null_markers) {
      deferred.claim.null_markers.emplace(marker, field);
    }
    scan.plan_cache.deferred = std::move(deferred.claim);
  }

  irs::Filter::ptr root = std::move(root_and);
  connector::EnsureIncludeSides(*root);
  irs::Optimize(root, {.scored = scan.score.text.has_value(),
                       .analyzed_fields = std::move(analyzed_fields),
                       .null_markers = &null_markers});

  scan.search.filter = std::move(root);
  scan.search.filter_scorers = std::move(filter_scorers);
  for (auto& req : scan.offsets.requests) {
    if (req.bind) {
      req.bind->stored_filter = scan.search.filter;
    }
  }
  return true;
}

bool TryClaimSearchFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::ScanBindData& bind_data,
  duckdb::ClientContext& context) {
  const auto indexes = bind_data.relation.InvertedIndexes();
  return WithSearchGetters(
    get, bind_data, indexes, context, [&](const SearchGetters& getters) {
      return ClaimSearchConjuncts(filters, bind_data, getters, context);
    });
}

bool TryClaimSearchTableFilter(
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters,
  duckdb::LogicalGet& get, connector::ScanBindData& bind_data,
  const search::SearchTable& shard, duckdb::ClientContext& context) {
  bind_data.relation.indexes = catalog::RelationInvertedIndexes(
    &context, shard.GetSchemaId(), shard.GetTableId());
  const auto indexes = bind_data.relation.InvertedIndexes();
  // Hold one immutable config snapshot for the whole claim so entry pointers
  // stay valid if DDL swaps the config mid-plan.
  const auto config = shard.GetIndexConfig();
  return WithSearchGetters(
    get, bind_data, indexes, context, [&](const SearchGetters& getters) {
      const connector::ColumnGetter getter =
        [&](const duckdb::BoundColumnRefExpression& ref)
        -> std::optional<connector::SearchColumnInfo> {
        if (auto info = getters.getter(ref)) {
          info->column_stored = true;
          return info;
        }
        const auto col_id = ResolveColumnId(ref.Binding(), bind_data, get);
        if (col_id == catalog::kInvalidColumnId) {
          return std::nullopt;
        }
        const auto field_id = static_cast<irs::field_id>(col_id);
        const auto it = config->find(field_id);
        if (it == config->end() || !it->second.IsTermDict()) {
          return std::nullopt;
        }
        auto type = bind_data.ColumnTypeById(col_id);
        if (type.id() == duckdb::LogicalTypeId::INVALID) {
          return std::nullopt;
        }
        auto info = MakeSearchColumnInfo(field_id, &it->second, std::move(type),
                                         shard.GetTokenizer(context, field_id));
        info.column_stored = true;
        return info;
      };
      return ClaimSearchConjuncts(
        filters, bind_data,
        SearchGetters{getter, getters.expr_getter, getters.analyzed_fields,
                      getters.null_markers, getters.column_id},
        context);
    });
}

// Every SereneDB scan of the plan decides whether its plan may be cached
// across executions; a scan under a WHERE decides again when its filters are
// pushed down (IResearchPushdownComplexFilter).
void DecidePlanCacheForScans(duckdb::LogicalOperator& op) {
  if (auto scan = AsSearchScan(op)) {
    DecidePlanCache(*scan->bind_data, {});
  }
  for (auto& child : op.children) {
    DecidePlanCacheForScans(*child);
  }
}

void RewriteSearchCallsToColumnRefs(
  duckdb::OptimizerExtensionInput& input,
  duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  RewriteIResearchExpressions(input.context, plan, plan,
                              input.optimizer.binder);
  DecidePlanCacheForScans(*plan);
}

}  // namespace

// A vector-scored scan reads its knobs and its query vector at execution
// and rebuilt its parameterized WHERE then too, so its plan may be cached
// across the executions of a prepared statement: unless some conjunct with a
// parameter was declined and stays a filter above the scan, which the
// template plan would evaluate after the graph walk instead of inside it.
void DecidePlanCache(
  connector::ScanBindData& scan,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& residual) {
  if (!scan.score.vector || scan.score.text || scan.ts_dict.Active() ||
      scan.offsets.Active() || scan.plan_cache.declined_parameter ||
      !scan.plan_cache.reacquire_snapshot) {
    scan.plan_cache.cache_plan = false;
    return;
  }
  const bool residual_parameter =
    absl::c_any_of(residual, [](const auto& e) { return e->HasParameter(); });
  scan.plan_cache.cache_plan = !residual_parameter;
}

void IResearchPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (filters.empty() || !bind_data_ptr) {
    return;
  }
  auto& bind_data = bind_data_ptr->Cast<connector::ScanBindData>();
  auto& ss = bind_data;
  if (!bind_data.relation.IsIndexRelation()) {
    if (ss.ts_dict.Active()) {
      const auto indexes = bind_data.relation.InvertedIndexes();
      const auto field = ss.ts_dict.requests.front().field_id;
      const auto it = absl::c_find_if(indexes, [&](const auto* index) -> bool {
        return index->FindEntry(field);
      });
      if (it != indexes.end()) {
        ClaimTsDictFilter(filters, get, bind_data, ss, **it, context);
      }
      return;
    }
    if (!bind_data.search.filter && bind_data.relation.IsSearchTable() &&
        !bind_data.IsViewBacked()) {
      // The shard comes off the entry the scan was bound to: a search table's
      // bind data carries no catalog::Table of its own.
      const auto* entry = dynamic_cast<const catalog::SereneDBTableEntry*>(
        bind_data.relation.table_entry.get());
      if (entry != nullptr && entry->GetSearchData()) {
        TryClaimSearchTableFilter(filters, get, bind_data,
                                  *entry->GetSearchData(), context);
      }
      DecidePlanCache(bind_data, filters);
    }
    return;
  }
  if (ss.ts_dict.Active()) {
    ClaimTsDictFilter(filters, get, bind_data, ss,
                      bind_data.relation.ScannedIndex(), context);
    return;
  }
  if (ss.search.filter) {
    return;
  }
  TryClaimAnnRange(filters, get, bind_data, context);
  if (!filters.empty()) {
    TryClaimSearchFilter(filters, get, bind_data, context);
  }
  DecidePlanCache(bind_data, filters);
}

namespace {

// A stand-in for a parameter's value while a conjunct is claimed on its shape:
// non-NULL, since a comparison with NULL is no shape the filter builder
// accepts, and of the parameter's type.
duckdb::Value ShapeValue(const duckdb::LogicalType& type) {
  if (type.IsNumeric()) {
    return duckdb::Value::Numeric(type, 1);
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return duckdb::Value::BOOLEAN(true);
    case duckdb::LogicalTypeId::VARCHAR:
      return duckdb::Value("a");
    default:
      break;
  }
  duckdb::Value out;
  for (const char* text : {"2000-01-01 00:00:00", "1"}) {
    if (duckdb::Value{text}.DefaultTryCastAs(type, out, nullptr) &&
        !out.IsNull()) {
      return out;
    }
  }
  return duckdb::Value{type};
}

// `CAST(col AS wider)` against a constant of the wider type, as the binder
// shapes a comparison of a narrow integer column with a parameter of a wider
// integer type. The filter builder wants the bare column: the constant moves
// to the column's type when it fits there, else the predicate is decided by
// the column's range (nothing, or every non-NULL row).
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
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CAST) {
    return nullptr;
  }
  const auto& child = expr.Cast<duckdb::BoundCastExpression>().Child();
  if (child.GetExpressionClass() != duckdb::ExpressionClass::BOUND_COLUMN_REF ||
      !IsPlainInteger(child.GetReturnType()) ||
      !IsPlainInteger(expr.GetReturnType())) {
    return nullptr;
  }
  return &child.Cast<duckdb::BoundColumnRefExpression>();
}

// `col <op> value` with the value of the column's type; `op` is the
// comparison with the column on the left.
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
  duckdb::Value fitted;
  if (value.DefaultTryCastAs(type, fitted, nullptr) && !fitted.IsNull()) {
    return compare(op, std::move(fitted));
  }
  // Out of the column's range: above it when positive (every plain integer
  // type holds zero), below it otherwise.
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
  // A comparison with NULL: the builder's "matches nothing".
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
    if (col == nullptr) {
      col = CastOfIntegerColumn(right);
      constant = &left;
      op = duckdb::FlipComparisonExpression(op);
    }
    if (col == nullptr || constant->GetExpressionClass() !=
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
    if (col == nullptr ||
        lower.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT ||
        upper.GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT) {
      return expr;
    }
    const auto& lo = lower.Cast<duckdb::BoundConstantExpression>().GetValue();
    const auto& hi = upper.Cast<duckdb::BoundConstantExpression>().GetValue();
    if (lo.IsNull() || hi.IsNull() || !IsPlainInteger(lo.type()) ||
        !IsPlainInteger(hi.type())) {
      return expr;
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

}  // namespace

duckdb::unique_ptr<duckdb::Expression> NormalizeClaimShape(
  duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::Expression> expr) {
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = NormalizeClaimShape(context, std::move(child));
    });
  if (expr->GetExpressionClass() != duckdb::ExpressionClass::BOUND_CONSTANT &&
      expr->IsFoldable()) {
    duckdb::Value folded;
    if (duckdb::ExpressionExecutor::TryEvaluateScalar(context, *expr, folded)) {
      return duckdb::make_uniq<duckdb::BoundConstantExpression>(
        std::move(folded));
    }
  }
  return FoldColumnCast(std::move(expr));
}

duckdb::unique_ptr<duckdb::Expression> SubstituteParameters(
  duckdb::unique_ptr<duckdb::Expression> expr, bool with_values) {
  if (expr->GetExpressionClass() == duckdb::ExpressionClass::BOUND_PARAMETER) {
    const auto& param = expr->Cast<duckdb::BoundParameterExpression>();
    auto value = with_values ? param.ParameterData()->GetValue()
                             : ShapeValue(param.GetReturnType());
    if (value.type() != param.GetReturnType()) {
      value = value.DefaultCastAs(param.GetReturnType());
    }
    return duckdb::make_uniq<duckdb::BoundConstantExpression>(std::move(value));
  }
  duckdb::ExpressionIterator::EnumerateChildren(
    *expr, [&](duckdb::unique_ptr<duckdb::Expression>& child) {
      child = SubstituteParameters(std::move(child), with_values);
    });
  return expr;
}

std::optional<connector::SearchColumnInfo> ResolveSearchColumnById(
  duckdb::ClientContext& context, const connector::ScanBindData& scan,
  catalog::ColumnId col_id, bool column_stored) {
  auto type = scan.ColumnTypeById(col_id);
  if (type.id() == duckdb::LogicalTypeId::INVALID) {
    return std::nullopt;
  }
  const bool table_backed = !scan.IsViewBacked();
  const auto finish = [&](connector::SearchColumnInfo info) {
    if (table_backed && scan.IsColumnNotNull(col_id)) {
      info.null_field_id = irs::field_limits::invalid();
    }
    info.column_stored = column_stored;
    return info;
  };
  for (const auto* index : scan.relation.InvertedIndexes()) {
    const auto* info = index->FindColumnInfo(col_id);
    if (info == nullptr || !info->IsTermDict()) {
      continue;
    }
    const auto field_id = index->TermFieldForColumn(col_id);
    const auto dicts = catalog::ResolveTokenizers(context, *index);
    return finish(
      MakeSearchColumnInfo(field_id, info, std::move(type),
                           index->GetTokenizer(context, dicts, field_id)));
  }
  // A search table's stored column with a term dictionary of its own.
  if (scan.relation.IsSearchTable() && table_backed) {
    const auto* entry = dynamic_cast<const catalog::SereneDBTableEntry*>(
      scan.relation.table_entry.get());
    if (entry != nullptr && entry->GetSearchData()) {
      const auto& shard = *entry->GetSearchData();
      const auto config = shard.GetIndexConfig();
      const auto field_id = static_cast<irs::field_id>(col_id);
      const auto it = config->find(field_id);
      if (it != config->end() && it->second.IsTermDict()) {
        return finish(
          MakeSearchColumnInfo(field_id, &it->second, std::move(type),
                               shard.GetTokenizer(context, field_id)));
      }
    }
  }
  return std::nullopt;
}

void RegisterIResearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(
    db.config, duckdb::OptimizerExtension{
                 .rule = &RewriteSearchCallsToColumnRefs,
                 .anchor = duckdb::OptimizerType::FILTER_PUSHDOWN,
                 .where = duckdb::OptimizerHookPosition::Before,
               });
}

}  // namespace sdb::optimizer
