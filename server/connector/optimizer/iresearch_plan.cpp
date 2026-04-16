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

#include <absl/base/internal/endian.h>

#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_limit.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_top_n.hpp>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/functions/search.h"
#include "connector/functions/vector.h"
#include "connector/search_filter_builder.hpp"
#include "connector/search_filter_printer.hpp"
#include "search/inverted_index_shard.h"
#include "storage_engine/index_shard.h"

namespace sdb::optimizer {
namespace {

// ---------------------------------------------------------------------------
// Mutation context
// ---------------------------------------------------------------------------

bool IsMutationOp(duckdb::LogicalOperatorType type) {
  switch (type) {
    case duckdb::LogicalOperatorType::LOGICAL_DELETE:
    case duckdb::LogicalOperatorType::LOGICAL_UPDATE:
    case duckdb::LogicalOperatorType::LOGICAL_MERGE_INTO:
      return true;
    default:
      return false;
  }
}

// ---------------------------------------------------------------------------
// Common: resolve the inverted index + shard for a serenedb_scan
// ---------------------------------------------------------------------------

struct ResolvedIresearch {
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<search::InvertedIndexShard> shard;
};

// Look up the InvertedIndex for the scan. Priority:
//   1. Explicit -- when the entry is a SereneDBIndexScanEntry on an
//      inverted index (FROM idx_name).
//   2. Auto -- the first InvertedIndex on the underlying table.
//
// Returns nullptr index when none is available; nullptr shard when the
// shard isn't readable (no live snapshot yet).
std::optional<ResolvedIresearch> ResolveIresearch(
  const connector::SereneDBScanBindData& bind_data,
  const catalog::Snapshot& snapshot) {
  ResolvedIresearch out;

  // Explicit (FROM idx_name) priority.
  if (bind_data.table_entry) {
    if (const auto* idx_entry =
          dynamic_cast<const connector::SereneDBIndexScanEntry*>(
            &*bind_data.table_entry)) {
      out.index = idx_entry->GetInvertedIndex();
    }
  }
  // Auto-detect over the table.
  if (!out.index) {
    for (auto& obj : snapshot.GetIndexesByTable(bind_data.table->GetId())) {
      if (obj->GetType() == catalog::ObjectType::InvertedIndex) {
        out.index =
          std::dynamic_pointer_cast<const catalog::InvertedIndex>(obj);
        break;
      }
    }
  }
  if (!out.index) {
    return std::nullopt;
  }

  for (auto& shard : snapshot.GetIndexShardsByTable(bind_data.table->GetId())) {
    if (shard->GetIndexId() == out.index->GetId() &&
        shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
      out.shard = std::dynamic_pointer_cast<search::InvertedIndexShard>(shard);
      break;
    }
  }
  if (!out.shard || !out.shard->GetInvertedIndexSnapshot()) {
    return std::nullopt;
  }
  return out;
}

// Map a column name as seen in a DuckDB expression back to its catalog
// Column::Id on the underlying SereneDB table. Returns the special
// invalid-id sentinel when the name isn't on the table.
catalog::Column::Id ColumnIdByName(const catalog::Table& table,
                                   std::string_view name) {
  for (const auto& col : table.Columns()) {
    if (col.name == name) {
      return col.id;
    }
  }
  return std::numeric_limits<catalog::Column::Id>::max();
}

// Encode a Column::Id as iresearch HNSW field name (8 big-endian bytes,
// no Mangle suffix -- vectors are stored without a per-type tag).
std::string MakeHnswFieldName(catalog::Column::Id col_id) {
  std::string name(sizeof(col_id), '\0');
  absl::big_endian::Store(name.data(), col_id);
  return name;
}

// ---------------------------------------------------------------------------
// Vector argument extraction (shared by ANN topk + ANN range)
// ---------------------------------------------------------------------------

bool IsDistanceFunction(std::string_view name) {
  return name == connector::kL2Distance;
}

// Pull a flat float vector from a constant ARRAY Value. Rejects mixed /
// non-float element types and nulls.
bool TryExtractQueryVector(const duckdb::Value& val, std::vector<float>& out) {
  using duckdb::LogicalTypeId;
  if (val.type().id() != LogicalTypeId::ARRAY) {
    return false;
  }
  const auto& children = duckdb::ArrayValue::GetChildren(val);
  if (children.empty()) {
    return false;
  }
  out.reserve(children.size());
  for (const auto& child : children) {
    if (child.IsNull()) {
      return false;
    }
    switch (child.type().id()) {
      case LogicalTypeId::FLOAT:
        out.push_back(child.GetValue<float>());
        break;
      case LogicalTypeId::DOUBLE:
        out.push_back(static_cast<float>(child.GetValue<double>()));
        break;
      default:
        return false;
    }
  }
  return true;
}

// Identify column + constant inside a distance call: distance(col, vec)
// or distance(vec, col). Returns nullptr on shape mismatch.
struct DistanceArgs {
  duckdb::Expression* col_arg = nullptr;
  duckdb::BoundConstantExpression* const_arg = nullptr;
};
DistanceArgs ExtractDistanceArgs(duckdb::BoundFunctionExpression& func_expr) {
  DistanceArgs out;
  for (auto& child : func_expr.children) {
    if (child->expression_class == duckdb::ExpressionClass::BOUND_CONSTANT) {
      out.const_arg = &child->Cast<duckdb::BoundConstantExpression>();
    } else if (child->expression_class ==
                 duckdb::ExpressionClass::BOUND_COLUMN_REF ||
               child->expression_class == duckdb::ExpressionClass::BOUND_REF) {
      out.col_arg = child.get();
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Case 4: ANN top-k  (LogicalTopN -> Projection -> LogicalGet)
// ---------------------------------------------------------------------------

bool TryAnnTopk(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    return false;
  }
  auto& top_n = plan->Cast<duckdb::LogicalTopN>();
  if (top_n.limit == 0) {
    return false;
  }
  if (top_n.orders.size() != 1 ||
      top_n.orders[0].type != duckdb::OrderType::ASCENDING) {
    return false;
  }
  if (top_n.orders[0].expression->type !=
      duckdb::ExpressionType::BOUND_COLUMN_REF) {
    return false;
  }
  auto& order_col_ref =
    top_n.orders[0].expression->Cast<duckdb::BoundColumnRefExpression>();

  if (top_n.children.size() != 1 ||
      top_n.children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return false;
  }
  auto& projection = top_n.children[0]->Cast<duckdb::LogicalProjection>();

  const auto proj_col_idx = order_col_ref.binding.column_index;
  if (proj_col_idx >= projection.expressions.size()) {
    return false;
  }
  auto& dist_expr = *projection.expressions[proj_col_idx];
  if (dist_expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  auto& func_expr = dist_expr.Cast<duckdb::BoundFunctionExpression>();
  if (!IsDistanceFunction(func_expr.function.name) ||
      func_expr.children.size() < 2) {
    return false;
  }

  if (projection.children.size() != 1 ||
      projection.children[0]->type !=
        duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = projection.children[0]->Cast<duckdb::LogicalGet>();
  if (!get.bind_data ||
      !dynamic_cast<connector::SereneDBScanBindData*>(&*get.bind_data)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (!bind_data.table) {
    return false;
  }
  if (!std::holds_alternative<connector::FullTableScan>(
        bind_data.scan_source)) {
    return false;
  }

  auto args = ExtractDistanceArgs(func_expr);
  if (!args.col_arg || !args.const_arg) {
    return false;
  }
  std::vector<float> query_vector;
  if (!TryExtractQueryVector(args.const_arg->value, query_vector)) {
    return false;
  }
  auto col_id = ColumnIdByName(*bind_data.table, args.col_arg->GetName());
  if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
    return false;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  connector::ANNScan ann;
  ann.index_id = resolved->index->GetId();
  ann.field_name = MakeHnswFieldName(col_id);
  ann.query_vector = std::move(query_vector);
  ann.top_k = static_cast<size_t>(top_n.limit);
  bind_data.scan_source = std::move(ann);
  get.function = connector::CreateIresearchAnnScanFunction();

  // The HNSW scan returns rows pre-sorted, bounded; drop the TopN.
  plan = std::move(top_n.children[0]);
  return true;
}

// ---------------------------------------------------------------------------
// Case 3: ANN range  (LogicalFilter[ distance(col, vec) < radius ] ->
// LogicalGet)
// ---------------------------------------------------------------------------

bool TryAnnRange(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    return false;
  }
  auto& filter = plan->Cast<duckdb::LogicalFilter>();
  if (filter.children.size() != 1) {
    return false;
  }
  // Descend through any intermediate LogicalFilters inserted by DuckDB's
  // FilterPushdown optimiser before reaching the LogicalGet.
  duckdb::LogicalOperator* get_op = filter.children[0].get();
  while (get_op->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    if (get_op->children.size() != 1) {
      return false;
    }
    get_op = get_op->children[0].get();
  }
  if (get_op->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = get_op->Cast<duckdb::LogicalGet>();
  if (!get.bind_data ||
      !dynamic_cast<connector::SereneDBScanBindData*>(&*get.bind_data)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (!bind_data.table) {
    return false;
  }
  if (!std::holds_alternative<connector::FullTableScan>(
        bind_data.scan_source)) {
    return false;
  }

  duckdb::idx_t match_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::BoundFunctionExpression* func_expr_ptr = nullptr;
  float radius = 0.0f;

  for (duckdb::idx_t i = 0; i < filter.expressions.size(); ++i) {
    auto& expr = *filter.expressions[i];
    if (expr.expression_class != duckdb::ExpressionClass::BOUND_COMPARISON) {
      continue;
    }
    auto& cmp = expr.Cast<duckdb::BoundComparisonExpression>();
    duckdb::Expression* func_side = nullptr;
    duckdb::Expression* const_side = nullptr;
    switch (cmp.type) {
      case duckdb::ExpressionType::COMPARE_LESSTHAN:
      case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
        func_side = cmp.left.get();
        const_side = cmp.right.get();
        break;
      case duckdb::ExpressionType::COMPARE_GREATERTHAN:
      case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        func_side = cmp.right.get();
        const_side = cmp.left.get();
        break;
      default:
        continue;
    }
    if (func_side->expression_class !=
        duckdb::ExpressionClass::BOUND_FUNCTION) {
      continue;
    }
    auto& func = func_side->Cast<duckdb::BoundFunctionExpression>();
    if (!IsDistanceFunction(func.function.name) || func.children.size() < 2) {
      continue;
    }
    if (const_side->expression_class !=
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      continue;
    }
    auto& const_expr = const_side->Cast<duckdb::BoundConstantExpression>();
    if (const_expr.value.IsNull()) {
      continue;
    }
    switch (const_expr.value.type().id()) {
      case duckdb::LogicalTypeId::FLOAT:
        radius = const_expr.value.GetValue<float>();
        break;
      case duckdb::LogicalTypeId::DOUBLE:
        radius = static_cast<float>(const_expr.value.GetValue<double>());
        break;
      default:
        continue;
    }
    func_expr_ptr = &func;
    match_idx = i;
    break;
  }

  if (match_idx == duckdb::DConstants::INVALID_INDEX) {
    return false;
  }

  auto args = ExtractDistanceArgs(*func_expr_ptr);
  if (!args.col_arg || !args.const_arg) {
    return false;
  }
  std::vector<float> query_vector;
  if (!TryExtractQueryVector(args.const_arg->value, query_vector)) {
    return false;
  }
  auto col_id = ColumnIdByName(*bind_data.table, args.col_arg->GetName());
  if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
    return false;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  connector::RangeSearchScan rss;
  rss.index_id = resolved->index->GetId();
  rss.field_name = MakeHnswFieldName(col_id);
  rss.query_vector = std::move(query_vector);
  rss.radius = radius;
  bind_data.scan_source = std::move(rss);
  get.function = connector::CreateIresearchRangeScanFunction();

  filter.expressions.erase(filter.expressions.begin() + match_idx);
  if (filter.expressions.empty()) {
    plan = std::move(filter.children[0]);
  }
  return true;
}

// ---------------------------------------------------------------------------
// Case 1/2: boolean filter via search_filter_builder
// ---------------------------------------------------------------------------

// Build a SearchColumnInfo for a column ref, if the column belongs to
// our scan AND it's part of an inverted index. Returns nullopt
// otherwise (the search builder will then refuse to claim that
// expression and we leave it on the LogicalFilter).
//
// `projected_column_ids` is indexed by binding.column_index after
// projection pushdown reordering -- same translation we do in
// rocksdb_plan. `analyzer_provider` queries the InvertedIndex for the
// per-column analyzer (op_class).
struct SearchColumnContext {
  duckdb::TableIndex table_index;
  std::span<const catalog::Column::Id> projected_column_ids;
  containers::FlatHashMap<catalog::Column::Id, duckdb::LogicalType>
    column_type_by_id;
  containers::FlatHashSet<catalog::Column::Id> indexed_column_ids;
  std::function<catalog::ColumnAnalyzer(catalog::Column::Id)> analyzer_provider;
};

connector::ColumnGetter MakeColumnGetter(SearchColumnContext& ctx) {
  return [&ctx](const duckdb::BoundColumnRefExpression& ref)
           -> std::optional<connector::SearchColumnInfo> {
    if (ref.binding.table_index != ctx.table_index) {
      return std::nullopt;
    }
    if (ref.binding.column_index >= ctx.projected_column_ids.size()) {
      return std::nullopt;
    }
    const auto col_id = ctx.projected_column_ids[ref.binding.column_index];
    if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
      return std::nullopt;
    }
    if (!ctx.indexed_column_ids.contains(col_id)) {
      return std::nullopt;
    }
    auto type_it = ctx.column_type_by_id.find(col_id);
    if (type_it == ctx.column_type_by_id.end()) {
      return std::nullopt;
    }
    connector::SearchColumnInfo info;
    info.column_id = col_id;
    info.logical_type = type_it->second;
    info.analyzer = ctx.analyzer_provider(col_id);
    return info;
  };
}

bool TrySearchFilter(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_FILTER) {
    return false;
  }
  auto& filter = plan->Cast<duckdb::LogicalFilter>();
  if (filter.children.size() != 1) {
    return false;
  }
  // DuckDB's built-in optimisers (FilterPushdown, RemoveUnusedColumns, etc.)
  // may insert LogicalFilter and LogicalProjection nodes between the outer
  // LogicalFilter and the LogicalGet. Descend through all of them.
  duckdb::LogicalOperator* get_op = filter.children[0].get();
  while (get_op->type == duckdb::LogicalOperatorType::LOGICAL_FILTER ||
         get_op->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    if (get_op->children.size() != 1) {
      return false;
    }
    get_op = get_op->children[0].get();
  }
  if (get_op->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
    return false;
  }
  auto& get = get_op->Cast<duckdb::LogicalGet>();
  if (!get.bind_data ||
      !dynamic_cast<connector::SereneDBScanBindData*>(&*get.bind_data)) {
    return false;
  }
  auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
  if (!bind_data.table) {
    return false;
  }
  if (!std::holds_alternative<connector::FullTableScan>(
        bind_data.scan_source)) {
    return false;
  }
  if (filter.expressions.empty()) {
    return false;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto resolved = ResolveIresearch(bind_data, *snapshot);
  if (!resolved) {
    return false;
  }

  // Build the column-resolution context for the filter builder.
  // ctx.projected_column_ids is a std::span that borrows from projected_ids --
  // projected_ids must outlive ctx / getter.
  constexpr auto kInvalidId = std::numeric_limits<catalog::Column::Id>::max();
  std::vector<catalog::Column::Id> projected_ids;
  projected_ids.reserve(get.GetColumnIds().size());
  for (const auto& ci : get.GetColumnIds()) {
    if (!ci.HasPrimaryIndex()) {
      projected_ids.push_back(kInvalidId);
      continue;
    }
    const auto phys = ci.GetPrimaryIndex();
    projected_ids.push_back(phys < bind_data.column_ids.size()
                              ? bind_data.column_ids[phys]
                              : kInvalidId);
  }
  SearchColumnContext ctx;
  ctx.table_index = get.table_index;
  ctx.projected_column_ids = projected_ids;

  for (const auto& col : bind_data.table->Columns()) {
    ctx.column_type_by_id.emplace(col.id, col.type);
  }
  for (auto col_id : resolved->index->GetColumnIds()) {
    ctx.indexed_column_ids.insert(col_id);
  }
  auto index_ptr = resolved->index;
  auto snapshot_for_analyzer = snapshot;
  ctx.analyzer_provider = [index_ptr,
                           snapshot_for_analyzer](catalog::Column::Id col_id) {
    return index_ptr->GetColumnAnalyzer(snapshot_for_analyzer, col_id);
  };
  auto getter = MakeColumnGetter(ctx);

  // Per-expression claim: try each filter expression individually so we
  // can leave non-iresearch predicates on the LogicalFilter. The
  // BooleanFilter::PopBack() rollback handles partial-add failures.
  irs::And root;
  std::vector<size_t> claimed_indices;
  for (size_t i = 0; i < filter.expressions.size(); ++i) {
    const auto before = root.size();
    std::span<const duckdb::unique_ptr<duckdb::Expression>> single{
      &filter.expressions[i], 1};
    auto result = connector::MakeSearchFilter(root, single, getter);
    if (result.ok() && root.size() > before) {
      claimed_indices.push_back(i);
    } else {
      while (root.size() > before) {
        root.PopBack();
      }
    }
  }
  if (claimed_indices.empty()) {
    return false;
  }

  // Capture the demangled filter summary BEFORE preparing (prepare
  // consumes the tree into an opaque Query).
  auto col_name_lookup = [table_ptr = bind_data.table](
                           catalog::Column::Id col_id) -> std::string_view {
    static thread_local std::string fallback;
    for (const auto& col : table_ptr->Columns()) {
      if (col.id == col_id) {
        return col.name;
      }
    }
    fallback = absl::StrCat("col", col_id);
    return fallback;
  };
  std::string filter_summary = irs::ToStringDemangled(root, col_name_lookup);

  auto& reader = *resolved->shard->GetInvertedIndexSnapshot()->reader;
  connector::SearchScan search;
  search.snapshot = resolved->shard->GetInvertedIndexSnapshot();
  search.reader = &reader;
  search.query = root.prepare({.index = reader});
  search.filter_summary = std::move(filter_summary);
  bind_data.scan_source = std::move(search);
  get.function = connector::CreateIresearchSearchScanFunction();

  // Drop claimed expressions from the filter. If everything was claimed,
  // lift the LogicalGet up to replace the LogicalFilter entirely.
  for (auto it = claimed_indices.rbegin(); it != claimed_indices.rend(); ++it) {
    filter.expressions.erase(filter.expressions.begin() +
                             static_cast<std::ptrdiff_t>(*it));
  }
  if (filter.expressions.empty()) {
    plan = std::move(filter.children[0]);
  }
  return true;
}

// ---------------------------------------------------------------------------
// Score / offsets attachment helpers (cases 1 + 2)
// ---------------------------------------------------------------------------

// Walks down the `Projection -> [Filter] -> Get` shape (any number of
// projection / filter layers) looking for a serenedb_scan LogicalGet
// whose bind data is currently a SearchScan (i.e. an earlier rule pass
// already claimed the boolean filter). Returns nullptr if the chain
// doesn't end in such a scan.
struct FoundScan {
  duckdb::LogicalGet* get;
  connector::SereneDBScanBindData* bind_data;
  connector::SearchScan* search_scan;
};
std::optional<FoundScan> FindSearchScanChild(duckdb::LogicalOperator& op) {
  if (op.children.size() != 1) {
    return std::nullopt;
  }
  auto& child = *op.children[0];
  if (child.type == duckdb::LogicalOperatorType::LOGICAL_GET) {
    auto& get = child.Cast<duckdb::LogicalGet>();
    if (!get.bind_data ||
        !dynamic_cast<connector::SereneDBScanBindData*>(&*get.bind_data)) {
      return std::nullopt;
    }
    auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
    auto* ss = std::get_if<connector::SearchScan>(&bind_data.scan_source);
    if (!ss) {
      return std::nullopt;
    }
    return FoundScan{&get, &bind_data, ss};
  }
  if (child.type == duckdb::LogicalOperatorType::LOGICAL_FILTER ||
      child.type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION ||
      child.type == duckdb::LogicalOperatorType::LOGICAL_LIMIT ||
      child.type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    // Allow the search-scan chain to span any composition of
    // projection / filter / limit / topn nodes between the rule's
    // current node and the LogicalGet at the bottom. The score /
    // offsets / score_top_k attachments below all rely on this lookup
    // to find the right scan to mutate.
    return FindSearchScanChild(child);
  }
  return std::nullopt;
}

// Extract a constant Value pointer from `expr`, or null if not a
// BoundConstantExpression. Used by the scorer-param parser to read
// compile-time k1 / b / with_norms arguments.
const duckdb::Value* TryGetConstantValue(const duckdb::Expression& expr) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_CONSTANT) {
    return nullptr;
  }
  return &expr.Cast<duckdb::BoundConstantExpression>().value;
}

// True iff `expr` is a BoundColumnRefExpression bound to the given
// scan's table_index. We treat the actual column position as opaque --
// any column from the scan is OK, but the convention is `tableoid` so
// the binding lasts through projection pushdown.
bool IsScanColumnRef(const duckdb::Expression& expr,
                     duckdb::TableIndex scan_table_index) {
  if (expr.expression_class != duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    return false;
  }
  return expr.Cast<duckdb::BoundColumnRefExpression>().binding.table_index ==
         scan_table_index;
}

// Resolve a column reference (post projection-pushdown) to its catalog
// Column::Id via `bind_data.column_ids`. Returns kInvalidId if the ref
// doesn't belong to the scan.
catalog::Column::Id ResolveColumnId(
  const duckdb::BoundColumnRefExpression& ref,
  const connector::SereneDBScanBindData& bind_data,
  const duckdb::LogicalGet& get) {
  if (ref.binding.table_index != get.table_index) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  const auto col_idx = ref.binding.column_index;
  const auto& column_ids = get.GetColumnIds();
  if (col_idx >= column_ids.size()) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  if (!column_ids[col_idx].HasPrimaryIndex()) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  const auto phys = column_ids[col_idx].GetPrimaryIndex();
  if (phys >= bind_data.column_ids.size()) {
    return std::numeric_limits<catalog::Column::Id>::max();
  }
  return bind_data.column_ids[phys];
}

// Walk a LogicalProjection's expressions for sdb_score / sdb_offsets
// calls anchored on the SearchScan child. Attach a Scorer / Offsets
// request to the SearchScan bind_data when we find them.
//
// We do NOT rewrite the projection expressions here: the runtime side
// will recognise these calls as "produced by the scan" once it lands.
// For the optimization stage the only observable effect is the new
// strategy-name suffix in EXPLAIN ("fulltext_score" / "+offsets").
//
// TODO(runtime): once execution honours scorer / offsets, rewrite the
// projection to pull the score / offsets columns directly from the
// scan output instead of evaluating the stub function.
bool TryAttachScoreOffsets(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
    return false;
  }
  auto& projection = plan->Cast<duckdb::LogicalProjection>();
  auto found = FindSearchScanChild(projection);
  if (!found) {
    return false;
  }
  auto* search = found->search_scan;
  bool changed = false;
  for (duckdb::idx_t i = 0; i < projection.expressions.size(); ++i) {
    auto& expr = *projection.expressions[i];
    if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
      continue;
    }
    auto& func = expr.Cast<duckdb::BoundFunctionExpression>();
    const auto& name = func.function.name;
    if (name == connector::kBm25 || name == connector::kTfidf) {
      // bm25(tableoid [, k1 DOUBLE, b DOUBLE])
      // tfidf(tableoid [, with_norms BOOLEAN])
      //
      // The iresearch_plan rule extracts the parameter constants at
      // compile time and plumbs them through bind data as typed
      // fields; the runtime executor reads ScorerParams directly, no
      // per-row re-parsing. Only one scorer per scan -- later calls
      // are ignored.
      if (func.children.empty() ||
          !IsScanColumnRef(*func.children[0], found->get->table_index)) {
        continue;
      }
      if (search->scorer.kind != connector::SearchScan::ScorerKind::None) {
        // A scorer was already set by an earlier projection entry.
        // Skip to avoid silently overwriting.
        continue;
      }
      if (name == connector::kBm25) {
        search->scorer.kind = connector::SearchScan::ScorerKind::Bm25;
        if (func.children.size() == 3) {
          // bm25(tableoid, k1, b)
          auto* k1_const = TryGetConstantValue(*func.children[1]);
          auto* b_const = TryGetConstantValue(*func.children[2]);
          if (!k1_const || !b_const) {
            // Non-constant params -- the rule refuses to claim; fall
            // back to stub-eval (runtime will raise). Reset kind.
            search->scorer.kind = connector::SearchScan::ScorerKind::None;
            continue;
          }
          search->scorer.bm25_k1 = k1_const->GetValue<double>();
          search->scorer.bm25_b = b_const->GetValue<double>();
        }
        // else (1-arg form): keep defaults k1=1.2, b=0.75.
      } else {
        search->scorer.kind = connector::SearchScan::ScorerKind::Tfidf;
        if (func.children.size() == 2) {
          // tfidf(tableoid, with_norms)
          auto* c = TryGetConstantValue(*func.children[1]);
          if (!c) {
            search->scorer.kind = connector::SearchScan::ScorerKind::None;
            continue;
          }
          search->scorer.tfidf_with_norms = c->GetValue<bool>();
        }
        // else (1-arg form): keep default with_norms=false.
      }
      changed = true;
      continue;
    }
    if (name == connector::kOffsets) {
      // sdb_offsets(col) -- single VARCHAR column ref. The column ref's
      // own binding.table_index identifies the scan (no separate
      // tableoid anchor is needed); we match it against the scan we
      // found via FindSearchScanChild.
      if (func.children.size() != 1 ||
          func.children[0]->expression_class !=
            duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        continue;
      }
      auto& col_ref =
        func.children[0]->Cast<duckdb::BoundColumnRefExpression>();
      if (col_ref.binding.table_index != found->get->table_index) {
        continue;
      }
      const auto col_id =
        ResolveColumnId(col_ref, *found->bind_data, *found->get);
      if (col_id == std::numeric_limits<catalog::Column::Id>::max()) {
        continue;
      }
      // Append unless we already have an entry for this column.
      bool already = false;
      for (const auto& req : search->offsets) {
        if (req.column_id == col_id) {
          already = true;
          break;
        }
      }
      if (!already) {
        search->offsets.push_back({.column_id = col_id, .projection_index = i});
      }
      changed = true;
      continue;
    }
  }
  return changed;
}

// Pull a LIMIT k upstream into SearchScan.score_top_k when scoring is
// attached. Both LogicalLimit and LogicalTopN are recognised; for TopN
// we only pull the limit if the ORDER BY matches the score column
// (descending). The naive case (LogicalLimit with no ordering) is
// always allowed because iresearch's scored iteration already returns
// the top-k by score.
bool TryAttachScoreTopK(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
  if (plan->type != duckdb::LogicalOperatorType::LOGICAL_LIMIT &&
      plan->type != duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
    return false;
  }
  auto found = FindSearchScanChild(*plan);
  if (!found) {
    return false;
  }
  if (found->search_scan->scorer.kind ==
      connector::SearchScan::ScorerKind::None) {
    return false;  // No scoring requested -- nothing to prune.
  }
  if (found->search_scan->score_top_k) {
    return false;  // Already pulled.
  }
  size_t pulled = 0;
  if (plan->type == duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
    auto& limit_op = plan->Cast<duckdb::LogicalLimit>();
    if (limit_op.limit_val.Type() != duckdb::LimitNodeType::CONSTANT_VALUE) {
      return false;
    }
    pulled = limit_op.limit_val.GetConstantValue();
  } else {
    // LogicalTopN: only pull when single descending order (the score
    // column; runtime stage will tighten to check the order expression
    // actually references our score output).
    auto& top_n = plan->Cast<duckdb::LogicalTopN>();
    if (top_n.limit == 0 || top_n.orders.size() != 1 ||
        top_n.orders[0].type != duckdb::OrderType::DESCENDING) {
      return false;
    }
    pulled = static_cast<size_t>(top_n.limit);
  }
  found->search_scan->score_top_k = pulled;
  // The scored scan already returns top-k rows ordered by score, so
  // the Limit / TopN node is redundant -- drop it. Replace `plan`
  // with its child (skipping the Limit / TopN layer); the caller's
  // walker continues down the new subtree.
  plan = std::move(plan->children[0]);
  return true;
}

// ---------------------------------------------------------------------------
// Walker / dispatcher
// ---------------------------------------------------------------------------

class IresearchPlanOptimizer : public duckdb::OptimizerExtension {
 public:
  IresearchPlanOptimizer() { optimize_function = Optimize; }

 private:
  // True if the rule made a change; useful for chaining but currently
  // unused (the walker keeps going regardless).
  static bool TryOptimize(duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_TOP_N) {
      // ANN topk first; if it doesn't fire the TopN may still be a
      // score-pullup target.
      if (TryAnnTopk(plan)) {
        return true;
      }
      return TryAttachScoreTopK(plan);
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      // ANN range first (its pattern is more specific and won't be
      // misinterpreted by the search-filter path; if it fires the
      // search-filter path won't see a FullTableScan anymore).
      if (TryAnnRange(plan)) {
        return true;
      }
      return TrySearchFilter(plan);
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_PROJECTION) {
      return TryAttachScoreOffsets(plan);
    }
    if (plan->type == duckdb::LogicalOperatorType::LOGICAL_LIMIT) {
      return TryAttachScoreTopK(plan);
    }
    return false;
  }

  // Bottom-up walk with a mutation-context flag: when the current
  // subtree root is a LOGICAL_DELETE / LOGICAL_UPDATE / LOGICAL_MERGE_INTO
  // node, none of its descendant scans get iresearch-rewritten
  // (eventual-consistency constraint).
  static void Walk(duckdb::unique_ptr<duckdb::LogicalOperator>& plan,
                   bool in_mutation) {
    const bool subtree_in_mutation = in_mutation || IsMutationOp(plan->type);
    for (auto& child : plan->children) {
      Walk(child, subtree_in_mutation);
    }
    if (!subtree_in_mutation) {
      TryOptimize(plan);
    }
  }

  static void Optimize(duckdb::OptimizerExtensionInput& /*input*/,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    // Two passes. Pass 1 attaches search filters (TrySearchFilter +
    // TryAnnRange + TryAnnTopk) AND attaches scorer / offsets via
    // TryAttachScoreOffsets when the surrounding LogicalProjection is
    // visited. Pass 2 picks up TryAttachScoreTopK on a LogicalLimit /
    // LogicalTopN that sits between the LogicalProjection and the
    // SearchScan -- in a bottom-up walk those Limit / TopN nodes are
    // visited BEFORE the LogicalProjection that attaches the scorer,
    // so the first-pass topk-pullup can't see the scorer yet. The
    // second pass runs after the scorer attachment is in place.
    Walk(plan, /*in_mutation=*/false);
    Walk(plan, /*in_mutation=*/false);
  }
};

}  // namespace

void RegisterIresearchPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(db.config, IresearchPlanOptimizer());
}

}  // namespace sdb::optimizer
