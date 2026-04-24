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

// Optimizer rule: RocksDB strategy selection.
//
// Walks the plan, finds LogicalFilter -> LogicalGet(serenedb_scan) and, for
// rocksdb-backed predicates (PK or SK), swaps LogicalGet.function and
// bind_data to a specialised scan function.
//
// Strategy selection:
//
//   FROM table_name: build candidate set = { PK, SK_1, SK_2, ... } with
//     each SK's column list from the catalog. Run the PK-filter extractor
//     on the combined WHERE expression against each candidate's column
//     list. Pick the candidate whose extraction result is *best*:
//         Points > Ranges > Full
//         for equal scan types, lower cost wins:
//           points: num_points * (num_columns + sk_extra) is lower
//           ranges: num_columns + sk_extra is lower
//         PK beats SK on a tie (no extra MultiGet needed).
//
//   FROM index_name (secondary index): the candidate set is just that
//     one SK. Inverted indexes are owned by the iresearch_plan rule, so
//     they're ignored here.
//
// On a match we swap LogicalGet.function to one of
//   CreatePkPointScanFunction / CreatePkRangeScanFunction /
//   CreateSkPointScanFunction / CreateSkRangeScanFunction
// and update bind_data.scan_source with the corresponding variant. The
// executor stub still falls through to the full-table loop; specialised
// execution lands in a follow-up Phase 2/4 step.
//
// Because the executor stubs don't yet honour the specialised scan sources,
// the rule keeps the surrounding LogicalFilter intact (i.e. it does not
// claim the matched predicates). The TODO below is where the claim will
// land once the executor catches up.

#include "connector/optimizer/rocksdb_plan.h"

#include <duckdb/main/config.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/optimizer/remove_unused_columns.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <limits>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/optimizer/flatten_projection_ids.h"
#include "connector/rocksdb_filter.hpp"

namespace sdb::optimizer {
namespace {

// One rocksdb-backed access path (PK or one SK).
struct IndexCandidate {
  enum class Kind { Pk, Sk };
  Kind kind;
  std::vector<catalog::Column::Id> column_ids;
  ObjectId sk_shard_id{};  // SK-only
  bool sk_unique = false;  // SK-only
};

// Build a single duckdb::Expression view of the LogicalFilter's
// expressions. Single expression -> no allocation, return a raw pointer.
// Multiple expressions -> wrap them in a synthetic AND (children are
// cloned so the originals remain valid).
[[nodiscard]] const duckdb::Expression* AsCombinedFilterExpr(
  const duckdb::LogicalFilter& filter,
  duckdb::unique_ptr<duckdb::Expression>& owner) {
  if (filter.expressions.size() == 1) {
    return filter.expressions[0].get();
  }
  auto and_expr = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
    duckdb::ExpressionType::CONJUNCTION_AND);
  for (const auto& expr : filter.expressions) {
    and_expr->children.push_back(expr->Copy());
  }
  owner = std::move(and_expr);
  return owner.get();
}

// Resolve the shard ObjectId for a given secondary index on a table, via
// the current catalog snapshot. Returns a null ObjectId if not found.
[[nodiscard]] ObjectId ResolveSkShardId(const catalog::Snapshot& snapshot,
                                        ObjectId table_id,
                                        ObjectId sk_index_id) {
  for (auto& shard : snapshot.GetIndexShardsByTable(table_id)) {
    if (shard->GetIndexId() == sk_index_id) {
      return shard->GetId();
    }
  }
  return ObjectId{};
}

// Build the set of possible indexes for a scan.
[[nodiscard]] std::vector<IndexCandidate> BuildIndexesCandidates(
  const connector::SereneDBScanBindData& bind_data) {
  std::vector<IndexCandidate> out;
  if (!bind_data.table_entry) {
    return out;
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  const auto table_id = bind_data.table->GetId();

  if (const auto* idx_entry =
        dynamic_cast<const connector::SereneDBIndexScanEntry*>(
          &*bind_data.table_entry)) {
    // FROM index_name: only the designated index is eligible. Inverted
    // indexes are handled by the iresearch_plan rule.
    if (!idx_entry->IsSecondaryIndex()) {
      return out;
    }
    for (auto& index : snapshot->GetIndexesByTable(table_id)) {
      if (index->GetType() != catalog::ObjectType::SecondaryIndex) {
        continue;
      }
      auto shard_id = ResolveSkShardId(*snapshot, table_id, index->GetId());
      if (shard_id != idx_entry->GetSecondaryIndexShardId()) {
        continue;
      }
      const auto& sk = basics::downCast<const catalog::SecondaryIndex>(*index);
      auto cols = sk.GetColumnIds();
      out.emplace_back(
        IndexCandidate::Kind::Sk,
        std::vector<catalog::Column::Id>(cols.begin(), cols.end()), shard_id,
        sk.IsUnique());
      break;
    }
    return out;
  }

  // FROM table_name: PK + all rocksdb-backed secondary indexes.
  auto pk_cols = bind_data.table->PKColumns();
  if (!pk_cols.empty()) {
    out.emplace_back(
      IndexCandidate::Kind::Pk,
      std::vector<catalog::Column::Id>(pk_cols.begin(), pk_cols.end()));
  }

  for (auto& index : snapshot->GetIndexesByTable(table_id)) {
    if (index->GetType() != catalog::ObjectType::SecondaryIndex) {
      continue;
    }
    auto shard_id = ResolveSkShardId(*snapshot, table_id, index->GetId());
    if (shard_id == ObjectId{}) {
      continue;  // Shard may be missing mid-drop; skip.
    }
    const auto& sk = basics::downCast<const catalog::SecondaryIndex>(*index);
    auto cols = sk.GetColumnIds();
    out.emplace_back(IndexCandidate::Kind::Sk,
                     std::vector<catalog::Column::Id>(cols.begin(), cols.end()),
                     shard_id, sk.IsUnique());
  }
  return out;
}

struct PhysicalScanCandidate {
  const IndexCandidate* index = nullptr;
  size_t num_columns = 0;
  connector::ExtractAndRewriteResult result;
};

// Scan-type order: Points > Ranges > Full.
// For equal scan types:
//   Points: lower num_points * (num_columns + sk_extra) is better.
//   Ranges/Full: fewer effective columns (num_columns + sk_extra) is better.
// Final tiebreaker: PK beats SK (no extra materialization needed).
[[nodiscard]] bool StrictlyBetter(const PhysicalScanCandidate& lhs,
                                  const PhysicalScanCandidate& rhs) {
  // ConstraintKind values are ordered so higher = better: Points > Ranges >
  // None.
  if (lhs.result.kind != rhs.result.kind) {
    return lhs.result.kind > rhs.result.kind;
  }

  auto effective_cols = [](const PhysicalScanCandidate& c) {
    return c.num_columns + (c.index->kind == IndexCandidate::Kind::Sk ? 1 : 0);
  };
  auto cols_l = effective_cols(lhs);
  auto cols_r = effective_cols(rhs);

  if (lhs.result.kind == connector::ConstraintKind::Points) {
    auto cost_l = lhs.result.constraints.size() * cols_l;
    auto cost_r = rhs.result.constraints.size() * cols_r;
    if (cost_l != cost_r) {
      return cost_l < cost_r;
    }
  } else if (lhs.result.kind == connector::ConstraintKind::Ranges &&
             lhs.index->kind == IndexCandidate::Kind::Sk &&
             rhs.index->kind == IndexCandidate::Kind::Sk) {
    SDB_ASSERT(cols_l == cols_r);
    // Two SK range scans with equal effective cols: prefer the wider index.
    // We choose wider index as best effort, it's not generally better.
    // TODO(mkornaukhov) look at column projections in the remaining filters and
    // statistics for better choice.
    auto idx_cols_l = lhs.index->column_ids.size();
    auto idx_cols_r = rhs.index->column_ids.size();
    return idx_cols_l > idx_cols_r;
  }

  // Otherwise prefer PK over SK version
  return lhs.index->kind == IndexCandidate::Kind::Pk &&
         rhs.index->kind == IndexCandidate::Kind::Sk;
}

class RocksDBPlanOptimizer : public duckdb::OptimizerExtension {
 public:
  RocksDBPlanOptimizer() { optimize_function = Optimize; }

  // Try to rewrite a LogicalFilter -> LogicalGet(serenedb_scan) into a
  // PK / SK specialised scan.
  static bool TryOptimize(duckdb::ClientContext& /*context*/,
                          duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    if (plan->type != duckdb::LogicalOperatorType::LOGICAL_FILTER) {
      return false;
    }
    auto& filter = plan->Cast<duckdb::LogicalFilter>();
    if (filter.children.size() != 1 ||
        filter.children[0]->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
      return false;
    }
    auto& get = filter.children[0]->Cast<duckdb::LogicalGet>();
    if (!get.bind_data ||
        !dynamic_cast<connector::SereneDBScanBindData*>(&*get.bind_data)) {
      return false;
    }
    auto& bind_data = get.bind_data->Cast<connector::SereneDBScanBindData>();
    if (!bind_data.table) {
      return false;
    }
    if (filter.expressions.empty()) {
      return false;
    }

    // We only consider entries whose scan_source is either the default
    // FullTableScan (regular table) or a SecondaryIndexScan (FROM
    // sk_index_name). iresearch-claimed scans, ANN, range-search, and
    // already-specialised pk/sk scans stay as-is.
    const auto kind = bind_data.scan_source->Kind();
    if (kind != connector::ScanSourceKind::FullTable &&
        kind != connector::ScanSourceKind::SecondaryIndex) {
      return false;
    }

    auto indexes = BuildIndexesCandidates(bind_data);
    if (indexes.empty()) {
      return false;
    }

    // Build the resolver's projected column ids: indexed by the filter's
    // binding.column_index after DuckDB's projection pushdown may have
    // reordered get.column_ids. get.column_ids[k] gives the physical column
    // index into bind_data.column_ids, which yields the catalog::Column::Id.
    constexpr auto kInvalidId = std::numeric_limits<catalog::Column::Id>::max();
    std::vector<catalog::Column::Id> projected_column_ids;
    projected_column_ids.reserve(get.GetColumnIds().size());
    for (const auto& ci : get.GetColumnIds()) {
      if (!ci.HasPrimaryIndex()) {
        projected_column_ids.push_back(kInvalidId);
        continue;
      }
      auto phys = ci.GetPrimaryIndex();
      projected_column_ids.push_back(phys < bind_data.column_ids.size()
                                       ? bind_data.column_ids[phys]
                                       : kInvalidId);
    }
    connector::ColumnResolver resolver{get.table_index, projected_column_ids};

    // Build the combined filter expression once; each index reads it read-only.
    duckdb::unique_ptr<duckdb::Expression> synthetic;
    const auto* combined = AsCombinedFilterExpr(filter, synthetic);

    // Evaluate every index and pick the best candidate.
    PhysicalScanCandidate best;
    for (auto& idx : indexes) {
      auto result = connector::ExtractAndRewriteFilterExpr(
        *combined, idx.column_ids, resolver,
        idx.kind == IndexCandidate::Kind::Pk, idx.sk_unique);
      if (idx.kind == IndexCandidate::Kind::Pk &&
          result.kind == connector::ConstraintKind::None) {
        continue;
      }

      PhysicalScanCandidate cand{&idx, projected_column_ids.size(),
                                 std::move(result)};
      if (!best.index || StrictlyBetter(cand, best)) {
        best = std::move(cand);
      }
    }

    if (!best.index) {
      return false;
    }

    // Convert the winning constraints to runtime-ready form.
    const auto& cols = best.index->column_ids;
    std::vector<connector::ResolvedPoint> points;
    std::vector<connector::ResolvedRange> ranges;
    if (best.result.kind == connector::ConstraintKind::Points) {
      points = connector::ToSortedResolvedPoints(best.result.constraints, cols);
    } else {
      ranges = connector::ToSortedDisjointRanges(best.result.constraints, cols);
    }

    auto remove_extra_filter = [&]() {
      filter.expressions.clear();
      if (best.result.remaining_filter) {
        filter.expressions.push_back(std::move(best.result.remaining_filter));
      }
      if (filter.expressions.empty()) {
        plan = std::move(filter.children[0]);
      }
    };

    // Swap function + bind_data based on the winner.
    if (best.index->kind == IndexCandidate::Kind::Pk) {
      if (best.result.kind == connector::ConstraintKind::Points) {
        auto pk = std::make_unique<connector::PkPointScan>();
        pk->column_ids = cols;
        pk->points = std::move(points);
        bind_data.scan_source = std::move(pk);
        get.function = connector::CreatePKPointsLookupFunction();
        remove_extra_filter();
      } else {
        auto pk = std::make_unique<connector::PkRangeScan>();
        pk->column_ids = cols;
        pk->ranges = std::move(ranges);
        bind_data.scan_source = std::move(pk);
        get.function = connector::CreatePKRangesScanFunction();
        remove_extra_filter();
      }
    } else {
      if (best.result.kind == connector::ConstraintKind::Points) {
        auto sk = std::make_unique<connector::SkPointScan>();
        sk->shard_id = best.index->sk_shard_id;
        sk->is_unique = best.index->sk_unique;
        sk->column_ids = cols;
        sk->points = std::move(points);
        bind_data.scan_source = std::move(sk);
        get.function = connector::CreateSKPointsLookupFunction();
        remove_extra_filter();
      } else if (best.result.kind == connector::ConstraintKind::Ranges) {
        auto sk = std::make_unique<connector::SkRangeScan>();
        sk->shard_id = best.index->sk_shard_id;
        sk->is_unique = best.index->sk_unique;
        sk->column_ids = cols;
        sk->ranges = std::move(ranges);
        bind_data.scan_source = std::move(sk);
        get.function = connector::CreateSKRangesScanFunction();
        remove_extra_filter();
      } else {
        auto si = std::make_unique<connector::SecondaryIndexScan>();
        si->shard_id = best.index->sk_shard_id;
        si->is_unique = best.index->sk_unique;
        bind_data.scan_source = std::move(si);
      }
    }

    return true;
  }

  static bool OptimizeChildren(
    duckdb::ClientContext& context,
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    bool changed = false;
    for (auto& child : plan->children) {
      changed |= OptimizeChildren(context, child);
    }
    changed |= TryOptimize(context, plan);
    return changed;
  }

  // Post-walk cleanup: any LogicalGet we swapped to a filter_prune=false
  // variant carries stale projection_ids if an earlier optimizer pass
  // populated them (e.g. parquet/json external tables run filter_prune=true
  // before the swap). Collapse projection_ids into column_ids so the
  // subsequent RemoveUnusedColumns pass doesn't leave dangling indices --
  // see flatten_projection_ids.h.
  static void FlattenSwappedGets(
    duckdb::LogicalOperator& root,
    duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    for (auto& child : plan->children) {
      FlattenSwappedGets(root, child);
    }
    if (plan->type != duckdb::LogicalOperatorType::LOGICAL_GET) {
      return;
    }
    auto& get = plan->Cast<duckdb::LogicalGet>();
    if (get.projection_ids.empty() || get.function.filter_prune) {
      return;
    }
    FlattenProjectionIds(root, get);
  }

  static void Optimize(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    bool changed = OptimizeChildren(input.context, plan);
    if (changed) {
      FlattenSwappedGets(*plan, plan);
      // ColumnLifetimeAnalyzer ran before us and populated projection_maps
      // on LogicalFilter/LogicalOrder/LogicalJoin based on the pre-swap
      // key positions. Our filter mutations + the follow-up
      // RemoveUnusedColumns will shift column_ids, leaving those maps
      // pointing at positions that no longer exist. Wipe them so
      // RemoveUnusedColumns operates on a clean slate.
      ClearProjectionMaps(*plan);
      duckdb::RemoveUnusedColumns unused{input.optimizer};
      unused.VisitOperator(plan);
    }
  }
};

}  // namespace

void RegisterRocksDBPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(db.config, RocksDBPlanOptimizer());
}

}  // namespace sdb::optimizer
