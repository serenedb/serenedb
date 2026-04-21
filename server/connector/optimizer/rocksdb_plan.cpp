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
//         Points > Ranges > None
//         fewer remaining post-claim predicates is better
//         PK is preferred over SK on a tie (the PK is already the
//         primary covering key, so driving the scan from it avoids an
//         extra MultiGet step).
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
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <limits>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/rocksdb_filter.hpp"

namespace sdb::optimizer {
namespace {

// Candidate = one rocksdb-backed access path (PK or one SK).
struct Candidate {
  enum class Kind { Pk, Sk };
  Kind kind;
  // Columns in index order (for PK: table.PKColumns(); for SK: the SK's
  // leading column id sequence).
  std::vector<catalog::Column::Id> column_ids;
  // SK-only fields; default-constructed for PK candidates.
  ObjectId sk_shard_id;
  bool sk_unique = false;
};

// Counts non-trivial leaves of the remaining filter so we can prefer
// strategies that claim more predicates.
size_t CountRemainingLeaves(const duckdb::Expression* expr) {
  if (!expr) {
    return 0;
  }
  if (expr->expression_class == duckdb::ExpressionClass::BOUND_CONJUNCTION) {
    const auto& conj = expr->Cast<duckdb::BoundConjunctionExpression>();
    if (conj.type == duckdb::ExpressionType::CONJUNCTION_AND) {
      size_t total = 0;
      for (const auto& child : conj.children) {
        total += CountRemainingLeaves(child.get());
      }
      return total;
    }
  }
  return 1;
}

// Build a single duckdb::Expression view of the LogicalFilter's
// expressions. Single expression -> no allocation, return a raw pointer.
// Multiple expressions -> wrap them in a synthetic AND (children are
// cloned so the originals remain valid).
const duckdb::Expression* AsCombinedFilterExpr(
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
ObjectId ResolveSkShardId(const catalog::Snapshot& snapshot, ObjectId table_id,
                          ObjectId sk_index_id) {
  for (auto& shard : snapshot.GetIndexShardsByTable(table_id)) {
    if (shard->GetIndexId() == sk_index_id) {
      return shard->GetId();
    }
  }
  return ObjectId{};
}

// Build the candidate set for a scan.
std::vector<Candidate> BuildCandidates(
  const connector::SereneDBScanBindData& bind_data) {
  std::vector<Candidate> out;
  if (!bind_data.table_entry) {
    return out;
  }

  if (const auto* idx_entry =
        dynamic_cast<const connector::SereneDBIndexScanEntry*>(
          &*bind_data.table_entry)) {
    // FROM index_name: only the designated index is a candidate. Inverted
    // indexes are handled by the iresearch_plan rule.
    if (!idx_entry->IsSecondaryIndex()) {
      return out;
    }
    auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
    const auto table_id = bind_data.table->GetId();
    // Find the catalog::SecondaryIndex that owns this shard.
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
      Candidate c;
      c.kind = Candidate::Kind::Sk;
      c.column_ids.assign(cols.begin(), cols.end());
      c.sk_shard_id = shard_id;
      c.sk_unique = sk.IsUnique();
      out.push_back(std::move(c));
      break;
    }
    return out;
  }

  // FROM table_name: PK + all rocksdb-backed secondary indexes.
  auto pk_cols = bind_data.table->PKColumns();
  if (!pk_cols.empty()) {
    Candidate pk;
    pk.kind = Candidate::Kind::Pk;
    pk.column_ids.assign(pk_cols.begin(), pk_cols.end());
    out.push_back(std::move(pk));
  }

  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  const auto table_id = bind_data.table->GetId();

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
    Candidate c;
    c.kind = Candidate::Kind::Sk;
    c.column_ids.assign(cols.begin(), cols.end());
    c.sk_shard_id = shard_id;
    c.sk_unique = sk.IsUnique();
    out.push_back(std::move(c));
  }
  return out;
}

// Evaluated candidate: the extraction result plus its tiebreak metrics.
// Owns the extraction result so the winner can be converted to resolved
// points / ranges after selection.
struct Evaluated {
  const Candidate* candidate = nullptr;
  connector::ConstraintKind kind = connector::ConstraintKind::None;
  size_t remaining = std::numeric_limits<size_t>::max();
  connector::ExtractAndRewriteResult result;
};

// Is `a` a strictly better evaluated candidate than `b`?
//   Points beats Ranges (scan precision).
//   Fewer remaining-filter leaves beats more (more predicates claimed).
//   PK beats SK on a tie (PK drives the scan without an extra MultiGet).
bool StrictlyBetter(const Evaluated& a, const Evaluated& b) {
  if (a.kind != b.kind) {
    return a.kind == connector::ConstraintKind::Points;
  }
  if (a.remaining != b.remaining) {
    return a.remaining < b.remaining;
  }
  return a.candidate->kind == Candidate::Kind::Pk &&
         b.candidate->kind == Candidate::Kind::Sk;
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

    auto candidates = BuildCandidates(bind_data);
    if (candidates.empty()) {
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

    // Build the combined filter expression once; each candidate reads it
    // read-only.
    duckdb::unique_ptr<duckdb::Expression> synthetic;
    const auto* combined = AsCombinedFilterExpr(filter, synthetic);

    // TODO(mkornaukhov) rewrite mechanism that choose best index
    // in the way discussed with mbkkt

    // Evaluate every candidate and pick the best. We own the extraction
    // result because ExtractAndRewriteResult holds a unique_ptr (remaining
    // filter), so updates to `best` move-assign.
    Evaluated best;
    for (auto& cand : candidates) {
      auto result = connector::ExtractAndRewriteFilterExpr(
        *combined, cand.column_ids, resolver,
        /*is_primary_key=*/cand.kind == Candidate::Kind::Pk,
        /*is_unique=*/cand.sk_unique);
      if (cand.kind == Candidate::Kind::Pk &&
          result.kind == connector::ConstraintKind::None) {
        continue;
      }

      Evaluated ev;
      ev.candidate = &cand;
      ev.kind = result.kind;
      ev.remaining = CountRemainingLeaves(result.remaining_filter.get());
      ev.result = std::move(result);
      if (!best.candidate || StrictlyBetter(ev, best)) {
        best = std::move(ev);
      }
    }

    if (!best.candidate) {
      return false;
    }

    // Convert the winning extractor constraints to runtime-ready form
    // (resolved Values per point / prefix + ColumnRange per range) so
    // both to_string and the eventual executor have structured data.
    const auto& cols = best.candidate->column_ids;
    std::vector<connector::ResolvedPoint> points;
    std::vector<connector::ResolvedRange> ranges;
    if (best.kind == connector::ConstraintKind::Points) {
      points = connector::ToResolvedPoints(best.result.constraints, cols);
      connector::SortAndDedupPoints(points);
    } else {
      ranges = connector::ToDisjointRanges(best.result.constraints, cols);
    }

    // TODO(phase2/4-follow-up): call this when
    // scan will emit already filtered rows
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
    if (best.candidate->kind == Candidate::Kind::Pk) {
      if (best.kind == connector::ConstraintKind::Points) {
        auto pk = std::make_unique<connector::PkPointScan>();
        pk->column_ids = cols;
        pk->points = std::move(points);
        bind_data.scan_source = std::move(pk);
        get.function = connector::CreatePkPointScanFunction();
        remove_extra_filter();
      } else {
        auto pk = std::make_unique<connector::PkRangeScan>();
        pk->column_ids = cols;
        pk->ranges = std::move(ranges);
        bind_data.scan_source = std::move(pk);
        get.function = connector::CreatePkRangeScanFunction();
        remove_extra_filter();
      }
    } else {
      if (best.kind == connector::ConstraintKind::Points) {
        auto sk = std::make_unique<connector::SkPointScan>();
        sk->shard_id = best.candidate->sk_shard_id;
        sk->is_unique = best.candidate->sk_unique;
        sk->column_ids = cols;
        sk->points = std::move(points);
        bind_data.scan_source = std::move(sk);
        get.function = connector::CreateSkPointScanFunction();
        remove_extra_filter();
      } else if (best.kind == connector::ConstraintKind::Ranges) {
        auto sk = std::make_unique<connector::SkRangeScan>();
        sk->shard_id = best.candidate->sk_shard_id;
        sk->is_unique = best.candidate->sk_unique;
        sk->column_ids = cols;
        sk->ranges = std::move(ranges);
        bind_data.scan_source = std::move(sk);
        get.function = connector::CreateSkRangeScanFunction();
        remove_extra_filter();
      } else {
        auto si = std::make_unique<connector::SecondaryIndexScan>();
        si->shard_id = best.candidate->sk_shard_id;
        si->is_unique = best.candidate->sk_unique;
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

  static void Optimize(duckdb::OptimizerExtensionInput& input,
                       duckdb::unique_ptr<duckdb::LogicalOperator>& plan) {
    OptimizeChildren(input.context, plan);
  }
};

}  // namespace

void RegisterRocksDBPlanOptimizer(duckdb::DatabaseInstance& db) {
  duckdb::OptimizerExtension::Register(db.config, RocksDBPlanOptimizer());
}

}  // namespace sdb::optimizer
