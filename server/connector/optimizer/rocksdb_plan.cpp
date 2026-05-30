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

#include "connector/optimizer/rocksdb_plan.h"

#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <limits>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/rocksdb_filter.hpp"
#include "pg/connection_context.h"

namespace sdb::optimizer {
namespace {

struct IndexCandidate {
  enum class Kind { Pk, Sk };
  Kind kind;
  std::vector<catalog::Column::Id> column_ids;
  ObjectId sk_shard_id{};  // SK-only
  bool sk_unique = false;  // SK-only
};

[[nodiscard]] duckdb::unique_ptr<duckdb::Expression> BuildCombinedFilterExpr(
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  auto and_expr = duckdb::make_uniq<duckdb::BoundConjunctionExpression>(
    duckdb::ExpressionType::CONJUNCTION_AND);
  for (const auto& expr : filters) {
    and_expr->children.push_back(expr->Copy());
  }
  return std::move(and_expr);
}

[[nodiscard]] ObjectId ResolveSkShardId(const catalog::Snapshot& snapshot,
                                        ObjectId sk_index_id) {
  if (auto shard = snapshot.GetIndexShard(sk_index_id)) {
    return shard->GetId();
  }
  return ObjectId{};
}

[[nodiscard]] std::vector<IndexCandidate> BuildIndexesCandidates(
  const connector::SereneDBScanBindData& bind_data,
  duckdb::ClientContext& context) {
  std::vector<IndexCandidate> out;
  if (!bind_data.table_entry) {
    return out;
  }
  const auto& tbd = bind_data.As<connector::TableScanBindData>();

  auto snapshot =
    connector::GetSereneDBContext(context).EnsureCatalogSnapshot();
  const auto table_id = tbd.table->GetId();

  if (bind_data.entry_kind == connector::ScanEntryKind::SecondaryIndex) {
    const auto& idx_entry =
      basics::downCast<const connector::TableSecondaryIndexScanEntry>(
        *bind_data.table_entry);
    for (auto& index : snapshot->GetIndexesByRelation(table_id)) {
      if (index->GetType() != catalog::ObjectType::SecondaryIndex) {
        continue;
      }
      auto shard_id = ResolveSkShardId(*snapshot, index->GetId());
      if (shard_id != idx_entry.GetSecondaryIndexShardId()) {
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
  if (bind_data.IsInvertedIndexEntry()) {
    return out;
  }

  auto pk_cols = tbd.table->PKColumns();
  if (!pk_cols.empty()) {
    out.emplace_back(
      IndexCandidate::Kind::Pk,
      std::vector<catalog::Column::Id>(pk_cols.begin(), pk_cols.end()));
  }

  for (auto& index : snapshot->GetIndexesByRelation(table_id)) {
    if (index->GetType() != catalog::ObjectType::SecondaryIndex) {
      continue;
    }
    auto shard_id = ResolveSkShardId(*snapshot, index->GetId());
    if (shard_id == ObjectId{}) {
      continue;
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

// Order: Points > Ranges > Full; lower cost wins on ties; PK beats SK.
[[nodiscard]] bool StrictlyBetter(const PhysicalScanCandidate& lhs,
                                  const PhysicalScanCandidate& rhs) {
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
    // TODO(mkornaukhov): use stats + filter projections; wider index is a
    // heuristic.
    auto idx_cols_l = lhs.index->column_ids.size();
    auto idx_cols_r = rhs.index->column_ids.size();
    return idx_cols_l > idx_cols_r;
  }

  return lhs.index->kind == IndexCandidate::Kind::Pk &&
         rhs.index->kind == IndexCandidate::Kind::Sk;
}

}  // namespace

void RocksDBPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (filters.empty() || bind_data_ptr == nullptr) {
    return;
  }
  auto& bind_data = bind_data_ptr->Cast<connector::SereneDBScanBindData>();
  if (bind_data.IsViewBacked()) {
    return;
  }

  const auto kind = bind_data.scan_source->Kind();
  if (kind != connector::ScanSourceKind::FullTable &&
      kind != connector::ScanSourceKind::SecondaryIndex) {
    return;
  }

  auto indexes = BuildIndexesCandidates(bind_data, context);
  if (indexes.empty()) {
    return;
  }

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

  duckdb::unique_ptr<duckdb::Expression> synthetic;
  const duckdb::Expression* combined = nullptr;
  if (filters.size() == 1) {
    combined = filters[0].get();
  } else {
    synthetic = BuildCombinedFilterExpr(filters);
    combined = synthetic.get();
  }

  // Evaluate every index and pick the best candidate.
  PhysicalScanCandidate best;
  for (auto& idx : indexes) {
    auto result = connector::ExtractAndRewriteFilterExpr(
      *combined, idx.column_ids, resolver, idx.kind == IndexCandidate::Kind::Pk,
      idx.sk_unique);
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
    return;
  }

  // Convert the winning constraints to runtime-ready form.
  const auto& cols = best.index->column_ids;
  std::vector<connector::ResolvedPoint> points;
  std::vector<connector::ResolvedRange> ranges;
  if (best.result.kind == connector::ConstraintKind::Points) {
    points = connector::ToSortedResolvedPoints(best.result.constraints, cols);
  } else if (best.result.kind == connector::ConstraintKind::Ranges) {
    ranges = connector::ToSortedDisjointRanges(best.result.constraints, cols);
  }

  auto claim_filters = [&]() {
    filters.clear();
    if (best.result.remaining_filter) {
      filters.push_back(std::move(best.result.remaining_filter));
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
      claim_filters();
    } else {
      auto pk = std::make_unique<connector::PkRangeScan>();
      pk->column_ids = cols;
      pk->ranges = std::move(ranges);
      bind_data.scan_source = std::move(pk);
      get.function = connector::CreatePKRangesScanFunction();
      claim_filters();
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
      claim_filters();
    } else if (best.result.kind == connector::ConstraintKind::Ranges) {
      auto sk = std::make_unique<connector::SkRangeScan>();
      sk->shard_id = best.index->sk_shard_id;
      sk->is_unique = best.index->sk_unique;
      sk->column_ids = cols;
      sk->ranges = std::move(ranges);
      bind_data.scan_source = std::move(sk);
      get.function = connector::CreateSKRangesScanFunction();
      claim_filters();
    } else {
      auto si = std::make_unique<connector::SecondaryIndexScan>();
      si->shard_id = best.index->sk_shard_id;
      si->is_unique = best.index->sk_unique;
      bind_data.scan_source = std::move(si);
    }
  }
}

}  // namespace sdb::optimizer
