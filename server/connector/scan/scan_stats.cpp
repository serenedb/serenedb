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

#include <duckdb/common/types/variant.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/string_stats.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>

#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"

namespace sdb::connector {
namespace {

const duckdb::BaseStatistics* StoredColumnStats(
  const ScanBindData& bind, const irs::DirectoryReader& reader,
  ColumnId col_id) {
  if (bind.relation.IsInvertedIndex()) {
    const auto* info = bind.relation.ScannedIndex().FindColumnInfo(col_id);
    if (!info || !info->store_values) {
      return nullptr;
    }
  } else if (col_id > kMaxRealColumnIdValue) {
    return nullptr;
  }
  return reader.GetColumnStats(col_id);
}

const irs::FieldTermRange* KeywordTermRange(const ScanBindData& bind,
                                            const irs::DirectoryReader& reader,
                                            duckdb::idx_t column_index) {
  if (!bind.relation.inverted_config ||
      bind.columns.types[column_index] != duckdb::LogicalType::VARCHAR) {
    return nullptr;
  }
  const auto& index = bind.relation.ScannedIndex();
  const auto field_id = index.TermField(bind.columns.ids[column_index]);
  return index.IsKeywordField(field_id) ? reader.GetTermRange(field_id)
                                        : nullptr;
}

duckdb::string_t AsStringT(irs::bytes_view bytes) {
  return {reinterpret_cast<const char*>(bytes.data()),
          static_cast<uint32_t>(bytes.size())};
}

duckdb::unique_ptr<duckdb::BaseStatistics> ColumnStats(
  const ScanBindData& bind, const irs::DirectoryReader& reader,
  duckdb::idx_t column_index) {
  if (column_index >= bind.columns.ids.size()) {
    return nullptr;
  }
  const auto* stored =
    StoredColumnStats(bind, reader, bind.columns.ids[column_index]);
  const auto* bounds = KeywordTermRange(bind, reader, column_index);
  if (!bounds) {
    return stored ? stored->ToUnique() : nullptr;
  }
  auto stats =
    stored ? stored->ToUnique()
           : duckdb::StringStats::CreateUnknown(duckdb::LogicalType::VARCHAR)
               .ToUnique();
  duckdb::StringStats::SetMin(*stats, AsStringT(bounds->min),
                              duckdb::StringStatsType::EXACT_STATS);
  duckdb::StringStats::SetMax(*stats, AsStringT(bounds->max),
                              duckdb::StringStatsType::EXACT_STATS);
  return stats;
}

class ScanPartition final : public duckdb::PartitionRowGroup {
 public:
  explicit ScanPartition(const ScanBindData& bind,
                         search::InvertedIndexSnapshotPtr snapshot)
    : _bind{bind}, _snapshot{std::move(snapshot)} {}

  duckdb::unique_ptr<duckdb::BaseStatistics> GetColumnStatistics(
    const duckdb::StorageIndex& index) final {
    if (index.HasChildren()) {
      return nullptr;
    }
    return ColumnStats(_bind, _snapshot->reader,
                       ScanColumnOfStorage(_bind, index.GetPrimaryIndex()));
  }

  bool MinMaxIsExact(const duckdb::StorageIndex&) final { return true; }

  bool HasPendingWrites() final { return false; }

 private:
  const ScanBindData& _bind;
  search::InvertedIndexSnapshotPtr _snapshot;
};

}  // namespace

duckdb::vector<duckdb::PartitionStatistics> IResearchScanPartitionStats(
  duckdb::ClientContext& context, duckdb::GetPartitionStatsInput& input) {
  duckdb::vector<duckdb::PartitionStatistics> result;
  if (!input.bind_data) {
    return result;
  }
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  if (!bind.ReadsEveryRow()) {
    return result;
  }
  auto snapshot = bind.Snapshot(context);
  const auto& reader = snapshot->reader;
  auto& stats = result.emplace_back();
  stats.count = reader.live_docs_count();
  stats.count_type = duckdb::CountType::COUNT_EXACT;
  if (reader.docs_count() == reader.live_docs_count()) {
    stats.partition_row_group =
      duckdb::make_shared_ptr<ScanPartition>(bind, std::move(snapshot));
  }
  return result;
}

duckdb::unique_ptr<duckdb::BaseStatistics> IResearchScanStatistics(
  duckdb::ClientContext& context,
  duckdb::TableFunctionGetStatisticsInput& input) {
  if (!input.bind_data) {
    return nullptr;
  }
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  if (!input.column_index.HasPrimaryIndex()) {
    return nullptr;
  }
  const auto column_index = input.column_index.GetPrimaryIndex();
  const auto snapshot = bind.Snapshot(context);
  if (!input.column_index.HasChildren()) {
    auto stats = ColumnStats(bind, snapshot->reader, column_index);
    SDB_ASSERT(!stats || stats->GetType() == bind.columns.types[column_index]);
    return stats;
  }
  if (column_index >= bind.columns.ids.size()) {
    return nullptr;
  }
  const auto* stats =
    StoredColumnStats(bind, snapshot->reader, bind.columns.ids[column_index]);
  if (!stats) {
    return nullptr;
  }
  const duckdb::BaseStatistics* leaf = stats;
  const duckdb::ColumnIndex* node = &input.column_index;
  if (bind.columns.types[column_index].id() == duckdb::LogicalTypeId::VARIANT) {
    if (!duckdb::VariantStats::IsShredded(*stats)) {
      return nullptr;
    }
    leaf = &duckdb::VariantStats::GetShreddedStats(*stats);
    while (node->HasChildren()) {
      node = &node->GetChildIndex(0);
      if (node->HasPrimaryIndex()) {
        return nullptr;
      }
      const duckdb::VariantPathComponent comp{node->GetFieldName()};
      const auto child =
        duckdb::VariantShreddedStats::FindChildStats(*leaf, comp);
      if (!child) {
        return nullptr;
      }
      leaf = child.get();
    }
  } else {
    while (node->HasChildren()) {
      node = &node->GetChildIndex(0);
      if (!node->HasPrimaryIndex() ||
          leaf->GetType().id() != duckdb::LogicalTypeId::STRUCT) {
        return nullptr;
      }
      const auto field = node->GetPrimaryIndex();
      if (field >= duckdb::StructType::GetChildCount(leaf->GetType())) {
        return nullptr;
      }
      leaf = &duckdb::StructStats::GetChildStats(*leaf, field);
    }
  }
  if (leaf->GetType().IsNested() || !input.column_index.HasType()) {
    return nullptr;
  }
  const auto& want = input.column_index.GetScanType();
  if (leaf->GetType() == want) {
    return leaf->ToUnique();
  }
  if (leaf->GetType().IsNumeric() && want.IsNumeric() &&
      duckdb::NumericStats::HasMinMax(*leaf)) {
    const auto cmin = duckdb::NumericStats::Min(*leaf).DefaultTryCastAs(want);
    const auto cmax = duckdb::NumericStats::Max(*leaf).DefaultTryCastAs(want);
    if (cmin && cmax) {
      auto casted = duckdb::NumericStats::CreateEmpty(want);
      duckdb::NumericStats::SetMin(casted, *cmin);
      duckdb::NumericStats::SetMax(casted, *cmax);
      return casted.ToUnique();
    }
  }
  return nullptr;
}

}  // namespace sdb::connector
