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
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>

#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::BaseStatistics> IResearchScanStatistics(
  duckdb::ClientContext&, duckdb::TableFunctionGetStatisticsInput& input) {
  if (!input.bind_data) {
    return nullptr;
  }
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  if (!input.column_index.HasPrimaryIndex()) {
    return nullptr;
  }
  const auto column_index = input.column_index.GetPrimaryIndex();
  if (column_index >= bind.columns.ids.size()) {
    return nullptr;
  }
  const auto col_id = bind.columns.ids[column_index];
  if (bind.relation.IsInvertedIndex()) {
    const auto* info = bind.relation.ScannedIndex().FindColumnInfo(col_id);
    if (!info || !info->store_values) {
      return nullptr;
    }
  } else if (bind.relation.IsSearchTable()) {
    if (col_id.id() > catalog::kMaxRealColumnIdValue) {
      return nullptr;
    }
  } else {
    return nullptr;
  }
  if (!bind.search.snapshot) {
    return nullptr;
  }
  const auto* stats = bind.search.snapshot->reader.GetColumnStats(col_id);
  if (stats == nullptr) {
    return nullptr;
  }
  if (!input.column_index.HasChildren()) {
    SDB_ASSERT(stats->GetType() == bind.columns.types[column_index]);
    return stats->ToUnique();
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
    duckdb::Value cmin;
    duckdb::Value cmax;
    if (duckdb::NumericStats::Min(*leaf).DefaultTryCastAs(want, cmin,
                                                          nullptr) &&
        duckdb::NumericStats::Max(*leaf).DefaultTryCastAs(want, cmax,
                                                          nullptr)) {
      auto casted = duckdb::NumericStats::CreateEmpty(want);
      duckdb::NumericStats::SetMin(casted, cmin);
      duckdb::NumericStats::SetMax(casted, cmax);
      return casted.ToUnique();
    }
  }
  return nullptr;
}

}  // namespace sdb::connector
