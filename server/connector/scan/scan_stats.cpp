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

#include <absl/algorithm/container.h>

#include <cmath>
#include <duckdb/common/types/variant.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/string_stats.hpp>
#include <duckdb/storage/statistics/struct_stats.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>

#include "catalog/entry/search_table.h"
#include "connector/constraint_keys.h"
#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"
#include "search/key_spec.h"

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

duckdb::string_t AsStringT(irs::bytes_view bytes) {
  return {reinterpret_cast<const char*>(bytes.data()),
          static_cast<uint32_t>(bytes.size())};
}

uint64_t RowsOnlyDocs(const irs::DirectoryReader& reader) {
  uint64_t docs = 0;
  for (const auto& segment : reader) {
    if (RowsOnlySegment(segment)) {
      docs += segment.docs_count();
    }
  }
  return docs;
}

uint64_t DistinctWith(const irs::FieldTermRange& terms, uint64_t unseen) {
  if (unseen == 0 || terms.docs == 0) {
    return terms.distinct;
  }
  const auto seen = static_cast<double>(std::min(terms.distinct, terms.docs));
  const auto sampled = static_cast<double>(terms.docs);
  const auto once = std::pow(seen / sampled, 2) * seen;
  return static_cast<uint64_t>(seen +
                               once / sampled * static_cast<double>(unseen));
}

bool ExactBounds(const duckdb::BaseStatistics& stats) {
  const auto exact = [](duckdb::StringStatsType type) {
    return type == duckdb::StringStatsType::EXACT_STATS ||
           type == duckdb::StringStatsType::EMPTY_STATS;
  };
  return exact(duckdb::StringStats::GetMinType(stats)) &&
         exact(duckdb::StringStats::GetMaxType(stats));
}

duckdb::unique_ptr<duckdb::BaseStatistics> KeywordBounds(
  const ScanBindData& bind, const irs::DirectoryReader& reader,
  duckdb::idx_t column_index, uint64_t& distinct) {
  if (!bind.relation.inverted_config ||
      bind.columns.types[column_index] != duckdb::LogicalType::VARCHAR) {
    return nullptr;
  }
  const auto& index = bind.relation.ScannedIndex();
  const auto column = bind.columns.ids[column_index];
  const auto field_id = index.TermField(column);
  if (!index.IsKeywordField(field_id)) {
    return nullptr;
  }
  auto bounds =
    duckdb::StringStats::CreateEmpty(duckdb::LogicalType::VARCHAR).ToUnique();
  if (const auto* terms =
        reader.GetTermRange(field_id, irs::StringLayout::kStrings)) {
    duckdb::StringStats::SetMin(*bounds, AsStringT(terms->min),
                                duckdb::StringStatsType::EXACT_STATS);
    duckdb::StringStats::SetMax(*bounds, AsStringT(terms->max),
                                duckdb::StringStatsType::EXACT_STATS);
    distinct = DistinctWith(*terms, RowsOnlyDocs(reader));
  }
  for (const auto& segment : reader) {
    if (!RowsOnlySegment(segment)) {
      continue;
    }
    const auto* columns = segment.GetColReader();
    const auto* values = columns ? columns->Column(column) : nullptr;
    if (!values || !ExactBounds(values->MergedStatistics())) {
      return nullptr;
    }
    bounds->Merge(values->MergedStatistics());
  }
  return duckdb::StringStats::HasMinMax(*bounds) ? std::move(bounds) : nullptr;
}

uint64_t KeyDistinct(const ScanBindData& bind,
                     const irs::DirectoryReader& reader, ColumnId column) {
  if (!bind.relation.IsSearchTable() || !bind.relation.inverted_config) {
    return 0;
  }
  const auto& specs =
    bind.relation.table_entry->Cast<catalog::SearchTableEntry>().KeySpecs();
  for (const auto& key : bind.relation.ScannedIndex().constraint_keys) {
    if (key.columns.size() != 1 || key.columns.front() != column) {
      continue;
    }
    const auto spec = absl::c_find_if(specs, [&](const search::KeySpec& spec) {
      return spec.key.field == key.field;
    });
    if (spec == specs.end() || spec->kind == search::KeyKind::Unique) {
      return reader.live_docs_count();
    }
    if (const auto* terms =
          reader.GetTermRange(key.field, kConstraintKeyDict)) {
      return DistinctWith(*terms, RowsOnlyDocs(reader));
    }
  }
  return 0;
}

duckdb::unique_ptr<duckdb::BaseStatistics> ColumnStats(
  const ScanBindData& bind, const irs::DirectoryReader& reader,
  duckdb::idx_t column_index) {
  if (column_index >= bind.columns.ids.size()) {
    return nullptr;
  }
  const auto column = bind.columns.ids[column_index];
  const auto* stored = StoredColumnStats(bind, reader, column);
  uint64_t term_distinct = 0;
  const auto bounds = KeywordBounds(bind, reader, column_index, term_distinct);
  auto stats = stored ? stored->ToUnique() : nullptr;
  if (bounds) {
    if (!stats) {
      stats = duckdb::StringStats::CreateUnknown(duckdb::LogicalType::VARCHAR)
                .ToUnique();
    }
    const auto min = duckdb::StringStats::Min(*bounds);
    const auto max = duckdb::StringStats::Max(*bounds);
    duckdb::StringStats::SetMin(*stats, duckdb::string_t{min},
                                duckdb::StringStatsType::EXACT_STATS);
    duckdb::StringStats::SetMax(*stats, duckdb::string_t{max},
                                duckdb::StringStatsType::EXACT_STATS);
  }
  if (stats && stats->GetDistinctCount() == 0) {
    auto distinct = KeyDistinct(bind, reader, column);
    if (distinct == 0) {
      distinct = term_distinct;
    }
    if (distinct != 0) {
      stats->SetDistinctCount(
        std::min<uint64_t>(distinct, reader.live_docs_count()));
    }
  }
  return stats;
}

class ScanPartition final : public duckdb::PartitionRowGroup {
 public:
  ScanPartition(const ScanBindData& bind, const irs::DirectoryReader& reader)
    : _bind{bind}, _reader{reader} {}

  duckdb::unique_ptr<duckdb::BaseStatistics> GetColumnStatistics(
    const duckdb::StorageIndex& index) final {
    if (index.HasChildren()) {
      return nullptr;
    }
    return ColumnStats(_bind, _reader,
                       ScanColumnOfStorage(_bind, index.GetPrimaryIndex()));
  }

  bool MinMaxIsExact(const duckdb::StorageIndex&) final { return true; }

  bool HasPendingWrites() final { return false; }

 private:
  const ScanBindData& _bind;
  const irs::DirectoryReader& _reader;
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
  const auto& reader = *bind.Read(context).reader;
  auto& stats = result.emplace_back();
  stats.count = reader.live_docs_count();
  stats.count_type = duckdb::CountType::COUNT_EXACT;
  if (reader.docs_count() == reader.live_docs_count() &&
      reader.PatchedDocs() == 0) {
    stats.partition_row_group =
      duckdb::make_shared_ptr<ScanPartition>(bind, reader);
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
  const auto& reader = *bind.Read(context).reader;
  if (!input.column_index.HasChildren()) {
    auto stats = ColumnStats(bind, reader, column_index);
    SDB_ASSERT(!stats || stats->GetType() == bind.columns.types[column_index]);
    return stats;
  }
  if (column_index >= bind.columns.ids.size()) {
    return nullptr;
  }
  const auto* stats =
    StoredColumnStats(bind, reader, bind.columns.ids[column_index]);
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
