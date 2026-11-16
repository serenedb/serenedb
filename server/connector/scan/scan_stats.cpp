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
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/document_mask.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/constraint_keys.h"
#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"
#include "search/key_spec.h"

namespace sdb::connector {
namespace {

bool StoresColumnStats(const ScanBindData& bind, ColumnId col_id) {
  if (bind.relation.IsInvertedIndex()) {
    const auto* info = bind.relation.ScannedIndex().FindColumnInfo(col_id);
    return info && info->store_values;
  }
  return col_id <= kMaxRealColumnIdValue;
}

const duckdb::BaseStatistics* StoredColumnStats(
  const ScanBindData& bind, const irs::DirectoryReader& reader,
  ColumnId col_id) {
  return StoresColumnStats(bind, col_id) ? reader.GetColumnStats(col_id)
                                         : nullptr;
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
    duckdb::StringStats::SetMin(*stats, duckdb::StringStats::Min(*bounds),
                                duckdb::StringStatsType::EXACT_STATS);
    duckdb::StringStats::SetMax(*stats, duckdb::StringStats::Max(*bounds),
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

  duckdb::optionally_owned_ptr<const duckdb::BaseStatistics>
  GetColumnStatistics(const duckdb::StorageIndex& index) final {
    if (index.HasChildren()) {
      return {};
    }
    return duckdb::unique_ptr<const duckdb::BaseStatistics>(ColumnStats(
      _bind, _reader, ScanColumnOfStorage(_bind, index.GetPrimaryIndex())));
  }

  bool MinMaxIsExact(const duckdb::StorageIndex&) final { return true; }

  bool HasPendingWrites() final { return false; }

 private:
  const ScanBindData& _bind;
  const irs::DirectoryReader& _reader;
};

uint64_t DeletedRows(const irs::SubReader& segment, uint64_t begin,
                     uint64_t end) {
  const auto* mask = segment.docs_mask();
  return mask ? mask->CountInRange(
                  static_cast<irs::doc_id_t>(irs::doc_limits::min() + begin),
                  static_cast<irs::doc_id_t>(irs::doc_limits::min() + end))
              : 0;
}

class RowGroupPartition final : public duckdb::PartitionRowGroup {
 public:
  RowGroupPartition(const ScanBindData& bind, const irs::SubReader& segment,
                    const irs::ColReader* columns, ScanRowGroup position,
                    uint64_t begin, uint64_t end, uint64_t live)
    : _bind{bind},
      _segment{segment},
      _columns{columns},
      _position{position},
      _begin{begin},
      _end{end},
      _live{live} {}

  ScanRowGroup Position() const noexcept { return _position; }
  uint64_t Live() const noexcept { return _live; }

  duckdb::optionally_owned_ptr<const duckdb::BaseStatistics>
  GetColumnStatistics(const duckdb::StorageIndex& index) final {
    if (index.HasChildren()) {
      return {};
    }
    const auto& blocks = Blocks(index.GetPrimaryIndex());
    if (!blocks.column) {
      return {};
    }
    if (blocks.first.block == blocks.last.block) {
      return blocks.column->RowGroupStatistics(blocks.first.block);
    }
    auto merged =
      blocks.column->RowGroupStatistics(blocks.first.block).ToUnique();
    for (auto block = blocks.first.block + 1; block <= blocks.last.block;
         ++block) {
      merged->Merge(blocks.column->RowGroupStatistics(block));
    }
    return duckdb::unique_ptr<const duckdb::BaseStatistics>(std::move(merged));
  }

  bool MinMaxIsExact(const duckdb::StorageIndex& index) final {
    if (_live != _end - _begin || index.HasChildren()) {
      return false;
    }
    const auto& blocks = Blocks(index.GetPrimaryIndex());
    return blocks.column && !blocks.column->Patched() &&
           DeletedRows(_segment, blocks.first.begin, blocks.last.end) == 0;
  }

  bool HasPendingWrites() final { return false; }

 private:
  struct CoveringBlocks {
    const irs::ColumnReader* column = nullptr;
    irs::BlockWindow first;
    irs::BlockWindow last;
  };

  const CoveringBlocks& Blocks(duckdb::idx_t storage_index) {
    if (_resolved != storage_index) {
      _blocks = {.column = Column(ScanColumnOfStorage(_bind, storage_index))};
      if (_blocks.column) {
        _blocks.first = _blocks.column->Locate(_begin);
        _blocks.last = _blocks.column->Locate(_end - 1, _blocks.first);
      }
      _resolved = storage_index;
    }
    return _blocks;
  }

  const irs::ColumnReader* Column(duckdb::idx_t column_index) const {
    if (column_index >= _bind.columns.ids.size()) {
      return nullptr;
    }
    const auto col_id = _bind.columns.ids[column_index];
    const auto* column = StoresColumnStats(_bind, col_id) && _columns
                           ? _columns->Column(col_id)
                           : nullptr;
    if (!column || column->DataRgCount() == 0 || column->RowCount() < _end ||
        column->Type().IsNested() ||
        column->Type() != _bind.columns.types[column_index]) {
      return nullptr;
    }
    return column;
  }

  const ScanBindData& _bind;
  const irs::SubReader& _segment;
  const irs::ColReader* _columns;
  ScanRowGroup _position;
  uint64_t _begin;
  uint64_t _end;
  uint64_t _live;
  duckdb::idx_t _resolved = duckdb::DConstants::INVALID_INDEX;
  CoveringBlocks _blocks;
};

struct RowGroupPartitions {
  std::vector<RowGroupPartition> parts;
};

duckdb::shared_ptr<RowGroupPartitions> BuildRowGroupPartitions(
  const ScanBindData& bind, const irs::IndexReader& reader,
  duckdb::vector<duckdb::PartitionStatistics>& stats) {
  auto holder = duckdb::make_shared_ptr<RowGroupPartitions>();
  auto& parts = holder->parts;
  const auto rg_size = bind.relation.RowGroupSize();
  size_t row_groups = 0;
  for (size_t seg = 0; seg < reader.size(); ++seg) {
    row_groups += SegmentRowGroups{reader[seg], rg_size}.Count();
  }
  parts.reserve(row_groups);
  for (uint32_t seg = 0; seg < reader.size(); ++seg) {
    const auto& segment = reader[seg];
    const SegmentRowGroups groups{segment, rg_size};
    const auto* columns = segment.GetColReader();
    for (uint32_t rg = 0; rg < groups.Count(); ++rg) {
      const auto begin = groups.Begin(rg);
      const auto end = groups.End(rg);
      const auto deleted = DeletedRows(segment, begin, end);
      if (deleted != end - begin) {
        parts.emplace_back(bind, segment, columns, ScanRowGroup{seg, rg}, begin,
                           end, end - begin - deleted);
      }
    }
  }
  stats.reserve(parts.size());
  for (auto& part : parts) {
    auto& partition = stats.emplace_back();
    partition.count = part.Live();
    partition.count_type = duckdb::CountType::COUNT_EXACT;
    partition.partition_row_group =
      duckdb::shared_ptr<duckdb::PartitionRowGroup>(holder, &part);
  }
  return holder;
}

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
  if (input.row_groups) {
    BuildRowGroupPartitions(bind, reader, result);
    return result;
  }
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

std::vector<ScanRowGroup> OffsetSkippedRowGroups(
  const ScanBindData& bind, const irs::IndexReader& reader) {
  SDB_ASSERT(bind.scan_order);
  const auto& order = *bind.scan_order;
  duckdb::vector<duckdb::PartitionStatistics> stats;
  const auto holder = BuildRowGroupPartitions(bind, reader, stats);
  const duckdb::RowGroupOrderOptions options{
    order.storage,        order.order_by,           order.order_type,
    order.null_order,     order.column_type,        {},
    order.skipped_groups, order.skipped_null_groups};
  std::vector<ScanRowGroup> skipped;
  for (const auto i :
       duckdb::RowGroupReorderer::GetOffsetPrunedPartitions(options, stats)) {
    skipped.push_back(holder->parts[i].Position());
  }
  absl::c_sort(skipped);
  return skipped;
}

duckdb::unique_ptr<duckdb::BaseStatistics> IResearchScanStatistics(
  duckdb::ClientContext& context,
  duckdb::TableFunctionGetStatisticsInput& input) {
  if (!input.bind_data) {
    return nullptr;
  }
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  if (!input.column_index.HasPrimaryIndex() || bind.IsPointRead()) {
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
      casted.CopyValidity(*leaf);
      duckdb::NumericStats::SetMin(casted, *cmin);
      duckdb::NumericStats::SetMax(casted, *cmax);
      return casted.ToUnique();
    }
  }
  return nullptr;
}

}  // namespace sdb::connector
