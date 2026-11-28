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
#include <duckdb/function/cast/cast_statistics.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
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
  const ScanBindData& bind, const irs::CompositeReader& reader,
  ColumnId col_id) {
  return StoresColumnStats(bind, col_id) ? reader.GetColumnStats(col_id)
                                         : nullptr;
}

duckdb::string_t AsStringT(irs::bytes_view bytes) {
  return {reinterpret_cast<const char*>(bytes.data()),
          static_cast<uint32_t>(bytes.size())};
}

uint64_t RowsOnlyDocs(const irs::CompositeReader& reader) {
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

duckdb::unique_ptr<duckdb::BaseStatistics> KeywordBounds(
  const ScanBindData& bind, const irs::CompositeReader& reader,
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
    duckdb::StatsWriter<duckdb::string_t> writer{duckdb::LogicalType::VARCHAR};
    writer.Update(AsStringT(terms->min));
    writer.Update(AsStringT(terms->max));
    writer.Merge(*bounds);
    distinct = DistinctWith(*terms, RowsOnlyDocs(reader));
  }
  for (const auto& segment : reader) {
    if (!RowsOnlySegment(segment)) {
      continue;
    }
    const auto* columns = segment.GetColReader();
    const auto* values = columns ? columns->Column(column) : nullptr;
    if (!values) {
      return nullptr;
    }
    bounds->Merge(values->MergedStatistics());
  }
  return duckdb::StringStats::HasMinMax(*bounds) ? std::move(bounds) : nullptr;
}

uint64_t KeyDistinct(const ScanBindData& bind,
                     const irs::CompositeReader& reader, ColumnId column) {
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
  const ScanBindData& bind, const irs::CompositeReader& reader,
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
                                duckdb::StringStats::GetMinType(*bounds));
    duckdb::StringStats::SetMax(*stats, duckdb::StringStats::Max(*bounds),
                                duckdb::StringStats::GetMaxType(*bounds));
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
  ScanPartition(const ScanBindData& bind, const irs::CompositeReader& reader)
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
  const irs::CompositeReader& _reader;
};

uint64_t DeletedRows(const irs::SubReader& segment, uint64_t begin,
                     uint64_t end) {
  const auto* mask = segment.docs_mask();
  return mask ? mask->CountInRange(
                  static_cast<irs::doc_id_t>(irs::doc_limits::min() + begin),
                  static_cast<irs::doc_id_t>(irs::doc_limits::min() + end))
              : 0;
}

size_t RowGroupCount(const ScanBindData& bind, const irs::IndexReader& reader) {
  const auto rg_size = bind.relation.RowGroupSize();
  size_t row_groups = 0;
  for (size_t seg = 0; seg < reader.size(); ++seg) {
    row_groups += SegmentRowGroups{reader[seg], rg_size}.Count();
  }
  return row_groups;
}

template<typename Visit>
void ForEachPartition(const ScanBindData& bind, const irs::IndexReader& reader,
                      Visit&& visit) {
  const auto rg_size = bind.relation.RowGroupSize();
  for (uint32_t seg = 0; seg < reader.size(); ++seg) {
    const auto& segment = reader[seg];
    const SegmentRowGroups groups{segment, rg_size};
    for (uint32_t rg = 0; rg < groups.Count(); ++rg) {
      const auto begin = groups.Begin(rg);
      const auto end = groups.End(rg);
      const auto deleted = DeletedRows(segment, begin, end);
      if (deleted != end - begin) {
        visit(ScanRowGroup{seg, rg}, begin, end, end - begin - deleted);
      }
    }
  }
}

const irs::ColumnReader* StatsColumn(const ScanBindData& bind,
                                     const irs::ColReader* columns,
                                     duckdb::idx_t column_index, uint64_t end) {
  if (!columns || column_index >= bind.columns.ids.size()) {
    return nullptr;
  }
  const auto col_id = bind.columns.ids[column_index];
  const auto* column =
    StoresColumnStats(bind, col_id) ? columns->Column(col_id) : nullptr;
  if (!column || column->DataBlocks().size() == 0 || column->RowCount() < end ||
      column->Type().IsNested() ||
      column->Type() != bind.columns.types[column_index]) {
    return nullptr;
  }
  return column;
}

struct CoveringBlocks {
  CoveringBlocks() = default;
  CoveringBlocks(const irs::ColumnReader* column_p, uint64_t begin,
                 uint64_t end)
    : column{column_p} {
    if (column) {
      first = column->Locate(begin);
      last = column->Locate(end - 1, first);
    }
  }

  const duckdb::BaseStatistics& FirstStatistics() const noexcept {
    return column->DataBlocks()[first.block].statistics;
  }

  bool OneBlock() const noexcept { return first.block == last.block; }

  bool HoldOnly(uint64_t begin, uint64_t end) const noexcept {
    return first.begin == begin && last.end == end;
  }

  void MergeOthers(duckdb::BaseStatistics& stats) const {
    for (auto block = first.block + 1; block <= last.block; ++block) {
      stats.Merge(column->DataBlocks()[block].statistics);
    }
  }

  const irs::ColumnReader* column = nullptr;
  irs::BlockWindow first;
  irs::BlockWindow last;
};

class RowGroupPartition final : public duckdb::PartitionRowGroup {
 public:
  RowGroupPartition(const ScanBindData& bind, const irs::SubReader& segment,
                    uint64_t begin, uint64_t end, uint64_t live)
    : _bind{bind}, _segment{segment}, _begin{begin}, _end{end}, _live{live} {}

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
    if (blocks.OneBlock()) {
      return blocks.FirstStatistics();
    }
    auto merged = blocks.FirstStatistics().ToUnique();
    blocks.MergeOthers(*merged);
    return duckdb::unique_ptr<const duckdb::BaseStatistics>(std::move(merged));
  }

  bool MinMaxIsExact(const duckdb::StorageIndex& index) final {
    if (_live != _end - _begin || index.HasChildren()) {
      return false;
    }
    const auto& blocks = Blocks(index.GetPrimaryIndex());
    return blocks.column && !blocks.column->Patched() &&
           (blocks.HoldOnly(_begin, _end) ||
            DeletedRows(_segment, blocks.first.begin, blocks.last.end) == 0);
  }

  bool HasPendingWrites() final { return false; }

 private:
  const CoveringBlocks& Blocks(duckdb::idx_t storage_index) {
    if (_resolved != storage_index) {
      _blocks = CoveringBlocks{
        StatsColumn(_bind, _segment.GetColReader(),
                    ScanColumnOfStorage(_bind, storage_index), _end),
        _begin, _end};
      _resolved = storage_index;
    }
    return _blocks;
  }

  const ScanBindData& _bind;
  const irs::SubReader& _segment;
  uint64_t _begin;
  uint64_t _end;
  uint64_t _live;
  duckdb::idx_t _resolved = duckdb::DConstants::INVALID_INDEX;
  CoveringBlocks _blocks;
};

struct RowGroupPartitions {
  std::vector<RowGroupPartition> parts;
};

bool Listed(const std::vector<duckdb::idx_t>& list, size_t& at,
            duckdb::idx_t partition) {
  while (at != list.size() && list[at] < partition) {
    ++at;
  }
  return at != list.size() && list[at] == partition;
}

duckdb::shared_ptr<RowGroupPartitions> BuildRowGroupPartitions(
  const ScanBindData& bind, const irs::IndexReader& reader,
  duckdb::vector<duckdb::PartitionStatistics>& stats) {
  auto holder = duckdb::make_shared_ptr<RowGroupPartitions>();
  auto& parts = holder->parts;
  parts.reserve(RowGroupCount(bind, reader));
  ForEachPartition(
    bind, reader,
    [&](ScanRowGroup position, uint64_t begin, uint64_t end, uint64_t live) {
      parts.emplace_back(bind, reader[position.seg], begin, end, live);
    });
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

void SelectScannedRowGroups(const ScanBindData& bind,
                            const irs::IndexReader& reader, bool order_prunes,
                            bool limit_prunes,
                            absl::FunctionRef<void(ScanRowGroup)> scan) {
  const auto* chosen =
    bind.partitions_to_scan ? &*bind.partitions_to_scan : nullptr;
  size_t chosen_at = 0;
  const auto is_chosen = [&](duckdb::idx_t partition) {
    return !chosen || Listed(*chosen, chosen_at, partition);
  };
  if (!order_prunes) {
    duckdb::idx_t partition = 0;
    ForEachPartition(bind, reader,
                     [&](ScanRowGroup position, uint64_t, uint64_t, uint64_t) {
                       if (is_chosen(partition++)) {
                         scan(position);
                       }
                     });
    return;
  }
  SDB_ASSERT(bind.scan_order);
  const auto& order = *bind.scan_order;
  const duckdb::RowGroupOrderOptions options{
    order.storage,
    order.order_by,
    order.order_type,
    order.null_order,
    order.column_type,
    limit_prunes ? order.row_limit : duckdb::optional_idx{},
    order.skipped_groups,
    order.skipped_null_groups,
  };
  const auto column_index =
    order.storage.HasChildren()
      ? duckdb::DConstants::INVALID_INDEX
      : ScanColumnOfStorage(bind, order.storage.GetPrimaryIndex());
  struct Partition {
    ScanRowGroup position;
    uint64_t rows;
  };
  const auto row_groups = RowGroupCount(bind, reader);
  std::vector<Partition> partitions;
  std::vector<duckdb::BaseStatistics> merged;
  duckdb::vector<duckdb::RowGroupReorderer::PartitionOrderStatistics>
    statistics;
  partitions.reserve(row_groups);
  merged.reserve(row_groups);
  statistics.reserve(row_groups);
  ForEachPartition(
    bind, reader,
    [&](ScanRowGroup position, uint64_t begin, uint64_t end, uint64_t live) {
      partitions.push_back({position, live});
      const CoveringBlocks blocks{
        StatsColumn(bind, reader[position.seg].GetColReader(), column_index,
                    end),
        begin, end};
      const bool holds_min_max = blocks.column && live == end - begin &&
                                 !blocks.column->Patched() &&
                                 blocks.HoldOnly(begin, end);
      if (!blocks.column) {
        statistics.emplace_back();
      } else if (blocks.OneBlock()) {
        statistics.emplace_back(&blocks.FirstStatistics(), holds_min_max);
      } else {
        auto& stats = merged.emplace_back(blocks.FirstStatistics().Copy());
        blocks.MergeOthers(stats);
        statistics.emplace_back(&stats, holds_min_max);
      }
    });
  auto kept = duckdb::RowGroupReorderer::OrderPartitions(
    options, statistics,
    [&](duckdb::idx_t partition) { return partitions[partition].rows; });
  absl::c_sort(kept);
  for (const auto partition : kept) {
    if (is_chosen(partition)) {
      scan(partitions[partition].position);
    }
  }
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
  if (leaf->GetType().IsNumeric() && want.IsNumeric()) {
    return duckdb::CastStatistics::TryPropagate(*leaf, leaf->GetType(), want);
  }
  return nullptr;
}

}  // namespace sdb::connector
