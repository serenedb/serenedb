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

#include "iresearch/formats/column/hot_column_reader.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/operator/comparison_operators.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/immutable_strings.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/statistics/string_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <optional>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

bool HasOwnBlocks(const duckdb::LogicalType& type) noexcept {
  switch (type.InternalType()) {
    case duckdb::PhysicalType::STRUCT:
    case duckdb::PhysicalType::LIST:
    case duckdb::PhysicalType::ARRAY:
      return false;
    default:
      return type.id() != duckdb::LogicalTypeId::VARIANT;
  }
}

template<typename T, typename Writer>
void Collect(Writer& writer, const WriteChunk& chunk) {
  const auto values = chunk.data.Values<T>();
  for (duckdb::idx_t i = 0; i < chunk.count; ++i) {
    const auto entry = values[i];
    if (entry.IsValid()) {
      writer.Update(entry.GetValue());
    } else {
      writer.SetHasNull();
    }
  }
}

template<typename T>
void CollectNumeric(duckdb::BaseStatistics& stats, const WriteChunk& chunk) {
  duckdb::StatsWriter<T> writer;
  Collect<T>(writer, chunk);
  writer.Merge(stats);
}

void SetExactBounds(duckdb::BaseStatistics& stats, const WriteChunk& chunk) {
  const auto values = chunk.data.Values<duckdb::string_t>();
  std::optional<duckdb::string_t> min;
  std::optional<duckdb::string_t> max;
  for (duckdb::idx_t i = 0; i < chunk.count; ++i) {
    const auto entry = values[i];
    if (!entry.IsValid()) {
      continue;
    }
    const auto value = entry.GetValue();
    if (!min || duckdb::LessThan::Operation(value, *min)) {
      min = value;
    }
    if (!max || duckdb::GreaterThan::Operation(value, *max)) {
      max = value;
    }
  }
  if (min) {
    duckdb::StringStats::SetMin(stats, *min,
                                duckdb::StringStatsType::EXACT_STATS);
    duckdb::StringStats::SetMax(stats, *max,
                                duckdb::StringStatsType::EXACT_STATS);
  }
}

struct IntervalStatsWriter {
  duckdb::BaseStatistics& stats;

  void Update(duckdb::interval_t value) {
    stats.SetHasNoNull();
    stats.UpdateNumericStats<duckdb::interval_t>(value);
  }
  void SetHasNull() { stats.SetHasNull(); }
};

duckdb::BaseStatistics ChunkStatistics(const duckdb::LogicalType& type,
                                       const WriteChunk& chunk) {
  auto stats = duckdb::BaseStatistics::CreateEmpty(type);
  switch (duckdb::BaseStatistics::GetStatsType(type)) {
    case duckdb::StatisticsType::NUMERIC_STATS:
      switch (type.InternalType()) {
        case duckdb::PhysicalType::BOOL:
          CollectNumeric<bool>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::INT8:
          CollectNumeric<int8_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::INT16:
          CollectNumeric<int16_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::INT32:
          CollectNumeric<int32_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::INT64:
          CollectNumeric<int64_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::UINT8:
          CollectNumeric<uint8_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::UINT16:
          CollectNumeric<uint16_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::UINT32:
          CollectNumeric<uint32_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::UINT64:
          CollectNumeric<uint64_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::INT128:
          CollectNumeric<duckdb::hugeint_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::UINT128:
          CollectNumeric<duckdb::uhugeint_t>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::FLOAT:
          CollectNumeric<float>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::DOUBLE:
          CollectNumeric<double>(stats, chunk);
          return stats;
        case duckdb::PhysicalType::INTERVAL: {
          IntervalStatsWriter writer{stats};
          Collect<duckdb::interval_t>(writer, chunk);
          return stats;
        }
        default:
          break;
      }
      break;
    case duckdb::StatisticsType::STRING_STATS:
    case duckdb::StatisticsType::GEOMETRY_STATS:
      if (type.InternalType() == duckdb::PhysicalType::VARCHAR) {
        duckdb::StatsWriter<duckdb::string_t> writer{type};
        Collect<duckdb::string_t>(writer, chunk);
        writer.Merge(stats);
        if (stats.GetStatsType() == duckdb::StatisticsType::STRING_STATS) {
          SetExactBounds(stats, chunk);
        }
        return stats;
      }
      break;
    default:
      break;
  }
  return duckdb::BaseStatistics::CreateUnknown(type);
}

}  // namespace

duckdb::BaseStatistics HotChunkStats::Get(const duckdb::LogicalType& type,
                                          const WriteChunk& chunk) {
  if (chunk.count != STANDARD_VECTOR_SIZE) {
    return ChunkStatistics(type, chunk);
  }
  const auto [it, fresh] =
    _full.try_emplace(chunk.data.GetBufferRef().get(),
                      duckdb::BaseStatistics::CreateUnknown(type));
  if (fresh) {
    it->second = ChunkStatistics(type, chunk);
  }
  return it->second.Copy();
}

void HotChunkStats::Seed(const WriteChunk& chunk,
                         const duckdb::BaseStatistics& stats) {
  SDB_ASSERT(chunk.count == STANDARD_VECTOR_SIZE);
  _full.try_emplace(chunk.data.GetBufferRef().get(), stats.Copy());
}

void HotChunkStats::MergeDistinct(DistinctStats mode, const WriteChunk& chunk,
                                  duckdb::DistinctStatistics& out) {
  if (chunk.count != STANDARD_VECTOR_SIZE) {
    UpdateDistinct(out, mode, chunk.data, chunk.count, _hashes);
    return;
  }
  auto& full = _full_distinct[chunk.data.GetBufferRef().get()];
  if (!full) {
    full = std::make_unique<duckdb::DistinctStatistics>();
    UpdateDistinct(*full, mode, chunk.data, chunk.count, _hashes);
  }
  out.Merge(*full);
}

std::unique_ptr<ColumnReader> HotColumnReader::Make(
  duckdb::DatabaseInstance& db, StagedColumn&& column, HotChunkStats* stats) {
  duckdb::shared_ptr<duckdb::DistinctStatistics> distinct;
  if (stats && column.distinct != DistinctStats::None) {
    distinct = duckdb::make_shared_ptr<duckdb::DistinctStatistics>();
    for (const auto& chunk : column.chunks) {
      stats->MergeDistinct(column.distinct, chunk, *distinct);
    }
  }
  auto reader = Build(db, std::move(column), stats);
  reader->_distinct = std::move(distinct);
  return reader;
}

std::unique_ptr<ColumnReader> HotColumnReader::Concat(
  duckdb::DatabaseInstance& db, field_id id, const duckdb::LogicalType& type,
  DistinctStats distinct, std::span<const HotColumnPart> parts) {
  uint64_t left = 0;
  for (const auto& part : parts) {
    left += part.rows;
  }
  StagedColumn column{.id = id, .type = type};
  HotChunkStats stats;
  std::optional<WriteChunk> pending;
  duckdb::idx_t capacity = 0;
  const auto flush = [&] {
    if (pending) {
      duckdb::FlatVector::SetSize(pending->data, pending->count);
      column.chunks.push_back(std::move(*pending));
      pending.reset();
    }
  };
  const auto append = [&](const duckdb::Vector* source, uint64_t rows) {
    duckdb::idx_t from = 0;
    while (rows != 0) {
      if (!pending) {
        capacity = static_cast<duckdb::idx_t>(
          std::min<uint64_t>(left, STANDARD_VECTOR_SIZE));
        pending.emplace(duckdb::Vector{type, capacity}, 0);
      }
      auto& chunk = *pending;
      const auto take = static_cast<duckdb::idx_t>(
        std::min<uint64_t>(rows, capacity - chunk.count));
      if (source) {
        duckdb::ImmutableStrings::Copy(*source, chunk.data, from + take, from,
                                       chunk.count);
      } else {
        for (duckdb::idx_t i = 0; i < take; ++i) {
          duckdb::FlatVector::SetNull(chunk.data, chunk.count + i, true);
        }
      }
      chunk.count += take;
      from += take;
      rows -= take;
      left -= take;
      if (chunk.count == capacity) {
        flush();
      }
    }
  };
  duckdb::shared_ptr<duckdb::DistinctStatistics> merged;
  duckdb::Vector hashes{duckdb::LogicalType::HASH, nullptr};
  if (distinct != DistinctStats::None) {
    merged = duckdb::make_shared_ptr<duckdb::DistinctStatistics>();
  }
  for (const auto& part : parts) {
    if (!part.column) {
      append(nullptr, part.rows);
      continue;
    }
    SDB_ENSURE(part.column->InMemory(), "column ", id,
               " of a piece merge is not held in memory");
    const auto& hot = utils::downCast<HotColumnReader>(*part.column);
    SDB_ASSERT(hot.Type() == type && hot.RowCount() <= part.rows);
    const auto blocks = hot.DataBlocks();
    for (size_t i = 0; i < hot._chunks.size(); ++i) {
      const auto& chunk = hot._chunks[i];
      if (chunk.count != STANDARD_VECTOR_SIZE) {
        append(&chunk.data, chunk.count);
        continue;
      }
      flush();
      const auto& shared = column.chunks.emplace_back(
        duckdb::Vector::Ref(chunk.data), chunk.count);
      if (!blocks.empty()) {
        stats.Seed(shared, blocks[i].statistics);
      }
      left -= chunk.count;
    }
    append(nullptr, part.rows - hot.RowCount());
    if (!merged) {
      continue;
    }
    if (const auto* stored = hot.Distinct()) {
      merged->Merge(*stored);
      continue;
    }
    for (const auto& chunk : hot._chunks) {
      UpdateDistinct(*merged, distinct, chunk.data, chunk.count, hashes);
    }
  }
  flush();
  SDB_ASSERT(left == 0);
  auto reader = Build(db, std::move(column), &stats);
  reader->_distinct = std::move(merged);
  return reader;
}

std::unique_ptr<HotColumnReader> HotColumnReader::Build(
  duckdb::DatabaseInstance& db, StagedColumn&& column, HotChunkStats* stats) {
  std::vector<std::unique_ptr<ColumnReader>> children;
  if (column.type.InternalType() == duckdb::PhysicalType::STRUCT &&
      column.type.id() != duckdb::LogicalTypeId::VARIANT) {
    const auto& child_types = duckdb::StructType::GetChildTypes(column.type);
    children.reserve(child_types.size());
    for (size_t i = 0; i < child_types.size(); ++i) {
      StagedColumn child{.id = column.id, .type = child_types[i].second};
      child.chunks.reserve(column.chunks.size());
      for (const auto& chunk : column.chunks) {
        const auto& entries = duckdb::StructVector::GetEntries(chunk.data);
        child.chunks.push_back(
          WriteChunk{duckdb::Vector::Ref(entries[i]), chunk.count});
      }
      children.push_back(Build(db, std::move(child), stats));
    }
  } else if (column.type.id() == duckdb::LogicalTypeId::ARRAY) {
    const auto size = duckdb::ArrayType::GetSize(column.type);
    StagedColumn child{.id = column.id,
                       .type = duckdb::ArrayType::GetChildType(column.type)};
    child.chunks.reserve(column.chunks.size());
    for (const auto& chunk : column.chunks) {
      child.chunks.push_back(WriteChunk{
        duckdb::Vector::Ref(duckdb::ArrayVector::GetChild(chunk.data)),
        chunk.count * size});
    }
    children.push_back(Build(db, std::move(child), stats));
  }
  std::vector<ColumnBlockMeta> blocks;
  if (HasOwnBlocks(column.type)) {
    const auto codec =
      duckdb::DBConfig::GetConfig(db).TryGetCompressionFunction(
        duckdb::CompressionType::COMPRESSION_UNCOMPRESSED,
        column.type.InternalType());
    SDB_ASSERT(codec);
    blocks.reserve(column.chunks.size());
    for (const auto& chunk : column.chunks) {
      blocks.push_back(ColumnBlockMeta{
        .statistics = stats
                        ? stats->Get(column.type, chunk)
                        : duckdb::BaseStatistics::CreateUnknown(column.type),
        .tuple_count = chunk.count,
        .codec = codec.get(),
      });
    }
  }
  return std::make_unique<HotColumnReader>(
    column.id, std::move(column.type), std::move(blocks),
    std::move(column.chunks), std::move(children));
}

HotColumnReader::HotColumnReader(
  field_id id, duckdb::LogicalType type, std::vector<ColumnBlockMeta> blocks,
  std::vector<WriteChunk> chunks,
  std::vector<std::unique_ptr<ColumnReader>> children)
  : ColumnReader{id, std::move(type), std::move(blocks), nullptr,
                 std::move(children)},
    _chunks{std::move(chunks)} {
  _starts.reserve(_chunks.size() + 1);
  uint64_t rows = 0;
  _starts.push_back(rows);
  for (const auto& chunk : _chunks) {
    rows += chunk.count;
    _starts.push_back(rows);
  }
  _row_count = rows;
  if (DataBlocks().empty()) {
    _stats = duckdb::BaseStatistics::CreateUnknown(_type).ToUnique();
  }
}

size_t HotColumnReader::Bytes() const noexcept {
  size_t bytes = 0;
  for (const auto& chunk : _chunks) {
    bytes += chunk.data.GetAllocationSize();
  }
  return bytes;
}

size_t HotColumnReader::ChunkOf(uint64_t row) const noexcept {
  return static_cast<size_t>(absl::c_upper_bound(_starts, row) -
                             _starts.begin()) -
         1;
}

void HotColumnReader::CopyRows(uint64_t row, duckdb::idx_t count,
                               duckdb::Vector& out, duckdb::idx_t out_offset,
                               bool share_strings) const {
  while (count != 0) {
    if (row >= _row_count) {
      for (duckdb::idx_t i = 0; i < count; ++i) {
        duckdb::FlatVector::SetNull(out, out_offset + i, true);
      }
      return;
    }
    const auto chunk = ChunkOf(row);
    const auto in_chunk = static_cast<duckdb::idx_t>(row - _starts[chunk]);
    const auto take =
      std::min<duckdb::idx_t>(count, _chunks[chunk].count - in_chunk);
    const auto& source = _chunks[chunk].data;
    if (share_strings &&
        source.GetType().InternalType() == duckdb::PhysicalType::VARCHAR &&
        source.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
      std::copy_n(
        duckdb::FlatVector::GetData<duckdb::string_t>(source) + in_chunk, take,
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(out) + out_offset);
      const auto& valid = duckdb::FlatVector::Validity(source);
      auto& target = duckdb::FlatVector::ValidityMutable(out);
      if (!valid.AllValid() || !target.AllValid()) {
        for (duckdb::idx_t i = 0; i < take; ++i) {
          target.Set(out_offset + i, valid.RowIsValid(in_chunk + i));
        }
      }
      out.AddHeapReference(source);
    } else {
      duckdb::VectorOperations::Copy(source, out, in_chunk + take, in_chunk,
                                     out_offset);
    }
    row += take;
    count -= take;
    out_offset += take;
  }
}

bool HotColumnReader::FetchPoint(PointReader&, uint64_t row,
                                 duckdb::Vector& out,
                                 duckdb::idx_t out_offset) const {
  if (row >= _row_count) {
    duckdb::FlatVector::SetNull(out, out_offset, true);
    return false;
  }
  CopyRows(row, 1, out, out_offset, true);
  return duckdb::FlatVector::Validity(out).RowIsValid(out_offset);
}

bool HotColumnReader::ValidPoint(PointReader&, uint64_t row,
                                 duckdb::Vector&) const {
  if (row >= _row_count) {
    return false;
  }
  const auto chunk = ChunkOf(row);
  duckdb::UnifiedVectorFormat format;
  _chunks[chunk].data.ToUnifiedFormat(format);
  const auto in_chunk = static_cast<duckdb::idx_t>(row - _starts[chunk]);
  return format.validity.RowIsValid(format.sel->get_index(in_chunk));
}

uint64_t HotColumnReader::RowGroupEnd(uint64_t row) const noexcept {
  if (row >= _row_count) {
    return _row_count;
  }
  return *absl::c_upper_bound(_starts, row);
}

duckdb::idx_t HotColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                    duckdb::idx_t count) const {
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  return ScanCount(s, result, count, 0);
}

duckdb::idx_t HotColumnReader::ScanCount(ScanState& s, duckdb::Vector& result,
                                         duckdb::idx_t count,
                                         duckdb::idx_t result_offset) const {
  CopyRows(s.st.offset_in_column, count, result, result_offset, true);
  s.st.offset_in_column += count;
  return count;
}

duckdb::idx_t HotColumnReader::GatherFilter(
  ScanState& s, uint64_t anchor, duckdb::idx_t span,
  duckdb::SelectionVector& sel, duckdb::idx_t sel_count,
  const duckdb::TableFilter&, duckdb::TableFilterState& filter_state,
  NullCheckKind, duckdb::Vector& scratch) const {
  SDB_ASSERT(anchor >= s.st.offset_in_column,
             "GatherFilter requires ascending rows");
  SDB_ASSERT(span <= STANDARD_VECTOR_SIZE, "GatherFilter decodes one vector");
  s.st.offset_in_column = anchor + span;
  duckdb::idx_t approved = sel_count;
  if (anchor < _row_count) {
    const auto chunk = ChunkOf(anchor);
    const auto in_chunk = static_cast<duckdb::idx_t>(anchor - _starts[chunk]);
    const auto& source = _chunks[chunk].data;
    if (in_chunk + span <= _chunks[chunk].count &&
        source.GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
        source.GetType() == scratch.GetType()) {
      scratch.Reference(duckdb::Vector{source, in_chunk, in_chunk + span});
      duckdb::ColumnSegment::FilterSelection(sel, scratch, filter_state, span,
                                             approved);
      return approved;
    }
  }
  scratch.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::FlatVector::ValidityMutable(scratch).SetAllValid(span);
  CopyRows(anchor, span, scratch, 0, false);
  duckdb::ColumnSegment::FilterSelection(sel, scratch, filter_state, span,
                                         approved);
  return approved;
}

}  // namespace irs
