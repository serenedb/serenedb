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

#include "iresearch/formats/column/col_copy_check.hpp"

#include <absl/algorithm/container.h>
#include <absl/numeric/int128.h>

#include <algorithm>
#include <memory>
#include <span>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

bool IsNested(const duckdb::LogicalType& type) noexcept {
  switch (type.InternalType()) {
    case duckdb::PhysicalType::STRUCT:
    case duckdb::PhysicalType::LIST:
    case duckdb::PhysicalType::ARRAY:
      return true;
    default:
      return false;
  }
}

uint64_t BytesIn(const ColumnReader& column, uint64_t begin, uint64_t end) {
  absl::uint128 bytes = 0;
  const auto blocks = column.DataBlocks();
  for (size_t block = 0; block != blocks.size(); ++block) {
    const auto first = column.DataBlockFirstRow(block);
    const auto lo = std::max(first, begin);
    const auto hi = std::min(first + blocks[block].tuple_count, end);
    if (lo < hi) {
      bytes += absl::uint128{blocks[block].byte_size} * (hi - lo) /
               blocks[block].tuple_count;
    }
  }
  auto total = static_cast<uint64_t>(bytes);
  if (const auto* validity = column.Validity()) {
    total += BytesIn(*validity, begin, end);
  }
  return total;
}

uint64_t BlockBytes(std::span<const ColumnBlockMeta> blocks) noexcept {
  uint64_t bytes = 0;
  for (const auto& block : blocks) {
    bytes += block.byte_size;
  }
  return bytes;
}

}  // namespace

uint64_t StoredBytes(const ColumnReader& column) {
  uint64_t bytes = BlockBytes(column.DataBlocks());
  if (const auto* validity = column.Validity()) {
    bytes += StoredBytes(*validity);
  }
  if (const auto* child = column.Child()) {
    bytes += StoredBytes(*child);
  }
  for (size_t i = 0; i != column.StructFieldCount(); ++i) {
    bytes += StoredBytes(column.StructField(i));
  }
  return bytes;
}

std::optional<ReencodeEstimate> ReencodeCache::Find(
  field_id column, const ReencodeKey& key) const {
  std::lock_guard lock{_mutex};
  const auto it = _entries.find(column);
  if (it == _entries.end() || it->second.first != key) {
    return std::nullopt;
  }
  return it->second.second;
}

void ReencodeCache::Store(field_id column, const ReencodeKey& key,
                          const ReencodeEstimate& estimate) {
  std::lock_guard lock{_mutex};
  _entries.insert_or_assign(column, std::pair{key, estimate});
}

std::optional<ReencodeEstimate> EstimateReencode(
  const ColReader& reader, const ColumnReader& column,
  const IndexFieldOptions& options, size_t samples) {
  if (samples == 0 || IsNested(column.Type())) {
    return std::nullopt;
  }
  SDB_ASSERT(column.RowCount() != 0);
  const auto column_options = options.GetColumnOptions(column.Id());
  auto codec_params = options.CodecParams(column_options);
  codec_params.tier = WriteTier::Merge;
  const ReencodeKey key{
    .skip_validity = column_options.skip_validity,
    .compression = column_options.compression,
    .codec_params = codec_params,
    .row_group_size = options.row_group_size,
    .samples = samples,
  };
  auto& cache = reader.Reencodes();
  if (auto cached = cache.Find(column.Id(), key)) {
    return cached;
  }
  const uint64_t rows = column.RowCount();
  const uint64_t group_rows = std::max<uint32_t>(1, options.row_group_size);
  const uint64_t groups = (rows + group_rows - 1) / group_rows;
  const uint64_t picks = std::min<uint64_t>(samples, groups);

  MemoryDirectory dir;
  ColWriter trial{dir, "copy_check", reader.Database(), WriteTier::Merge};
  auto& writer = trial.OpenColumn(
    column.Id(), column.Type(), column_options.skip_validity,
    static_cast<uint32_t>(group_rows), column_options.compression,
    /*hyperloglog=*/false, codec_params);
  auto state = column.InitScan(std::make_shared<ReadContext>(reader));
  ColumnReader::VectorScratch scratch{column.Type()};
  ReencodeEstimate estimate;
  uint64_t position = 0;
  for (uint64_t pick = 0; pick != picks; ++pick) {
    const uint64_t begin = (2 * pick + 1) * groups / (2 * picks) * group_rows;
    const uint64_t end = std::min(rows, begin + group_rows);
    column.Skip(state, begin - position);
    for (auto row = begin; row < end;) {
      const auto take = std::min<uint64_t>(end - row, STANDARD_VECTOR_SIZE);
      auto& batch = scratch.Reset();
      column.Scan(state, batch, take);
      writer.Append(batch, take);
      row += take;
    }
    position = end;
    estimate.stored_bytes += BytesIn(column, begin, end);
  }
  writer.Finish();
  const auto& written = writer.Meta();
  estimate.reencoded_bytes =
    BlockBytes(written.data) + BlockBytes(written.validity);
  estimate.codec_changed =
    column_options.compression != duckdb::CompressionType::COMPRESSION_AUTO &&
    absl::c_none_of(column.DataBlocks(), [&](const ColumnBlockMeta& stored) {
      return absl::c_any_of(written.data, [&](const ColumnBlockMeta& trial) {
        return trial.codec->type == stored.codec->type;
      });
    });
  cache.Store(column.Id(), key, estimate);
  return estimate;
}

}  // namespace irs
