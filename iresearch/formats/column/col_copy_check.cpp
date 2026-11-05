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

ReencodeKey KeyOf(const ColumnReader& column,
                  const IndexFieldOptions& options) {
  const auto column_options = options.GetColumnOptions(column.Id());
  return {
    .skip_validity = column_options.skip_validity,
    .compression = column_options.compression,
    .codec_params = options.CodecParams(column_options),
    .row_group_size = options.row_group_size,
  };
}

ReencodeEstimate TrialReencode(
  const ColReader& reader, const ColumnReader& column, const ReencodeKey& key,
  duckdb::ObjectCache* cache,
  std::shared_ptr<const codecs::TrainedDictionary> dictionary) {
  SDB_ASSERT(!column.Type().IsNested() && column.RowCount() != 0);
  const uint64_t rows = column.RowCount();
  const uint64_t group_rows = std::max<uint32_t>(1, key.row_group_size);
  const uint64_t groups = (rows + group_rows - 1) / group_rows;
  const uint64_t picks = std::min<uint64_t>(kReencodeSampleRowGroups, groups);

  MemoryDirectory dir;
  ColWriter trial{dir, "copy_check", reader.Database(), WriteTier::Merge};
  auto& writer =
    trial.OpenColumn(column.Id(), column.Type(), key.skip_validity,
                     static_cast<uint32_t>(group_rows), key.compression,
                     /*hyperloglog=*/false, key.codec_params);
  if (dictionary) {
    writer.ShareLeafDictionary({}, std::move(dictionary), 1);
  }
  auto state = column.InitScan(MergeReadContext(reader, cache));
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
  const auto encoded = [](const ColumnBlockMeta& block) {
    return block.codec->type != duckdb::CompressionType::COMPRESSION_CONSTANT &&
           block.codec->type != duckdb::CompressionType::COMPRESSION_EMPTY;
  };
  const auto stored = column.DataBlocks();
  estimate.codec_changed =
    key.compression != duckdb::CompressionType::COMPRESSION_AUTO &&
    absl::c_any_of(stored, encoded) &&
    absl::c_none_of(stored, [&](const ColumnBlockMeta& block) {
      return encoded(block) &&
             absl::c_any_of(written.data, [&](const ColumnBlockMeta& trial) {
               return trial.codec->type == block.codec->type;
             });
    });
  return estimate;
}

}  // namespace

bool ReencodePays(const ReencodeEstimate& estimate) noexcept {
  return estimate.codec_changed ||
         absl::uint128{estimate.reencoded_bytes} * 100 <
           absl::uint128{estimate.stored_bytes} *
             (100 - kReencodeMinSavingsPercent);
}

bool ReencodeCache::Find(field_id column, const ReencodeKey& key,
                         ReencodeEstimate& estimate) const {
  absl::MutexLock lock{&_mutex};
  const auto it = _entries.find(column);
  if (it == _entries.end() || it->second.key != key) {
    return false;
  }
  estimate = it->second.estimate;
  return true;
}

void ReencodeCache::Store(field_id column, const ReencodeKey& key,
                          const ReencodeEstimate& estimate) {
  absl::MutexLock lock{&_mutex};
  _entries.insert_or_assign(column, Entry{.key = key, .estimate = estimate});
}

ReencodeEstimate EstimateReencode(
  const ColReader& reader, const ColumnReader& column,
  const IndexFieldOptions& options, duckdb::ObjectCache* cache,
  std::shared_ptr<const codecs::TrainedDictionary> dictionary) {
  const auto key = KeyOf(column, options);
  if (dictionary) {
    return TrialReencode(reader, column, key, cache, std::move(dictionary));
  }
  return reader.Reencodes().Estimate(column.Id(), key, [&] {
    return TrialReencode(reader, column, key, cache, nullptr);
  });
}

}  // namespace irs
