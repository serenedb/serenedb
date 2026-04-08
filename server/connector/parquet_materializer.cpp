////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "parquet_materializer.hpp"

#include <velox/common/base/BitUtil.h>
#include <velox/dwio/common/Mutation.h>
#include <velox/dwio/parquet/reader/ParquetReader.h>

#include <cstring>
#include <ranges>

#include "basics/down_cast.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

ParquetMaterializer::ParquetMaterializer(
  velox::memory::MemoryPool& pool, std::shared_ptr<velox::ReadFile> source,
  std::unique_ptr<velox::dwio::common::Reader> reader,
  std::unique_ptr<velox::dwio::common::RowReader> row_reader,
  velox::RowTypePtr output_type, std::vector<catalog::Column::Id> column_ids)
  : _pool{pool},
    _source{std::move(source)},
    _reader{std::move(reader)},
    _row_reader{std::move(row_reader)},
    _output_type{std::move(output_type)} {
  for (size_t i = 0; i < column_ids.size(); ++i) {
    if (column_ids[i] == catalog::Column::kInvertedIndexScoreId) {
      _score_column_idx = i;
    } else if (column_ids[i] == catalog::Column::kInvertedIndexOffsetsId) {
      _offsets_column_indices.push_back(i);
    }
  }
  auto& parquet_reader =
    basics::downCast<velox::parquet::ParquetReader>(*_reader);
  auto metadata = parquet_reader.fileMetaData();
  auto num_groups = metadata.numRowGroups();
  _row_group_starts.reserve(num_groups);
  int64_t offset = 0;
  int64_t max_rg_rows = 0;
  for (int i = 0; i < num_groups; ++i) {
    _row_group_starts.push_back(offset);
    auto rg_rows = metadata.rowGroup(i).numRows();
    max_rg_rows = std::max(max_rg_rows, rg_rows);
    offset += rg_rows;
  }
  _total_rows = offset;
  _bitmap_buf.assign(velox::bits::nwords(max_rg_rows), ~uint64_t{0});
}

uint32_t ParquetMaterializer::FindRowGroup(int64_t row_number,
                                           uint32_t search_from) const {
  auto begin = _row_group_starts.begin() + search_from;
  auto it = std::upper_bound(begin, _row_group_starts.end(), row_number);
  SDB_ASSERT(it != _row_group_starts.begin(), "row before first row group");
  return it - _row_group_starts.begin() - 1;
}

int64_t ParquetMaterializer::RowGroupEnd(uint32_t rg) const {
  if (rg + 1 < _row_group_starts.size()) {
    return _row_group_starts[rg + 1];
  }
  return _total_rows;
}

velox::RowVectorPtr ParquetMaterializer::ReadRows(
  std::span<std::string> row_keys, velox::VectorPtr scores,
  std::vector<velox::VectorPtr> offsets_per_field) {
  if (row_keys.empty()) {
    return nullptr;
  }
  auto total = static_cast<velox::vector_size_t>(row_keys.size());

  if (_score_column_idx >= 0) {
    SDB_ASSERT(scores);
    auto* score_raw = scores->asFlatVector<float>()->mutableRawValues();
    std::span score_span{score_raw, row_keys.size()};
    std::ranges::sort(std::views::zip(row_keys, score_span), std::less{},
                      [](const auto& p) { return std::get<0>(p); });
  } else {
    std::sort(row_keys.begin(), row_keys.end());
  }

  _decoded_rows.resize(total);
  for (velox::vector_size_t i = 0; i < total; ++i) {
    _decoded_rows[i] = primary_key::ReadSigned<int64_t>(row_keys[i]);
  }

  velox::VectorPtr output =
    velox::BaseVector::create<velox::RowVector>(_output_type, total, &_pool);

  auto& parquet_reader =
    basics::downCast<velox::parquet::ParquetRowReader>(*_row_reader);

  uint32_t last_rg = 0;
  velox::vector_size_t out_offset = 0;

  std::span row_idx = _decoded_rows;
  for (velox::vector_size_t i = 0; i < total;) {
    auto rg = FindRowGroup(row_idx[i], last_rg);
    auto rg_start = _row_group_starts[rg];
    auto rg_end = RowGroupEnd(rg);

    // collect all row in this row group.
    auto it = std::lower_bound(row_idx.begin() + i, row_idx.end(), rg_end);
    velox::vector_size_t end = it - row_idx.begin();

    // mark wanted row_idx as not-deleted in the pre-filled bitmap.
    auto last_offset = row_idx[end - 1] - rg_start;
    uint64_t read_size = last_offset + 1;
    auto* bits = _bitmap_buf.data();
    for (auto k = i; k < end; ++k) {
      velox::bits::clearBit(bits, row_idx[k] - rg_start);
    }

    velox::dwio::common::Mutation mutation{};
    mutation.deletedRows = bits;

    parquet_reader.seekToRowGroup(rg);

    velox::VectorPtr result =
      velox::BaseVector::create(_output_type, read_size, &_pool);
    parquet_reader.next(read_size, result, &mutation);

    output->copy(result.get(), out_offset, 0, end - i);
    out_offset += end - i;

    for (auto k = i; k < end; ++k) {
      velox::bits::setBit(bits, row_idx[k] - rg_start);
    }

    last_rg = rg;
    i = end;
  }

  auto result = basics::downCast<velox::RowVector>(std::move(output));
  if (_score_column_idx >= 0) {
    SDB_ASSERT(scores);
    SDB_ASSERT(scores->size() == row_keys.size());
    scores->resize(result->size());
    result->children()[_score_column_idx] = std::move(scores);
  }
  for (size_t fi = 0; fi < _offsets_column_indices.size(); ++fi) {
    SDB_ASSERT(fi < offsets_per_field.size());
    auto& offsets = offsets_per_field[fi];
    SDB_ASSERT(offsets);
    SDB_ASSERT(offsets->size() == row_keys.size());
    offsets->resize(result->size());
    result->children()[_offsets_column_indices[fi]] = std::move(offsets);
  }
  return result;
}

}  // namespace sdb::connector
