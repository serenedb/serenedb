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

#include "basics/down_cast.h"
#include "connector/primary_key.hpp"

namespace sdb::connector {

ParquetMaterializer::ParquetMaterializer(
  velox::memory::MemoryPool& pool, std::shared_ptr<velox::ReadFile> source,
  std::unique_ptr<velox::dwio::common::Reader> reader,
  std::unique_ptr<velox::dwio::common::RowReader> row_reader,
  velox::RowTypePtr output_type)
  : _pool{pool},
    _source{std::move(source)},
    _reader{std::move(reader)},
    _row_reader{std::move(row_reader)},
    _output_type{std::move(output_type)} {
  auto& parquet_reader =
    basics::downCast<velox::parquet::ParquetReader>(*_reader);
  auto metadata = parquet_reader.fileMetaData();
  auto num_groups = metadata.numRowGroups();
  _row_group_starts.reserve(num_groups);
  int64_t offset = 0;
  for (int i = 0; i < num_groups; ++i) {
    _row_group_starts.push_back(offset);
    offset += metadata.rowGroup(i).numRows();
  }
  _total_rows = offset;
}

uint32_t ParquetMaterializer::RowGroupForRow(int64_t row) const {
  auto it =
    std::upper_bound(_row_group_starts.begin(), _row_group_starts.end(), row);
  SDB_ASSERT(it != _row_group_starts.begin(), "row before first row group");
  return static_cast<uint32_t>(it - _row_group_starts.begin() - 1);
}

velox::RowVectorPtr ParquetMaterializer::ReadRows(
  std::span<std::string> row_keys) {
  if (row_keys.empty()) {
    return nullptr;
  }

  auto decode = [](std::string_view key) {
    return primary_key::ReadSigned<int64_t>(key);
  };

  auto total = static_cast<velox::vector_size_t>(row_keys.size());
  auto output =
    velox::BaseVector::create<velox::RowVector>(_output_type, total, &_pool);

  velox::vector_size_t written = 0;
  size_t i = 0;

  while (i < row_keys.size()) {
    auto first_row = decode(row_keys[i]);
    auto rg = RowGroupForRow(first_row);
    auto rg_start = _row_group_starts[rg];
    auto rg_end = (rg + 1 < _row_group_starts.size())
                    ? _row_group_starts[rg + 1]
                    : _total_rows;

    size_t j = i + 1;
    while (j < row_keys.size() && decode(row_keys[j]) < rg_end) {
      ++j;
    }

    auto& parquet_reader =
      basics::downCast<velox::parquet::ParquetRowReader>(*_row_reader);
    parquet_reader.seekToRowGroup(rg);

    auto last_offset = decode(row_keys[j - 1]) - rg_start;
    auto read_size = static_cast<uint64_t>(last_offset + 1);
    auto num_words = velox::bits::nwords(read_size);
    std::vector<uint64_t> deleted(num_words, ~uint64_t{0});
    for (size_t k = i; k < j; ++k) {
      velox::bits::clearBit(deleted.data(), decode(row_keys[k]) - rg_start);
    }

    velox::dwio::common::Mutation mutation;
    mutation.deletedRows = deleted.data();

    velox::VectorPtr result =
      velox::BaseVector::create(_output_type, read_size, &_pool);
    parquet_reader.next(read_size, result, &mutation);
    if (result && result->size() > 0) {
      auto row_batch = basics::downCast<velox::RowVector>(std::move(result));
      for (auto& child : row_batch->children()) {
        child->loadedVector();
      }
      auto batch_size = row_batch->size();
      for (size_t col = 0; col < _output_type->size(); ++col) {
        output->childAt(col)->copy(row_batch->children()[col].get(), written, 0,
                                   batch_size);
      }
      written += batch_size;
    }

    i = j;
  }

  if (written == 0) {
    return nullptr;
  }

  output->resize(written);
  _produced += written;
  return output;
}

}  // namespace sdb::connector
