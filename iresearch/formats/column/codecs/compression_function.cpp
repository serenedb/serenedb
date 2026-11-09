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

#include "iresearch/formats/column/codecs/compression_function.hpp"

#include <cstring>
#include <duckdb/storage/buffer_manager.hpp>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

duckdb::BufferHandle ColumnSegment::Pin() const {
  auto handle = block;
  return ctx.Database().GetBufferManager().Pin(handle);
}

duckdb::BufferHandle& ColumnFetchState::GetOrInsertHandle(
  const ColumnSegment& segment) {
  if (!handle.IsValid()) {
    handle = segment.Pin();
  }
  return handle;
}

void EmptySkip(const ColumnSegment&, ColumnScanState&, duckdb::idx_t) {}

void ThrowCorrupted(std::string_view codec, std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("Corrupted ", codec, " segment: ", what));
}

CompressedSegment::CompressedSegment(const duckdb::LogicalType& type)
  : stats{duckdb::BaseStatistics::CreateEmpty(type)},
    _type{type},
    _buffer{std::make_unique<duckdb::data_t[]>(kBlockSize)} {}

void CompressedSegment::Reset() {
  std::memset(_buffer.get(), 0, kBlockSize);
  count = 0;
  stats = duckdb::BaseStatistics::CreateEmpty(_type);
}

void CompressedSegment::Flush(ColumnDataCheckpointData& checkpoint_data,
                              uint64_t size) {
  SDB_ASSERT(size <= kBlockSize);
  checkpoint_data.flush(std::move(stats), count,
                        {reinterpret_cast<const char*>(_buffer.get()), size});
  Reset();
}

}  // namespace irs::codecs
