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

#include <algorithm>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/table/column_segment.hpp>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

bool CompressAs(CodecType type, ColumnDataCheckpointData& data) {
  const auto* codec = GetCodec(type, data.type.InternalType());
  return codec && CompressAs(*codec, data);
}

}  // namespace

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

void CompressUncompressed(ColumnDataCheckpointData& data) {
  SDB_ENSURE(CompressAs(CodecType::Uncompressed, data),
             "column writer: no codec writes the physical type ",
             duckdb::TypeIdToString(data.type.InternalType()));
}

void Compress(CodecType method, ColumnDataCheckpointData& data) {
  if (CompressAs(CodecType::Empty, data) ||
      (!data.disabled.test(std::to_underlying(CodecType::Constant)) &&
       CompressAs(CodecType::Constant, data))) {
    return;
  }
  ++data.tuning.row_group;
  if (method == CodecType::Auto) {
    CompressAuto(data);
  } else if (!CompressAs(method, data)) {
    CompressUncompressed(data);
  }
}

void ThrowCorrupted(std::string_view codec, std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("Corrupted ", codec, " segment: ", what));
}

duckdb::idx_t FilterRows(duckdb::SelectionVector& sel, duckdb::Vector& values,
                         duckdb::TableFilterState& filter_state,
                         duckdb::idx_t count, duckdb::idx_t& sel_count) {
  SDB_ASSERT(sel_count != count || count == 0 || !sel.IsSet() ||
             (sel.get_index(0) == 0 && sel.get_index(count - 1) == count - 1));
  if (values.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
    duckdb::FlatVector::SetSize(values, count);
  }
  if (sel_count != count) {
    return duckdb::ColumnSegment::FilterSelection(sel, values, filter_state,
                                                  count, sel_count);
  }
  duckdb::SelectionVector every_row;
  duckdb::ColumnSegment::FilterSelection(every_row, values, filter_state, count,
                                         sel_count);
  if (sel_count != count) {
    sel.Initialize(every_row);
  }
  return sel_count;
}

CompressedSegment::CompressedSegment(const Codec& codec,
                                     const duckdb::LogicalType& type)
  : stats{duckdb::BaseStatistics::CreateEmpty(type)},
    _codec{codec},
    _type{type},
    _buffer{std::make_unique_for_overwrite<duckdb::data_t[]>(kBlockSize)} {}

void CompressedSegment::Reset() {
  count = 0;
  stats = duckdb::BaseStatistics::CreateEmpty(_type);
}

void CompressedSegment::Flush(ColumnDataCheckpointData& checkpoint_data,
                              uint64_t size) {
  SDB_ASSERT(size <= kBlockSize);
  const auto physical = _type.InternalType();
  if (_type.id() == duckdb::LogicalTypeId::VALIDITY && !stats.CanHaveNull()) {
    checkpoint_data.flush(*EmptyCodec(physical),
                          duckdb::BaseStatistics::CreateEmpty(_type), count, {},
                          {});
  } else if (const auto* constant = ConstantCodec(physical);
             constant && stats.IsConstant()) {
    checkpoint_data.flush(*constant, std::move(stats), count, {}, {});
  } else {
    stored += size;
    checkpoint_data.flush(_codec, std::move(stats), count,
                          {reinterpret_cast<const char*>(_buffer.get()), size},
                          {});
  }
  Reset();
}

}  // namespace irs::codecs
