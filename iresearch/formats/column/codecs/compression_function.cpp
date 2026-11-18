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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/buffer_manager.hpp>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

uint64_t StringBytes(std::span<const WriteChunk> chunks) {
  uint64_t bytes = 0;
  for (const auto& chunk : chunks) {
    const auto& input = chunk.data;
    if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
      for (const auto value : input.Values<duckdb::string_t>()) {
        if (value.IsValid()) {
          bytes += value.GetValueUnsafe().GetSize();
        }
      }
      continue;
    }
    const auto* values = duckdb::FlatVector::GetData<duckdb::string_t>(input);
    const auto& validity = duckdb::FlatVector::Validity(input);
    const auto count = input.size();
    if (validity.CannotHaveNull()) {
      for (duckdb::idx_t i = 0; i < count; ++i) {
        bytes += values[i].GetSize();
      }
    } else {
      for (duckdb::idx_t i = 0; i < count; ++i) {
        bytes += validity.RowIsValid(i) ? values[i].GetSize() : 0;
      }
    }
  }
  return bytes;
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

duckdb::idx_t CountRows(std::span<const WriteChunk> chunks) noexcept {
  duckdb::idx_t rows = 0;
  for (const auto& chunk : chunks) {
    rows += chunk.data.size();
  }
  return rows;
}

uint64_t RawInput(ColumnDataCheckpointData& data) {
  if (data.raw_input == 0) {
    const auto physical = data.type.InternalType();
    const auto bytes =
      physical == duckdb::PhysicalType::VARCHAR
        ? StringBytes(data.chunks)
        : data.rows * std::max<uint64_t>(duckdb::GetTypeIdSize(physical), 1);
    data.raw_input = std::max<uint64_t>(bytes, 1);
  }
  return data.raw_input;
}

bool HasRows(std::span<const WriteChunk> chunks) noexcept {
  return absl::c_any_of(
    chunks, [](const WriteChunk& chunk) { return chunk.data.size() != 0; });
}

const CompressionFunction& UncompressedCompression(
  duckdb::PhysicalType physical) {
  const auto* codec = GetCodec(CodecType::Uncompressed, physical);
  SDB_ENSURE(codec && codec->compression,
             "column writer: no codec writes the physical type ",
             duckdb::TypeIdToString(physical));
  return *codec->compression;
}

void Compress(CodecType method, ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  data.rows = CountRows(data.chunks);
  const auto* codec = GetCodec(method, physical);
  SDB_ASSERT(codec && codec->compression);
  if (!codec->compression->compress(data)) {
    UncompressedCompression(physical).compress(data);
  }
}

bool Writes(CodecType type, duckdb::PhysicalType physical,
            const CodecSet& disabled) noexcept {
  if (type == CodecType::Auto || disabled.test(std::to_underlying(type))) {
    return false;
  }
  const auto* codec = GetCodec(type, physical);
  return codec && codec->compression;
}

void ThrowCorrupted(std::string_view codec, std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("Corrupted ", codec, " segment: ", what));
}

void ColumnDataCheckpointData::FlushSegment(CodecType codec,
                                            duckdb::BaseStatistics stats,
                                            uint64_t rows,
                                            std::string_view bytes) {
  const auto physical = type.InternalType();
  if (stats.IsConstant() &&
      !disabled.test(std::to_underlying(CodecType::Constant))) {
    if (const auto* constant = GetCodec(CodecType::Constant, physical)) {
      flush(*constant, std::move(stats), rows, {});
      return;
    }
  }
  const auto* writer = GetCodec(codec, physical);
  SDB_ASSERT(writer);
  written += bytes.size();
  flush(*writer, std::move(stats), rows, bytes);
}

CompressedSegment::CompressedSegment(CodecType codec,
                                     const duckdb::LogicalType& type)
  : stats{duckdb::BaseStatistics::CreateEmpty(type)},
    _codec{codec},
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
  checkpoint_data.FlushSegment(
    _codec, std::move(stats), count,
    {reinterpret_cast<const char*>(_buffer.get()), size});
  Reset();
}

}  // namespace irs::codecs
