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

#pragma once

#include <absl/functional/any_invocable.h>

#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/write_stream.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"

namespace irs {

inline constexpr uint32_t kNormCodes = 16;
inline constexpr uint32_t kNormDirect = kNormCodes - 1;
inline constexpr uint64_t kNormExceptionShare = 256;
inline constexpr uint64_t kNormBucketTarget = 4;
inline constexpr uint32_t kNormMaxShift = 24;
inline constexpr size_t kNormValueSlack = sizeof(uint32_t);

constexpr uint32_t NormFirstCode(uint32_t bits) noexcept {
  return static_cast<uint32_t>((uint64_t{1} << bits) - kNormCodes);
}

constexpr uint32_t NormOverflowCode(uint32_t bits) noexcept {
  return static_cast<uint32_t>((uint64_t{1} << bits) - 1);
}

struct NormStats {
  uint64_t rows = 0;
  uint64_t sum = 0;
  uint64_t non_zero = 0;
  uint64_t wide8 = 0;
  uint64_t wide16 = 0;
  uint32_t min = std::numeric_limits<uint32_t>::max();
  uint32_t max = 0;

  void Add(std::span<const uint32_t> values) noexcept;
  void Merge(const NormStats& other) noexcept;
};

struct NormLayout {
  uint32_t bits = 0;
  uint32_t value = 0;
  uint64_t exceptions = 0;
};

NormLayout PickNormLayout(const NormStats& stats) noexcept;

struct NormRegionMeta {
  NormStats stats;
  uint64_t file_offset = 0;
  uint64_t table_offset = 0;
  uint32_t value = 0;
  uint32_t exceptions = 0;
  uint32_t overflow = 0;
  uint8_t bits = 0;
  uint8_t shift = 0;
  uint8_t exception_bytes = 0;
};

struct NormColumnMeta {
  uint64_t row_count = 0;
  std::vector<NormRegionMeta> regions;
};

struct NormColumnSpec {
  field_id id = 0;
  NormColumnMeta meta;
};

inline uint64_t NormSlotsSize(const NormRegionMeta& region) noexcept {
  return region.stats.rows * (region.bits / 8);
}

inline uint64_t NormBuckets(const NormRegionMeta& region) noexcept {
  return ((region.stats.rows - 1) >> region.shift) + 1;
}

inline uint64_t NormTableSize(const NormRegionMeta& region) noexcept {
  return NormBuckets(region) * sizeof(uint32_t) +
         uint64_t{region.overflow} * 2 * sizeof(uint32_t) +
         uint64_t{region.exceptions - region.overflow} *
           region.exception_bytes +
         kNormValueSlack;
}

void WriteNormColumns(duckdb::BinarySerializer& out, duckdb::field_id_t field,
                      std::span<const NormColumnSpec> columns);

class NormColumnWriter final {
 public:
  NormColumnWriter(field_id id, uint32_t row_group_size,
                   duckdb::WriteStream& out);

  NormColumnWriter(const NormColumnWriter&) = delete;
  NormColumnWriter& operator=(const NormColumnWriter&) = delete;

  void Append(uint64_t target_row, uint32_t value);

  void PadTo(uint64_t target);

  void OpenRegion(const NormStats& planned);

  void Write(std::span<const uint32_t> values);

  void CloseRegion();

  void Finalize();

  field_id Id() const noexcept { return _id; }

  uint64_t RowCount() const noexcept {
    return _meta.row_count + _values.size() + _region.stats.rows;
  }

  const NormColumnMeta& Meta() const noexcept { return _meta; }
  NormColumnMeta TakeMeta() noexcept { return std::move(_meta); }

 private:
  void FlushRowGroup();
  void Begin(const NormLayout& layout, uint64_t rows, uint32_t max);
  void Encode(std::span<const uint32_t> values);
  template<size_t Bytes>
  void EncodeSlots(std::span<const uint32_t> values, uint64_t first_row);
  uint32_t Escape(uint64_t row, uint32_t value);
  void End();

  field_id _id;
  duckdb::WriteStream* _out;
  uint32_t _row_group_size;
  NormColumnMeta _meta;
  std::vector<uint32_t> _values;
  NormRegionMeta _region;
  bool _open_region = false;
  uint64_t _bucket = 0;
  uint32_t _local = 0;
  std::vector<uint32_t> _bases;
  std::vector<uint32_t> _direct;
  std::vector<uint32_t> _overflow_rows;
  std::vector<uint32_t> _overflow_values;
  std::vector<byte_type> _staging;
};

struct SealedNorms {
  duckdb::shared_ptr<duckdb::InputFile> file;
  std::vector<NormColumnSpec> columns;
};

class NormsWriter final {
 public:
  explicit NormsWriter(absl::AnyInvocable<duckdb::WriteStream&()> open) noexcept
    : _open{std::move(open)} {}

  NormsWriter(const NormsWriter&) = delete;
  NormsWriter& operator=(const NormsWriter&) = delete;

  NormColumnWriter& Open(field_id id, uint32_t row_group_size);

  NormColumnWriter& Stream(field_id id);

  std::vector<NormColumnSpec> Seal(uint64_t rows) &&;

 private:
  NormColumnWriter& Add(field_id id, uint32_t row_group_size);

  absl::AnyInvocable<duckdb::WriteStream&()> _open;
  duckdb::WriteStream* _out = nullptr;
  std::vector<std::unique_ptr<NormColumnWriter>> _columns;
  containers::FlatHashMap<field_id, NormColumnWriter*> _by_id;
};

class BufferedNorms final {
 public:
  explicit BufferedNorms(duckdb::Allocator& allocator)
    : _allocator{allocator}, _writer{[this] -> duckdb::WriteStream& {
        return _buffer.emplace(_allocator);
      }} {}

  BufferedNorms(const BufferedNorms&) = delete;
  BufferedNorms& operator=(const BufferedNorms&) = delete;

  NormsWriter& Writer() noexcept { return _writer; }

  uint64_t Bytes() const noexcept {
    return _buffer ? _buffer->GetPosition() : 0;
  }
  uint64_t Capacity() const noexcept {
    return _buffer ? _buffer->GetCapacity() : 0;
  }

  SealedNorms Seal(uint64_t rows) &&;

 private:
  duckdb::Allocator& _allocator;
  std::optional<duckdb::MemoryStream> _buffer;
  NormsWriter _writer;
};

}  // namespace irs
