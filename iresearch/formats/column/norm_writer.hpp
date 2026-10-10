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

#include <cstdint>
#include <functional>
#include <limits>
#include <span>
#include <vector>

#include "iresearch/types.hpp"

namespace irs {

class IndexOutput;

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

class NormColumnWriter final {
 public:
  using Open = std::function<IndexOutput&()>;

  NormColumnWriter(field_id id, uint32_t row_group_size, IndexOutput& out);

  NormColumnWriter(field_id id, uint32_t row_group_size, Open open);

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

 private:
  void FlushRowGroup();
  void Begin(const NormLayout& layout, uint64_t rows, uint32_t max);
  void Encode(std::span<const uint32_t> values);
  template<size_t Bytes>
  void EncodeSlots(std::span<const uint32_t> values, uint64_t first_row);
  uint32_t Escape(uint64_t row, uint32_t value);
  void End();

  field_id _id;
  IndexOutput* _out = nullptr;
  Open _open;
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

}  // namespace irs
