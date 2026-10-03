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
#include <span>
#include <vector>

#include "iresearch/types.hpp"

namespace irs {

class IndexOutput;

inline constexpr uint32_t kNormBucketShift = 8;
inline constexpr uint32_t kNormBucketRows = uint32_t{1} << kNormBucketShift;
inline constexpr uint32_t kNormMaxBits = 32;
inline constexpr size_t kNormSlotSlack = sizeof(uint64_t);
inline constexpr size_t kNormOffsetSlack = 32;
inline constexpr size_t kNormValueSlack = sizeof(uint32_t);

struct NormRowGroupMeta {
  uint8_t bits = 0;
  uint32_t max = 0;
  uint64_t sum = 0;
  uint64_t non_zero_count = 0;
  uint64_t file_offset = 0;
};

struct NormColumnMeta {
  uint32_t row_group_size = 0;
  uint64_t row_count = 0;
  uint64_t file_offset = 0;
  uint64_t size = 0;
  uint64_t exceptions_offset = 0;
  uint32_t exceptions = 0;
  uint8_t exception_bytes = 0;
  std::vector<NormRowGroupMeta> row_groups;
};

inline uint64_t NormBuckets(uint64_t rows) noexcept {
  return (rows + kNormBucketRows - 1) >> kNormBucketShift;
}

inline uint64_t NormExceptionsSize(uint64_t rows, uint64_t exceptions,
                                   uint64_t exception_bytes) noexcept {
  return (NormBuckets(rows) + 1) * sizeof(uint32_t) + exceptions +
         kNormOffsetSlack + exceptions * exception_bytes + kNormValueSlack;
}

class NormColumnWriter final {
 public:
  NormColumnWriter(field_id id, uint32_t row_group_size, IndexOutput& out);

  NormColumnWriter(const NormColumnWriter&) = delete;
  NormColumnWriter& operator=(const NormColumnWriter&) = delete;

  void Append(uint64_t target_row, uint32_t value);

  void AppendValues(uint64_t target_row, std::span<const uint32_t> values);

  void PadTo(uint64_t target);

  void Finalize();

  field_id Id() const noexcept { return _id; }

  uint64_t RowCount() const noexcept {
    return _meta.row_count + _values.size();
  }

  uint32_t RowGroupSize() const noexcept { return _meta.row_group_size; }

  const NormColumnMeta& Meta() const noexcept { return _meta; }

 private:
  void FlushRowGroup();

  field_id _id;
  IndexOutput* _out;
  NormColumnMeta _meta;
  std::vector<uint32_t> _values;
  std::vector<uint32_t> _exception_rows;
  std::vector<uint32_t> _exception_values;
};

}  // namespace irs
