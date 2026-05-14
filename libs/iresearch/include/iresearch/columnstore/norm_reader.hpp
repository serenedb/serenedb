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

#include <absl/base/internal/endian.h>

#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "iresearch/columnstore/norm_writer.hpp"
#include "iresearch/types.hpp"

namespace irs {

class IndexInput;

namespace columnstore {

class NormColumnReader final {
 public:
  NormColumnReader(field_id id, std::vector<NormRowGroupPointer> pointers,
                   IndexInput& in);

  field_id Id() const noexcept { return _id; }
  size_t RowGroupCount() const noexcept { return _pointers.size(); }
  uint64_t RowCount() const noexcept { return _total_row_count; }

  uint64_t Sum() const noexcept { return _total_sum; }
  uint64_t NonZeroCount() const noexcept { return _total_non_zero; }

  uint8_t ByteSize(size_t rg) const noexcept {
    SDB_ASSERT(rg < _pointers.size());
    return _pointers[rg].byte_size;
  }
  uint64_t RowGroupRowCount(size_t rg) const noexcept {
    SDB_ASSERT(rg < _pointers.size());
    return _pointers[rg].row_count;
  }
  uint64_t RowGroupFirstRow(size_t rg) const noexcept {
    SDB_ASSERT(rg < _pointers.size());
    return _row_offsets[rg];
  }
  std::span<const byte_type> RowGroupBytes(size_t rg) const noexcept {
    SDB_ASSERT(rg < _spans.size());
    return _spans[rg];
  }

  std::pair<size_t, uint64_t> Locate(uint64_t row_pos) const noexcept;

  uint32_t Get(uint64_t row_pos) const noexcept;

 private:
  field_id _id;
  std::vector<NormRowGroupPointer> _pointers;
  std::vector<uint64_t> _row_offsets;  // cumulative; size = pointers + 1
  std::vector<std::span<const byte_type>> _spans;
  // Fallback heap copies for non-mmap backends; usually empty.
  std::vector<std::vector<byte_type>> _owned;
  uint64_t _total_row_count = 0;
  uint64_t _total_sum = 0;
  uint64_t _total_non_zero = 0;
};

// Decode one stored value from a row-group's raw bytes.
inline uint32_t ReadNormValue(const byte_type* bytes,
                              uint8_t byte_size) noexcept {
  if (byte_size == 1) {
    return *bytes;
  }
  if (byte_size == 2) {
    return absl::little_endian::Load16(bytes);
  }
  return absl::little_endian::Load32(bytes);
}

}  // namespace columnstore
}  // namespace irs
