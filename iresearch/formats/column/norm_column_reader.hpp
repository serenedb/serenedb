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
#include <vector>

#include "iresearch/formats/column/norm_writer.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class IndexInput;

IRS_FORCE_INLINE inline uint32_t NormSlot(const byte_type* base, uint32_t bits,
                                          doc_id_t doc) noexcept {
  const uint64_t bit = uint64_t{doc} * bits;
  return static_cast<uint32_t>(
    (absl::little_endian::Load64(base + (bit >> 3)) >> (bit & 7)) &
    ((uint64_t{1} << bits) - 1));
}

class NormColumnReader final {
 public:
  struct Window {
    const byte_type* base;
    doc_id_t first_doc;
    doc_id_t end_doc;
    size_t rg;
    uint32_t bits;
  };

  NormColumnReader(field_id id, NormColumnMeta meta, IndexInput& in);

  field_id Id() const noexcept { return _id; }
  size_t RowGroupCount() const noexcept { return _windows.size(); }
  uint64_t RowCount() const noexcept { return _row_count; }
  uint64_t Sum() const noexcept { return _sum; }
  uint64_t NonZeroCount() const noexcept { return _non_zero; }
  bool Uniform() const noexcept { return _uniform; }
  bool HasExceptions() const noexcept { return _exceptions != 0; }
  uint32_t MaxBits() const noexcept { return _max_bits; }

  uint32_t Bits(size_t rg) const noexcept {
    SDB_ASSERT(rg < _windows.size());
    return _windows[rg].bits;
  }

  const Window& Rg(size_t rg) const noexcept {
    SDB_ASSERT(rg < _windows.size());
    return _windows[rg];
  }

  const Window& Locate(doc_id_t doc) const noexcept {
    SDB_ASSERT(doc >= doc_limits::min());
    SDB_ASSERT(doc - doc_limits::min() < _row_count);
    return _windows[(doc - doc_limits::min()) / _rg_rows];
  }

  uint64_t RowGroupFirstRow(size_t rg) const noexcept {
    SDB_ASSERT(rg < _windows.size());
    return rg * _rg_rows;
  }

  uint64_t RowGroupRowCount(size_t rg) const noexcept {
    SDB_ASSERT(rg < _windows.size());
    return _windows[rg].end_doc - _windows[rg].first_doc;
  }

  std::span<const byte_type> RowGroupExtent(size_t rg) const noexcept;

  uint32_t Exception(doc_id_t doc) const noexcept;

  uint32_t Get(uint64_t row) const noexcept;

  void Decode(size_t rg, uint32_t* values) const noexcept;

 private:
  field_id _id;
  std::vector<Window> _windows;
  std::vector<byte_type> _owned;
  const byte_type* _begin = nullptr;
  const byte_type* _offsets = nullptr;
  const byte_type* _values = nullptr;
  uint64_t _rg_rows = 1;
  uint64_t _row_count = 0;
  uint64_t _sum = 0;
  uint64_t _non_zero = 0;
  uint32_t _exceptions = 0;
  uint32_t _max_bits = 0;
  uint8_t _exception_bytes = 0;
  bool _uniform = true;
};

}  // namespace irs
