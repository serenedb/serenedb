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

#include "iresearch/columnstore/norm_writer.hpp"

#include <cstdint>
#include <limits>
#include <utility>

#include "iresearch/columnstore/format.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs::columnstore {
namespace {

constexpr uint8_t PickByteSize(uint32_t max) noexcept {
  if (max <= std::numeric_limits<uint8_t>::max()) {
    return 1;
  }
  if (max <= std::numeric_limits<uint16_t>::max()) {
    return 2;
  }
  return 4;
}

}  // namespace

NormColumnWriter::NormColumnWriter(field_id id, uint64_t row_group_size,
                                   IndexOutput& out)
  : _id{id},
    _row_group_size{row_group_size != 0 ? row_group_size
                                        : kDefaultRowGroupSize},
    _out{&out} {
  _pending.reserve(_row_group_size);
}

void NormColumnWriter::Append(uint64_t target_row, uint32_t value) {
  SDB_ASSERT(!_finalized, "NormColumnWriter::Append after Finalize on column ",
             _id);
  SDB_ASSERT(target_row >= RowCount(),
             "NormColumnWriter::Append target_row=", target_row,
             " below RowCount=", RowCount(), " on column ", _id);
  PadTo(target_row);
  _pending.push_back(value);
  if (_pending.size() == _row_group_size) {
    FlushRowGroup();
  }
}

uint64_t NormColumnWriter::RowCount() const noexcept {
  uint64_t n = _pending.size();
  for (const auto& p : _pointers) {
    n += p.row_count;
  }
  return n;
}

void NormColumnWriter::PadTo(uint64_t target) {
  const auto current = RowCount();
  if (current >= target) {
    return;
  }
  SDB_ASSERT(!_finalized, "NormColumnWriter::PadTo after Finalize on column ",
             _id);
  uint64_t needed = target - current;
  while (needed != 0) {
    const auto remaining = _row_group_size - _pending.size();
    const auto chunk = std::min<uint64_t>(needed, remaining);
    _pending.insert(_pending.end(), chunk, 0u);
    needed -= chunk;
    if (_pending.size() == _row_group_size) {
      FlushRowGroup();
    }
  }
}

void NormColumnWriter::FlushRowGroup() {
  if (_pending.empty()) {
    return;
  }
  uint32_t rg_max = 0;
  uint64_t rg_sum = 0;
  uint64_t rg_non_zero = 0;
  for (const auto v : _pending) {
    if (v > rg_max) {
      rg_max = v;
    }
    rg_sum += v;
    rg_non_zero += static_cast<uint64_t>(v != 0);
  }
  const uint8_t byte_size = PickByteSize(rg_max);
  NormRowGroupPointer ptr{
    .byte_size = byte_size,
    .row_count = _pending.size(),
    .max = rg_max,
    .sum = rg_sum,
    .non_zero_count = rg_non_zero,
    .file_offset = _out->Position(),
  };
  if (byte_size == 1) {
    for (const auto v : _pending) {
      _out->WriteByte(static_cast<byte_type>(v));
    }
  } else if (byte_size == 2) {
    for (const auto v : _pending) {
      _out->WriteU16(static_cast<uint16_t>(v));
    }
  } else {
    for (const auto v : _pending) {
      _out->WriteU32(v);
    }
  }
  _pointers.push_back(ptr);
  _pending.clear();
}

void NormColumnWriter::Finalize() {
  if (_finalized) {
    return;
  }
  _finalized = true;
  FlushRowGroup();
  std::vector<uint32_t>{}.swap(_pending);
}

}  // namespace irs::columnstore
