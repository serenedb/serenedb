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

#include "iresearch/formats/column/norm_writer.hpp"

#include <absl/base/internal/endian.h>

#include <cstdint>
#include <cstring>
#include <duckdb/storage/storage_info.hpp>
#include <limits>
#include <utility>

#include "basics/resource_manager.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
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

template<uint8_t ByteSize>
IRS_FORCE_INLINE uint32_t LoadValue(const byte_type* src) noexcept {
  static_assert(ByteSize == 1 || ByteSize == 2 || ByteSize == 4);
  if constexpr (ByteSize == 1) {
    return *src;
  } else if constexpr (ByteSize == 2) {
    return absl::little_endian::Load16(src);
  } else {
    return absl::little_endian::Load32(src);
  }
}

}  // namespace

NormColumnWriter::NormColumnWriter(field_id id, uint32_t row_group_size,
                                   IndexOutput& out)
  : _id{id},
    _row_group_size{row_group_size},
    _out{&out},
    _pending{IResourceManager::gNoop, 32} {
  SDB_ASSERT(_row_group_size != 0);
}

void NormColumnWriter::Append(uint64_t target_row, uint32_t value) {
  SDB_ASSERT(target_row >= RowCount(),
             "NormColumnWriter::Append target_row=", target_row,
             " below RowCount=", RowCount(), " on column ", _id);
  PadTo(target_row);
  auto* p = _pending.Allocate(1);
  *p = value;
  if (!_spans.empty() && _spans.back().data() + _spans.back().size() == p) {
    _spans.back() = std::span{_spans.back().data(), _spans.back().size() + 1};
  } else {
    _spans.emplace_back(p, size_t{1});
  }
  ++_filled;
  _rg_max = std::max(_rg_max, value);
  _rg_sum += value;
  _rg_non_zero += static_cast<uint64_t>(value != 0);
  if (_filled == _row_group_size) {
    FlushRowGroup();
  }
}

namespace {

template<uint8_t ByteSize>
IRS_FORCE_INLINE void DecodeRun(uint32_t* IRS_RESTRICT dst,
                                const byte_type* IRS_RESTRICT src, size_t count,
                                uint32_t& rg_max, uint64_t& rg_sum,
                                uint64_t& rg_non_zero) noexcept {
  uint32_t m = rg_max;
  uint64_t sum = rg_sum;
  uint64_t nz = rg_non_zero;
  for (size_t i = 0; i != count; ++i) {
    const auto v = LoadValue<ByteSize>(src + i * ByteSize);
    dst[i] = v;
    m = std::max(m, v);
    sum += v;
    nz += static_cast<uint64_t>(v != 0);
  }
  rg_max = m;
  rg_sum = sum;
  rg_non_zero = nz;
}

}  // namespace

void NormColumnWriter::AppendBytes(uint64_t target_row, const byte_type* src,
                                   size_t count, uint8_t byte_size) {
  SDB_ASSERT(target_row >= RowCount(),
             "NormColumnWriter::AppendBytes target_row=", target_row,
             " below RowCount=", RowCount(), " on column ", _id);
  SDB_ASSERT(byte_size == 1 || byte_size == 2 || byte_size == 4,
             "NormColumnWriter::AppendBytes invalid byte_size=",
             static_cast<uint32_t>(byte_size));
  PadTo(target_row);
  size_t remaining = count;
  while (remaining != 0) {
    const auto until_flush = _row_group_size - _filled;
    const auto chunk = std::min<uint64_t>(remaining, until_flush);
    auto* p = _pending.Allocate(chunk);
    switch (byte_size) {
      case 1:
        DecodeRun<1>(p, src, chunk, _rg_max, _rg_sum, _rg_non_zero);
        break;
      case 2:
        DecodeRun<2>(p, src, chunk, _rg_max, _rg_sum, _rg_non_zero);
        break;
      default:
        DecodeRun<4>(p, src, chunk, _rg_max, _rg_sum, _rg_non_zero);
        break;
    }
    if (!_spans.empty() && _spans.back().data() + _spans.back().size() == p) {
      _spans.back() =
        std::span{_spans.back().data(), _spans.back().size() + chunk};
    } else {
      _spans.emplace_back(p, chunk);
    }
    _filled += chunk;
    src += chunk * byte_size;
    remaining -= chunk;
    if (_filled == _row_group_size) {
      FlushRowGroup();
    }
  }
}

uint64_t NormColumnWriter::RowCount() const noexcept {
  return _flushed + _filled;
}

void NormColumnWriter::PadTo(uint64_t target) {
  const auto current = RowCount();
  if (current >= target) {
    return;
  }
  uint64_t needed = target - current;
  while (needed != 0) {
    const auto until_flush = _row_group_size - _filled;
    const auto chunk = std::min<uint64_t>(needed, until_flush);
    auto* p = _pending.Allocate(chunk);
    std::memset(p, 0, chunk * sizeof(uint32_t));
    if (!_spans.empty() && _spans.back().data() + _spans.back().size() == p) {
      _spans.back() =
        std::span{_spans.back().data(), _spans.back().size() + chunk};
    } else {
      _spans.emplace_back(p, chunk);
    }
    _filled += chunk;
    needed -= chunk;
    if (_filled == _row_group_size) {
      FlushRowGroup();
    }
  }
}

void NormColumnWriter::FlushRowGroup() {
  if (_filled == 0) {
    return;
  }
  const uint8_t byte_size = PickByteSize(_rg_max);
  NormRowGroupPointer ptr{
    .byte_size = byte_size,
    .row_count = _filled,
    .max = _rg_max,
    .sum = _rg_sum,
    .non_zero_count = _rg_non_zero,
    .file_offset = _out->Position(),
  };
  for (const auto span : _spans) {
    if (byte_size == 1) {
      for (auto v : span) {
        _out->WriteByte(static_cast<byte_type>(v));
      }
    } else if (byte_size == 2) {
      for (auto v : span) {
        _out->WriteU16(static_cast<uint16_t>(v));
      }
    } else {
      for (auto v : span) {
        _out->WriteU32(v);
      }
    }
  }
  _pointers.push_back(ptr);
  _flushed += _filled;
  _filled = 0;
  _rg_max = 0;
  _rg_sum = 0;
  _rg_non_zero = 0;
  _spans.clear();
  _pending.Clear();
}

void NormColumnWriter::Finalize() {
  FlushRowGroup();
  _spans = {};
  _pending.Reset();
}

}  // namespace irs
