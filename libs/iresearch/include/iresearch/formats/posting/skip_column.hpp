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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>

#include "basics/bit_packing.hpp"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "iresearch/error/error.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {

// The skip structure of one field: a fixed number of columns sharing one
// entry numbering, addressed rather than read.
//
// Entries are grouped 128 at a time. A group holds every column's 128 values,
// each column bitpacked at its own width against its own base, laid out
// contiguously:
//
//   group b   [ col 0: 128 vals ][ col 1: 128 vals ] ... [ col C-1 ]
//
// and is described by one fixed-stride directory record:
//
//   u32 off          where the group's payload starts
//   u32 base[C]
//   u8  width[C]
//
// Everything is computable from `k`: no varints, no pointer chase, no level,
// and the directory is used in place rather than decoded into memory.
//
//   b = k / 128, slot = k % 128
//   value(c, k) = base[b][c] + unpack(payload(b, c), slot, width[b][c])
//
// A group's payload is contiguous, so column `c` sits at a prefix sum over the
// group's widths - no offset per column is stored. Groups are also written
// back to back, which keeps a short term's entries from costing a block of
// their own: entries from many terms share one group.
inline constexpr uint32_t kSkipBlockSize = 128;
inline constexpr uint32_t kMaxSkipColumns = 8;

static_assert(kSkipBlockSize % packed::kBlockSize32 == 0);

// A column of `kSkipBlockSize` values at `width` bits occupies this many
// bytes. Exact, because the block size is a multiple of 8.
IRS_FORCE_INLINE constexpr uint32_t SkipColumnBytes(uint32_t width) noexcept {
  return kSkipBlockSize * width / 8;
}

IRS_FORCE_INLINE constexpr uint32_t SkipDirStride(uint32_t columns) noexcept {
  return sizeof(uint32_t) + columns * (sizeof(uint32_t) + sizeof(uint8_t));
}

class SkipColumnsWriter : util::Noncopyable {
 public:
  explicit SkipColumnsWriter(IResourceManager& rm)
    : _dir{ManagedTypedAllocator<byte_type>{rm}} {}

  void Reset(uint32_t columns, IndexOutput& out) {
    SDB_ASSERT(columns != 0 && columns <= kMaxSkipColumns);
    _columns = columns;
    _size = 0;
    _count = 0;
    _dir.clear();
    // Group payloads are multiples of 16 bytes, so aligning the first one
    // keeps every group aligned for `packed::At`.
    for (auto pos = out.Position(); pos % sizeof(uint32_t) != 0; ++pos) {
      out.WriteByte(0);
    }
    _origin = out.Position();
  }

  uint64_t Size() const noexcept { return _count; }

  // One entry: `values` holds this entry's value for each column.
  void Push(const uint32_t* values, IndexOutput& out) {
    for (uint32_t c = 0; c != _columns; ++c) {
      Column(c)[_size] = values[c];
    }
    ++_size;
    ++_count;
    if (_size == kSkipBlockSize) {
      FlushGroup(out);
    }
  }

  // Flushes the trailing partial group and appends the directory. Returns
  // where the directory starts.
  uint64_t Finish(IndexOutput& out) {
    if (_size != 0) {
      // Pad each column with its own last value so the tail does not widen
      // the group. Readers never address past `_count`.
      for (uint32_t c = 0; c != _columns; ++c) {
        auto* col = Column(c);
        std::fill(col + _size, col + kSkipBlockSize, col[_size - 1]);
      }
      _size = kSkipBlockSize;
      FlushGroup(out);
    }
    const uint64_t dir_start = out.Position();
    out.WriteData(_dir.data(), _dir.size());
    return dir_start;
  }

 private:
  uint32_t* Column(uint32_t c) noexcept { return _buf + c * kSkipBlockSize; }

  void FlushGroup(IndexOutput& out) {
    SDB_ASSERT(_size == kSkipBlockSize);
    const uint64_t off = out.Position() - _origin;
    if (off > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
      throw IndexError{
        "while writing skip columns, error: field skip data too large"};
    }

    const auto record = _dir.size();
    _dir.resize(record + SkipDirStride(_columns));
    auto* rec = _dir.data() + record;
    Store32(rec, static_cast<uint32_t>(off));
    auto* widths = rec + sizeof(uint32_t) + _columns * sizeof(uint32_t);

    for (uint32_t c = 0; c != _columns; ++c) {
      auto* col = Column(c);
      const auto [min_it, max_it] = std::minmax_element(
        col, col + kSkipBlockSize);
      const uint32_t base = *min_it;
      const uint32_t width = packed::Maxbits32(*max_it - base);
      SDB_ASSERT(width <= 32);

      Store32(rec + sizeof(uint32_t) + c * sizeof(uint32_t), base);
      widths[c] = static_cast<uint8_t>(width);

      if (width != 0) {
        for (uint32_t i = 0; i != kSkipBlockSize; ++i) {
          col[i] -= base;
        }
        const auto bytes = SkipColumnBytes(width);
        std::memset(_encoded, 0, bytes);
        packed::Pack(col, col + kSkipBlockSize, _encoded, width);
        out.WriteData(reinterpret_cast<const byte_type*>(_encoded), bytes);
      }
    }

    _size = 0;
  }

  static void Store32(byte_type* p, uint32_t v) noexcept {
    std::memcpy(p, &v, sizeof(v));
  }

  uint32_t _buf[kMaxSkipColumns * kSkipBlockSize];
  uint32_t _encoded[kSkipBlockSize];
  ManagedVector<byte_type> _dir;
  uint64_t _origin = 0;
  uint64_t _count = 0;
  uint32_t _columns = 0;
  uint32_t _size = 0;
};

// A field's skip columns as a reader sees them: the directory where it lies,
// plus where the payload and the field's `.doc` data start. Derived once per
// field from its term dictionary record.
struct SkipColumnsView {
  const byte_type* dir = nullptr;
  uint64_t count = 0;
  uint64_t origin = 0;
  uint64_t doc_origin = 0;
  uint32_t columns = 0;

  bool Empty() const noexcept { return count == 0; }

  size_t DirBytes() const noexcept {
    return math::DivCeil64(count, kSkipBlockSize) * SkipDirStride(columns);
  }
};

// Which column holds what, recovered from the field's features and how many
// columns it wrote. The order is fixed by `PostingsWriterBase::BeginSkipField`.
struct SkipColumnIndex {
  uint8_t docs = 0;
  uint8_t docoff = 1;
  uint8_t posoff = 0;
  uint8_t posslot = 0;
  uint8_t payoff = 0;
  uint8_t bfreq = 0;
  uint8_t bdelta = 0;
  bool has_bound = false;
  bool has_norm = false;

  constexpr SkipColumnIndex(uint32_t columns, bool has_pos,
                            bool has_pay) noexcept {
    uint8_t n = 2;
    if (has_pos) {
      posoff = n++;
      posslot = n++;
    }
    if (has_pay) {
      payoff = n++;
    }
    if (n < columns) {
      has_bound = true;
      bfreq = n++;
      if (n < columns) {
        has_norm = true;
        bdelta = n++;
      }
    }
  }
};

// Reads a field's skip columns in place. The directory is a byte range that
// stays where it is; a group is resolved once and its per-column pointers,
// bases and widths cached, so repeated `Get` inside one group is arithmetic.
template<typename InputType = IndexInput>
class SkipColumnsReader : util::Noncopyable {
 public:
  // `dir` is the field's directory, `origin` where its payload starts, and
  // `data` the stream the payload is read from.
  explicit SkipColumnsReader(IResourceManager& rm = IResourceManager::gNoop)
    : _dec{ManagedTypedAllocator<uint32_t>{rm}} {}

  void Prepare(const byte_type* dir, uint64_t count, uint32_t columns,
               uint64_t origin, InputType& data) {
    SDB_ASSERT(columns != 0 && columns <= kMaxSkipColumns);
    // Only a term long enough to have entries gets here, so a term that
    // never skips never pays for the decode buffer.
    _dec.resize(static_cast<size_t>(columns) * kSkipBlockSize);
    _dir = dir;
    _count = count;
    _columns = columns;
    _stride = SkipDirStride(columns);
    _origin = origin;
    _data = &data;
    _group = kNoGroup;
  }

  uint64_t Size() const noexcept { return _count; }

  // Resolving a group unpacks all of it, so both of these are an array
  // index. `packed::At` is an out-of-line call, and a seek makes several
  // reads -- the gallop probes the key, then one value from each column --
  // where the old skip entry was a handful of inline varints. A group covers
  // 128 entries, so it is decoded far less often than it is read.
  IRS_FORCE_INLINE uint32_t GetKey(uint64_t k) {
    SDB_ASSERT(k < _count);
    const auto group = k / kSkipBlockSize;
    if (group != _group) [[unlikely]] {
      Load(group);
    }
    return _dec[k % kSkipBlockSize];
  }

  IRS_FORCE_INLINE uint32_t Get(uint32_t column, uint64_t k) {
    SDB_ASSERT(k < _count);
    SDB_ASSERT(column < _columns);
    const auto group = k / kSkipBlockSize;
    if (group != _group) [[unlikely]] {
      Load(group);
    }
    return _dec[column * kSkipBlockSize + k % kSkipBlockSize];
  }

  // One value, without decoding the group. Opening a term wants only entry
  // 0's `docoff`, and a term that is merely read start to finish never wants
  // anything else -- decoding six columns for it would be waste.
  uint32_t GetOnce(uint32_t column, uint64_t k) {
    SDB_ASSERT(k < _count);
    SDB_ASSERT(column < _columns);
    const auto* rec = _dir + (k / kSkipBlockSize) * _stride;
    const auto* widths = rec + sizeof(uint32_t) + _columns * sizeof(uint32_t);
    uint32_t base;
    std::memcpy(&base, rec + sizeof(uint32_t) + column * sizeof(uint32_t),
                sizeof(base));
    const uint32_t width = widths[column];
    if (width == 0) {
      return base;
    }
    uint32_t off;
    std::memcpy(&off, rec, sizeof(off));
    for (uint32_t c = 0; c != column; ++c) {
      off += SkipColumnBytes(widths[c]);
    }
    const auto bytes = SkipColumnBytes(width);
    SDB_ASSERT(_data);
    _data->Seek(_origin + off);
    const auto* p = _data->ReadVolatile(bytes);
    if (p == nullptr) [[unlikely]] {
      SDB_ASSERT(bytes <= sizeof(_scratch));
      _data->ReadData(reinterpret_cast<byte_type*>(_scratch), bytes);
      p = reinterpret_cast<const byte_type*>(_scratch);
    }
    return base + packed::At(reinterpret_cast<const uint32_t*>(p),
                             k % kSkipBlockSize, width);
  }

  void Reset() noexcept { _group = kNoGroup; }

 private:
  static constexpr uint64_t kNoGroup = std::numeric_limits<uint64_t>::max();

  void Load(uint64_t group) {
    const auto* rec = _dir + group * _stride;
    uint32_t off;
    std::memcpy(&off, rec, sizeof(off));
    const auto* widths = rec + sizeof(uint32_t) + _columns * sizeof(uint32_t);

    uint32_t bytes = 0;
    for (uint32_t c = 0; c != _columns; ++c) {
      std::memcpy(&_base[c], rec + sizeof(uint32_t) + c * sizeof(uint32_t),
                  sizeof(uint32_t));
      _width[c] = widths[c];
      if (_width[c] > 32) {
        throw IndexError{"while loading skip columns, error: bad width"};
      }
      bytes += SkipColumnBytes(_width[c]);
    }

    SDB_ASSERT(_data);
    _data->Seek(_origin + off);
    const auto* payload = _data->ReadVolatile(bytes);
    if (payload == nullptr) [[unlikely]] {
      SDB_ASSERT(bytes <= sizeof(_scratch));
      _data->ReadData(reinterpret_cast<byte_type*>(_scratch), bytes);
      payload = reinterpret_cast<const byte_type*>(_scratch);
    }

    for (uint32_t c = 0; c != _columns; ++c) {
      auto* out = _dec.data() + static_cast<size_t>(c) * kSkipBlockSize;
      const auto width = _width[c];
      const auto base = _base[c];
      if (width == 0) {
        std::fill_n(out, kSkipBlockSize, base);
      } else {
        const auto* in = reinterpret_cast<const uint32_t*>(payload);
        packed::Unpack(out, out + kSkipBlockSize, in, width);
        for (uint32_t i = 0; i != kSkipBlockSize; ++i) {
          out[i] += base;
        }
        payload += SkipColumnBytes(width);
      }
    }
    _group = group;
  }

  static constexpr uint32_t kKeyColumn = 0;

  ManagedVector<uint32_t> _dec;
  uint32_t _base[kMaxSkipColumns];
  uint8_t _width[kMaxSkipColumns];
  const byte_type* _dir = nullptr;
  uint64_t _count = 0;
  uint64_t _origin = 0;
  uint64_t _group = kNoGroup;
  InputType* _data = nullptr;
  uint32_t _columns = 0;
  uint32_t _stride = 0;
  uint32_t _scratch[kMaxSkipColumns * kSkipBlockSize];
};

// Smallest `k` in [first, last) with `Get(column, k) >= target`, or `last` if
// there is none. Galloping from `first` because seeks only move forward: a
// short jump costs a few probes, a long one degrades to a bisect.
template<typename Columns>
IRS_FORCE_INLINE uint64_t SkipColumnSeek(Columns& columns, uint64_t first,
                                         uint64_t last, uint32_t target) {
  SDB_ASSERT(first <= last);
  uint64_t lo = first;
  uint64_t hi = first;
  for (uint64_t step = 1; hi < last && columns.GetKey(hi) < target;
       step *= 2) {
    lo = hi + 1;
    hi += step;
  }
  if (hi > last) {
    hi = last;
  }
  while (lo < hi) {
    const auto mid = lo + (hi - lo) / 2;
    if (columns.GetKey(mid) < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

}  // namespace irs
