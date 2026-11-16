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
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs {

template<typename T>
class FrameReader {
 public:
  using Value = T;
  using U = numeric::Bits<sizeof(T)>;

  uint32_t FrameOf(uint64_t row) const noexcept {
    if (_per_frame != 0) {
      return static_cast<uint32_t>(row / _per_frame);
    }
    uint32_t lo = 0;
    uint32_t hi = _f.frame_count;
    while (hi - lo > 1) {
      const auto mid = lo + (hi - lo) / 2;
      if (FirstRow(mid) <= row) {
        lo = mid;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  T FrameMin(uint32_t f) const noexcept {
    return Scaled(static_cast<U>(MetaAt(f).min));
  }

  T FrameMax(uint32_t f) const noexcept {
    return Scaled(static_cast<U>(MetaAt(f).max));
  }

  void Read(this auto& self, uint64_t row, uint64_t count, T* out) {
    uint64_t done = 0;
    while (done < count) {
      if constexpr (std::is_integral_v<T>) {
        if (const auto n = self.DecodeDirect(
              row + done, count - done, reinterpret_cast<U*>(out + done))) {
          self.Rescale(out + done, n);
          done += n;
          continue;
        }
      }
      self.Seek(row + done);
      const auto take =
        std::min<uint64_t>(count - done, self._end - (row + done));
      self.Copy(row + done, take, out + done);
      self.Rescale(out + done, take);
      done += take;
    }
  }

  T Get(this auto& self, uint64_t row) {
    self.Seek(row);
    return self.At(row);
  }

  IRS_FORCE_INLINE T Fetch(this auto& self, uint64_t row, uint64_t next) {
    if (self.Holds(row)) {
      return self.At(row);
    }
    return self.FetchApart(row, next);
  }

  T At(uint64_t row) const noexcept {
    return Scaled(duckdb::Load<U>(_view + (row - _begin) * sizeof(T)));
  }

  void Copy(uint64_t row, uint64_t count, T* out) const noexcept {
    std::memcpy(out, _view + (row - _begin) * sizeof(T), count * sizeof(T));
  }

 protected:
  static constexpr uint32_t kNoFrame = std::numeric_limits<uint32_t>::max();
  static constexpr uint64_t kNearShare = 32;
  static constexpr uint64_t kScanCacheDivisor = 2;

  struct PackedBlock {
    const uint8_t* packed = nullptr;
    uint64_t base = 0;
    unsigned bits = 0;
  };

  using FforFrame =
    std::array<PackedBlock, kFforFrameRows / numeric::kBlockValues>;

  struct Item {
    const uint8_t* source = nullptr;
    uint32_t at = 0;
    uint32_t rows = 0;
  };

  FrameReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              const ReadContext::CacheSlot& slot)
    : _base{base},
      _f{frames},
      _cache{slot.touched ? slot.cache : nullptr},
      _slot{slot} {
    SDB_ENSURE(_f.width == sizeof(T), "numeric codec: width mismatch");
    if (Packed()) {
      _per_frame = _f.shape == NumericShape::Runs ? 0 : kFforFrameRows;
    } else if (_f.shape != NumericShape::Runs) {
      _per_frame = _f.FrameBytes() / _f.stored;
    }
    for (uint32_t f = 0; f < _f.frame_count; ++f) {
      const auto first = FirstRow(f);
      SDB_ENSURE((f == 0 ? first == 0 : first > FirstRow(f - 1)) &&
                   (_per_frame == 0 || first == uint64_t{f} * _per_frame) &&
                   first < _f.row_count,
                 "numeric codec: corrupted frame table");
    }
    SDB_ENSURE(
      _per_frame == 0 || uint64_t{_f.frame_count} * _per_frame >= _f.row_count,
      "numeric codec: corrupted frame table");
  }

  bool Packed() const noexcept { return _f.flags & kNumericPacked; }

  bool Shuffled() const noexcept { return _f.flags & kNumericShuffled; }

  bool Holds(uint64_t row) const noexcept {
    return row >= _begin && row < _end;
  }

  bool Near(uint64_t row, uint64_t other) const noexcept {
    const uint64_t unit = Packed() ? numeric::kBlockValues : _per_frame;
    return (row > other ? row - other : other - row) <= unit / kNearShare &&
           row / unit == other / unit;
  }

  T Scaled(U v) const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (const auto scale = static_cast<U>(_f.scale); scale > 1) {
        numeric::MultiplyBy(&v, 1, scale);
      }
    }
    return std::bit_cast<T>(v);
  }

  void Rescale(T* out, uint64_t count) const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (const auto scale = static_cast<U>(_f.scale); scale > 1) {
        numeric::MultiplyBy(reinterpret_cast<U*>(out), count, scale);
      }
    }
  }

  uint64_t FirstRow(uint32_t f) const noexcept {
    return duckdb::Load<uint32_t>(_base + _f.off_frames +
                                  f * kNumericFrameMetaSize);
  }

  uint64_t EndRow(uint32_t f) const noexcept {
    return f + 1 < _f.frame_count ? FirstRow(f + 1) : _f.row_count;
  }

  NumericFrameMeta MetaAt(uint32_t f) const noexcept {
    SDB_ASSERT(f < _f.frame_count);
    return duckdb::Load<NumericFrameMeta>(_base + _f.off_frames +
                                          f * kNumericFrameMetaSize);
  }

  NumericFrameMeta Meta(uint32_t f) const {
    SDB_ENSURE(f < _f.frame_count, "numeric codec: frame out of range");
    const auto m = MetaAt(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    SDB_ENSURE(
      m.frame.raw_len <= _f.FrameBytes() &&
        uint64_t{m.frame.comp_off} + m.frame.comp_len <= _f.data_size &&
        (_f.leaf != NumericLeaf::None || m.frame.comp_len == m.frame.raw_len) &&
        (_f.shape == NumericShape::Runs || Packed() ||
         m.frame.raw_len == rows * _f.stored),
      "numeric codec: corrupted frame table");
    return m;
  }

  const uint8_t* Data(const NumericFrameMeta& m) const noexcept {
    return _base + _f.off_data + m.frame.comp_off;
  }

  U* Rows(size_t rows) {
    if (_rows.size() < rows) {
      _rows.resize(rows);
    }
    return _rows.data();
  }

  void Window(const uint8_t* view, uint64_t begin, uint64_t end) noexcept {
    _view = view;
    _begin = begin;
    _end = end;
  }

  bool Admitted() noexcept {
    if (_admit < 0) {
      _admit = _slot.touched && SecondTouch(_slot);
    }
    return _admit == 1;
  }

  const uint8_t* Inflated(const NumericFrameMeta& m, uint8_t* dst,
                          uint32_t frame,
                          duckdb::shared_ptr<DecodedFrame>& held) {
    if (m.frame.comp_len == m.frame.raw_len) {
      return Data(m);
    }
    ScratchVector<uint8_t> admitted;
    if (_cache && Admitted()) {
      if (_segment_key.empty()) {
        _segment_key = DictionaryCacheKey(_slot.file_id, _slot.file_offset);
      }
      FrameCacheKey(_segment_key, frame, _key);
      if (auto hit = _cache->Get<DecodedFrame>(_key);
          hit && hit->Bytes().size() == m.frame.raw_len) {
        held = std::move(hit);
        return held->Bytes().data();
      }
      const auto room = _scan ? _cache->GetMaxMemoryRacy() / kScanCacheDivisor
                              : _cache->GetMaxMemoryRacy();
      if (_cache->GetCurrentMemoryRacy() + m.frame.raw_len <= room) {
        admitted.resize(m.frame.raw_len);
        dst = admitted.data();
      }
    }
    if (!dst) {
      if (_raw.size() < m.frame.raw_len) {
        _raw.resize(m.frame.raw_len);
      }
      dst = _raw.data();
      _source_frame = kNoFrame;
    }
    const auto* in = reinterpret_cast<const char*>(Data(m));
    auto* out = reinterpret_cast<char*>(dst);
    const bool ok =
      _f.leaf == NumericLeaf::Lz4
        ? Lz4::Decompressor::Decompress(in, m.frame.comp_len, out,
                                        m.frame.raw_len)
        : _f.leaf == NumericLeaf::Zstd &&
            (_zstd ? *_zstd : _zstd.emplace())
              .Decompress(in, m.frame.comp_len, out, m.frame.raw_len);
    SDB_ENSURE(ok, "numeric codec: corrupted frame");
    if (!admitted.empty()) {
      held = duckdb::make_shared_ptr<DecodedFrame>(std::move(admitted));
      _cache->Put(_key, held);
    }
    return dst;
  }

  void OpenSource(uint32_t f, const NumericFrameMeta& m, size_t need,
                  bool scan) {
    const bool prefix = _f.leaf == NumericLeaf::Lz4 && f != _source_frame &&
                        need < m.frame.raw_len &&
                        m.frame.comp_len != m.frame.raw_len && !Admitted();
    _source_len = prefix ? need : m.frame.raw_len;
    _source_held.reset();
    _scan = scan;
    ++_source_opens;
    if (prefix) {
      if (_raw.size() < m.frame.raw_len) {
        _raw.resize(m.frame.raw_len);
      }
      SDB_ENSURE(
        Lz4::Decompressor::DecompressPrefix(
          reinterpret_cast<const char*>(Data(m)), m.frame.comp_len,
          reinterpret_cast<char*>(_raw.data()), need, m.frame.raw_len) != 0,
        "numeric codec: corrupted frame");
      _source = _raw.data();
    } else {
      _source = Inflated(m, nullptr, f, _source_held);
    }
    _source_frame = f;
  }

  const uint8_t* Source(uint32_t f, const NumericFrameMeta& m, uint64_t row) {
    const uint8_t* src = Data(m);
    if (m.frame.comp_len != m.frame.raw_len) {
      if (f != _source_frame || _source_len != m.frame.raw_len) {
        OpenSource(f, m, m.frame.raw_len, true);
      }
      src = _source;
    }
    return src + (row - m.frame.first_entry) * _f.stored;
  }

  Item SourceItem(uint64_t row) {
    const auto f = FrameOf(row);
    const auto at = static_cast<uint32_t>(row - uint64_t{f} * _per_frame);
    const size_t need = (size_t{at} + 1) * _f.stored;
    if (f != _source_frame || need > _source_len) {
      OpenSource(f, Meta(f), need, false);
    }
    return {.source = _source, .at = at};
  }

  Item SourceLanes(uint64_t row) {
    _fetched = row;
    const auto f = FrameOf(row);
    const auto first = uint64_t{f} * _per_frame;
    const auto rows = static_cast<uint32_t>(EndRow(f) - first);
    const size_t need = size_t{rows} * _f.stored;
    if (f != _source_frame || need > _source_len) {
      OpenSource(f, Meta(f), need, false);
    }
    return {
      .source = _source,
      .at = static_cast<uint32_t>(row - first),
      .rows = rows,
    };
  }

  void DecodeBlock(this auto& self, uint64_t row) {
    const auto first = row - row % numeric::kBlockValues;
    auto* rows = self.Rows(numeric::kBlockValues);
    self.UnpackBlock(first, numeric::kBlockValues, rows);
    self.Window(
      reinterpret_cast<const uint8_t*>(rows), first,
      std::min<uint64_t>(first + numeric::kBlockValues, self._f.row_count));
  }

  uint64_t DecodeBlockDirect(this auto& self, uint64_t row, uint64_t count,
                             U* out) {
    const auto rows =
      std::min<uint64_t>(numeric::kBlockValues, self._f.row_count - row);
    if (row % numeric::kBlockValues != 0 || rows > count) {
      return 0;
    }
    self.UnpackBlock(row, rows, out);
    return rows;
  }

  const uint8_t* Unshuffled(const uint8_t* src, uint32_t rows) {
    if (_shuffled.size() < size_t{rows} * _f.stored) {
      _shuffled.resize(size_t{rows} * _f.stored);
    }
    numeric::Unshuffle(src, rows, _f.stored, _shuffled.data());
    return _shuffled.data();
  }

  IRS_FORCE_INLINE PackedBlock BlockAt(uint64_t row) {
    if (const auto f = static_cast<uint32_t>(row / kFforFrameRows);
        f != _ffor_frame) {
      OpenFfor(f);
    }
    return (*_ffor)[row % kFforFrameRows / numeric::kBlockValues];
  }

  template<typename W>
  static const W* Words(const uint8_t* packed) noexcept {
    SDB_ASSERT(reinterpret_cast<uintptr_t>(packed) % alignof(W) == 0);
    return reinterpret_cast<const W*>(packed);
  }

  template<typename V>
  size_t UnpackStream(const uint8_t* src, size_t len, size_t off, size_t blocks,
                      V* out) {
    const auto* metas = src + off;
    off += blocks * kFforBlockMetaBytes;
    SDB_ENSURE(off <= len, "numeric codec: corrupted blocks");
    for (size_t b = 0; b < blocks; ++b) {
      const auto* meta = metas + b * kFforBlockMetaBytes;
      const unsigned bits = meta[sizeof(uint64_t)];
      SDB_ENSURE(
        bits <= numeric::kMaxBits<V> && off + numeric::PackedBytes(bits) <= len,
        "numeric codec: corrupted blocks");
      numeric::kUnpack<V>[bits](Words<numeric::LaneWord<V>>(src + off),
                                out + b * numeric::kBlockValues,
                                static_cast<V>(duckdb::Load<uint64_t>(meta)));
      off += numeric::PackedBytes(bits);
    }
    return off;
  }

  duckdb::const_data_ptr_t _base;
  FrameHeader _f;
  uint32_t _per_frame = 0;
  unsigned _max_bits = numeric::kMaxBits<U>;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  uint64_t _next = 0;
  bool _scan = false;
  const uint8_t* _view = nullptr;
  duckdb::shared_ptr<DecodedFrame> _held;
  duckdb::shared_ptr<DecodedFrame> _view_held;
  uint32_t _source_frame = kNoFrame;
  size_t _source_len = 0;
  const uint8_t* _source = nullptr;
  uint32_t _source_opens = 0;
  uint64_t _fetched = std::numeric_limits<uint64_t>::max();
  ScratchVector<U> _block;

 private:
  void OpenFfor(uint32_t f) {
    SDB_ENSURE(f < _f.frame_count, "numeric codec: frame out of range");
    if (_ffor_frames.empty()) {
      _ffor_frames.resize(_f.frame_count);
    }
    auto& frame = _ffor_frames[f];
    if (!frame.front().packed) {
      IndexFfor(f, frame);
    }
    _ffor = &frame;
    _ffor_frame = f;
  }

  IRS_NO_INLINE void IndexFfor(uint32_t f, FforFrame& frame) {
    const auto m = Meta(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    const auto blocks =
      (rows + numeric::kBlockValues - 1) / numeric::kBlockValues;
    const auto* src = Data(m);
    size_t off = blocks * kFforBlockMetaBytes;
    SDB_ENSURE(m.frame.comp_len == m.frame.raw_len && off <= m.frame.raw_len,
               "numeric codec: corrupted blocks");
    for (size_t b = 0; b < blocks; ++b) {
      const auto* meta = src + b * kFforBlockMetaBytes;
      const unsigned bits = meta[sizeof(uint64_t)];
      SDB_ENSURE(bits <= _max_bits, "numeric codec: corrupted blocks");
      frame[b] = {
        .packed = src + off,
        .base = duckdb::Load<uint64_t>(meta),
        .bits = bits,
      };
      off += numeric::PackedBytes(bits);
    }
    SDB_ENSURE(off == m.frame.raw_len, "numeric codec: corrupted blocks");
  }

  duckdb::ObjectCache* _cache;
  ReadContext::CacheSlot _slot;
  std::string _segment_key;
  std::string _key;
  int8_t _admit = -1;
  duckdb::shared_ptr<DecodedFrame> _source_held;
  ScratchVector<U> _rows;
  ScratchVector<uint8_t> _raw;
  ScratchVector<uint8_t> _shuffled;
  const FforFrame* _ffor = nullptr;
  uint32_t _ffor_frame = kNoFrame;
  std::vector<FforFrame> _ffor_frames;
  std::optional<Zstd::Decompressor> _zstd;
};

}  // namespace irs::codecs
