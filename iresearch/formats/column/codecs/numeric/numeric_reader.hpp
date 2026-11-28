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

#include <absl/numeric/int128.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <limits>
#include <optional>
#include <string>
#include <type_traits>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs::numeric {

template<typename T>
class FrameReader {
 public:
  using Value = T;
  using U = Bits<sizeof(T)>;

  struct Matches {};

  static constexpr bool kHasRuns = false;
  static constexpr bool kFiltersRuns = false;
  static constexpr bool kFiltersEntries = false;
  static constexpr bool kPacks = true;

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
  static constexpr uint64_t kNearShare = 32;
  static constexpr uint64_t kScanCacheDivisor = 2;

  struct PackedBlock {
    const uint8_t* packed = nullptr;
    uint64_t base = 0;
    uint64_t step = 0;
    unsigned bits = 0;
    bool delta = false;
  };

  using BitpackingFrame =
    std::array<PackedBlock, kBitpackingFrameRows / kBlockValues>;

  struct Item {
    const uint8_t* source = nullptr;
    uint32_t at = 0;
    uint32_t rows = 0;
  };

  FrameReader(duckdb::const_data_ptr_t base, const FrameHeader& frames,
              CodecType codec, TrackedAllocator<> alloc,
              const ReadContext::CacheSlot& slot)
    : _base{base},
      _f{frames},
      _shape{ShapeOf(codec)},
      _leaf{LeafOf(codec)},
      _meta_size{static_cast<uint32_t>(FrameMetaSize(
        ShapeOf(codec), frames.flags & kNumericPacked, sizeof(T)))},
      _block{alloc},
      _cache{slot.touched ? slot.cache : nullptr},
      _slot{slot},
      _rows{alloc},
      _raw{alloc},
      _shuffled{alloc},
      _words{alloc} {
    if (_f.width != sizeof(T)) [[unlikely]] {
      ThrowCorrupted("numeric", "value width does not match the column");
    }
    if (Packed()) {
      _per_frame = _shape == Shape::Runs ? 0 : kBitpackingFrameRows;
    } else if (_shape != Shape::Runs) {
      _per_frame = _f.FrameBytes() / _f.stored;
    }
    for (uint32_t f = 0; f < _f.frame_count; ++f) {
      const auto first = FirstRow(f);
      if (!((f == 0 ? first == 0 : first > FirstRow(f - 1)) &&
            (_per_frame == 0 || first == uint64_t{f} * _per_frame) &&
            first < _f.row_count)) [[unlikely]] {
        ThrowCorrupted("numeric", "frame table is inconsistent");
      }
    }
    if (!(_per_frame == 0 ||
          uint64_t{_f.frame_count} * _per_frame >= _f.row_count)) [[unlikely]] {
      ThrowCorrupted("numeric", "frame table is inconsistent");
    }
  }

  bool Packed() const noexcept { return _f.flags & kNumericPacked; }

  bool Shuffled() const noexcept { return _f.flags & kNumericShuffled; }

  bool Holds(uint64_t row) const noexcept {
    return row >= _begin && row < _end;
  }

  bool Near(uint64_t row, uint64_t other) const noexcept {
    const uint64_t unit = Packed() ? kBlockValues : _per_frame;
    return (row > other ? row - other : other - row) <= unit / kNearShare &&
           row / unit == other / unit;
  }

  T Scaled(U v) const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (const auto scale = static_cast<U>(_f.scale); scale > 1) [[unlikely]] {
        MultiplyBy(&v, 1, scale);
      }
    }
    return std::bit_cast<T>(v);
  }

  void Rescale(T* out, uint64_t count) const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (const auto scale = static_cast<U>(_f.scale); scale > 1) {
        MultiplyBy(reinterpret_cast<U*>(out), count, scale);
      }
    }
  }

  uint64_t FirstRow(uint32_t f) const noexcept {
    const auto* at = _base + _f.off_frames + size_t{f} * _meta_size;
    if (_meta_size == kNumericFrameMetaSize) {
      return duckdb::Load<uint32_t>(at);
    }
    return _shape == Shape::Runs ? duckdb::Load<uint32_t>(at + sizeof(uint32_t))
                                 : uint64_t{f} * kBitpackingFrameRows;
  }

  uint64_t EndRow(uint32_t f) const noexcept {
    return f + 1 < _f.frame_count ? FirstRow(f + 1) : _f.row_count;
  }

  NumericFrameMeta MetaAt(uint32_t f) const noexcept {
    SDB_ASSERT(f < _f.frame_count);
    const auto* at = _base + _f.off_frames + size_t{f} * _meta_size;
    if (_meta_size == kNumericFrameMetaSize) {
      return duckdb::Load<NumericFrameMeta>(at);
    }
    NumericFrameMeta m;
    m.frame.comp_off = duckdb::Load<uint32_t>(at);
    m.frame.raw_len =
      (f + 1 < _f.frame_count ? duckdb::Load<uint32_t>(at + _meta_size)
                              : _f.data_size) -
      m.frame.comp_off;
    m.frame.comp_len = m.frame.raw_len;
    const auto* own = at + sizeof(uint32_t);
    if (_shape == Shape::Runs) {
      m.frame.first_entry = duckdb::Load<uint32_t>(own);
      m.base = duckdb::Load<uint16_t>(own + sizeof(uint32_t));
      own += sizeof(uint32_t) + sizeof(uint16_t);
    } else {
      m.frame.first_entry = f * kBitpackingFrameRows;
      m.base = duckdb::Load<U>(own);
      own += sizeof(U);
    }
    m.min = duckdb::Load<U>(own);
    m.max = duckdb::Load<U>(own + sizeof(U));
    return m;
  }

  NumericFrameMeta Meta(uint32_t f) const {
    SDB_ASSERT(f < _f.frame_count);
    const auto m = MetaAt(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    if (!(m.frame.raw_len <= _f.FrameBytes() &&
          uint64_t{m.frame.comp_off} + m.frame.comp_len <= _f.data_size &&
          (_leaf != ByteCodec::None || m.frame.comp_len == m.frame.raw_len) &&
          (_shape == Shape::Runs || Packed() ||
           m.frame.raw_len == rows * _f.stored))) [[unlikely]] {
      ThrowCorrupted("numeric", "frame table is inconsistent");
    }
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
    SDB_ASSERT(_leaf == ByteCodec::Lz4 || _leaf == ByteCodec::Zstd);
    const bool ok =
      _leaf == ByteCodec::Lz4
        ? Lz4::Decompressor::Decompress(in, m.frame.comp_len, out,
                                        m.frame.raw_len)
        : (_zstd ? *_zstd : _zstd.emplace(_rows.get_allocator()))
            .Decompress(in, m.frame.comp_len, out, m.frame.raw_len);
    if (!ok) [[unlikely]] {
      ThrowCorrupted("numeric", "frame is inconsistent");
    }
    if (!admitted.empty()) {
      held = duckdb::make_shared_ptr<DecodedFrame>(std::move(admitted));
      _cache->Put(_key, held);
    }
    return dst;
  }

  void OpenSource(uint32_t f, const NumericFrameMeta& m, size_t need,
                  bool scan) {
    const bool prefix = _leaf == ByteCodec::Lz4 && f != _source_frame &&
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
      if (Lz4::Decompressor::DecompressPrefix(
            reinterpret_cast<const char*>(Data(m)), m.frame.comp_len,
            reinterpret_cast<char*>(_raw.data()), need, m.frame.raw_len) == 0)
        [[unlikely]] {
        ThrowCorrupted("numeric", "frame is inconsistent");
      }
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
    if (f != _source_frame) {
      OpenSource(f, Meta(f), need, false);
    }
    SDB_ASSERT(need <= _source_len);
    return {
      .source = _source,
      .at = static_cast<uint32_t>(row - first),
      .rows = rows,
    };
  }

  void DecodeBlock(this auto& self, uint64_t row) {
    const auto first = row - row % kBlockValues;
    auto* rows = self.Rows(kBlockValues);
    self.UnpackBlock(
      first, std::min<uint64_t>(kBlockValues, self._f.row_count - first), rows);
    self.Window(reinterpret_cast<const uint8_t*>(rows), first,
                std::min<uint64_t>(first + kBlockValues, self._f.row_count));
  }

  uint64_t DecodeBlockDirect(this auto& self, uint64_t row, uint64_t count,
                             U* out) {
    const auto rows = std::min<uint64_t>(kBlockValues, self._f.row_count - row);
    if (row % kBlockValues != 0 || rows > count) {
      return 0;
    }
    self.UnpackBlock(row, rows, out);
    return rows;
  }

  const uint8_t* Unshuffled(const uint8_t* src, uint32_t rows) {
    if (_shuffled.size() < size_t{rows} * _f.stored) {
      _shuffled.resize(size_t{rows} * _f.stored);
    }
    Unshuffle(src, rows, _f.stored, _shuffled.data());
    return _shuffled.data();
  }

  template<typename V = U>
  IRS_FORCE_INLINE PackedBlock BlockAt(uint64_t row) {
    if (const auto f = static_cast<uint32_t>(row / kBitpackingFrameRows);
        f != _bitpacking_frame) {
      OpenBitpacking<V>(f);
    }
    return _bitpacking[row % kBitpackingFrameRows / kBlockValues];
  }

  static bool NearInBlock(uint64_t row, uint64_t next) noexcept {
    return next - row <= kBlockValues / kNearShare &&
           (row ^ next) < kBlockValues;
  }

  IRS_FORCE_INLINE void FetchRows(this auto& self, const duckdb::idx_t* rows,
                                  duckdb::idx_t count, T* out,
                                  const duckdb::SelectionVector& sel,
                                  duckdb::idx_t offset) {
    if constexpr (std::remove_reference_t<decltype(self)>::kPacks) {
      if (self.Packed()) {
        self.FetchPackedRows(rows, count, out, sel, offset);
        return;
      }
    }
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const auto row = static_cast<uint64_t>(rows[i]);
      SDB_ASSERT(row < self._f.row_count);
      out[offset + sel.get_index(i)] =
        self.Holds(row)
          ? self.At(row)
          : self.FetchFramed(row,
                             i + 1 < count ? uint64_t{rows[i + 1]} : kNoRow);
    }
  }

  IRS_FORCE_INLINE void FetchPackedRows(this auto& self,
                                        const duckdb::idx_t* rows,
                                        duckdb::idx_t count, T* out,
                                        const duckdb::SelectionVector& sel,
                                        duckdb::idx_t offset) {
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const auto row = static_cast<uint64_t>(rows[i]);
      SDB_ASSERT(row < self._f.row_count);
      T value;
      if (self.Holds(row)) {
        value = self.At(row);
      } else if (const auto block = self.BlockAt(row); block.bits == 0) {
        const uint64_t first = row - row % kBlockValues;
        const auto base = static_cast<U>(block.base);
        const auto step = static_cast<U>(block.step);
        for (;; ++i) {
          SDB_ASSERT(rows[i] < self._f.row_count);
          out[offset + sel.get_index(i)] = self.Scaled(
            static_cast<U>(base + static_cast<U>(rows[i] - first) * step));
          if (i + 1 == count || rows[i + 1] - first >= kBlockValues) {
            break;
          }
        }
        continue;
      } else if (block.delta) {
        i += self.FetchSummed(rows, i, count, out, sel, offset) - 1;
        continue;
      } else if (i + 1 != count && NearInBlock(row, rows[i + 1])) {
        value = self.Get(row);
      } else {
        value = self.Scaled(UnpackValue<U>(
          block.packed, block.bits, static_cast<uint32_t>(row % kBlockValues),
          static_cast<U>(block.base)));
      }
      out[offset + sel.get_index(i)] = value;
    }
  }

  IRS_FORCE_INLINE bool ConstantRows(this auto& self, uint64_t row,
                                     uint64_t count, T& value) {
    if constexpr (!std::remove_reference_t<decltype(self)>::kPacks) {
      return false;
    } else {
      if (!self.Packed()) {
        return false;
      }
      const auto block = self.BlockAt(row);
      if (block.bits != 0 || block.step != 0) {
        return false;
      }
      const auto base = static_cast<U>(block.base);
      const uint64_t next = row + kBlockValues - row % kBlockValues;
      if (next < row + count && !self.ConstantBlocks(next, row + count, base)) {
        return false;
      }
      value = self.Scaled(base);
      return true;
    }
  }

  IRS_NO_INLINE bool ConstantBlocks(this auto& self, uint64_t row, uint64_t end,
                                    U base) {
    for (uint64_t at = row; at < end; at += kBlockValues) {
      const auto block = self.BlockAt(at);
      if (block.bits != 0 || block.step != 0 ||
          static_cast<U>(block.base) != base) {
        return false;
      }
    }
    return true;
  }

  IRS_FORCE_INLINE bool NarrowBounds(this auto& self, uint64_t row,
                                     uint64_t count, T& min, T& max) {
    if constexpr (!std::remove_reference_t<decltype(self)>::kPacks) {
      return false;
    } else {
      if (!self.Packed()) {
        return false;
      }
      if (const auto f = static_cast<uint32_t>(row / kBitpackingFrameRows);
          f != self._bounds_frame) {
        const auto m = self.MetaAt(f);
        self._bounds_frame = f;
        self._bounds_limit =
          static_cast<U>(static_cast<U>(m.max) - static_cast<U>(m.min)) / 2;
      }
      const auto block = self.BlockAt(row);
      const U mask = block.bits >= kMaxBits<U>
                       ? std::numeric_limits<U>::max()
                       : static_cast<U>((uint64_t{1} << block.bits) - 1);
      if (mask > (block.delta ? self._bounds_limit / (kBlockValues - 1)
                              : self._bounds_limit)) {
        return false;
      }
      return self.BlockBounds(row, count, min, max);
    }
  }

  IRS_NO_INLINE bool BlockBounds(this auto& self, uint64_t row, uint64_t count,
                                 T& min, T& max) {
    using S = std::make_signed_t<U>;
    constexpr absl::int128 kLeast{std::numeric_limits<T>::min()};
    constexpr absl::int128 kMost{std::numeric_limits<T>::max()};
    const auto wide = [](uint64_t bits) {
      return absl::int128{std::bit_cast<T>(static_cast<U>(bits))};
    };
    const absl::int128 limit{self._bounds_limit};
    absl::int128 lo = kMost;
    absl::int128 hi = kLeast;
    for (uint64_t at = row, end = row + count; at < end;) {
      const auto block = self.BlockAt(at);
      const uint64_t from = at % kBlockValues;
      const uint64_t to =
        std::min<uint64_t>(kBlockValues, from + (end - at)) - 1;
      const auto base = wide(block.base);
      const auto mask = (absl::int128{1} << block.bits) - 1;
      auto first = base;
      auto last = std::min(base + mask, kMost);
      if (block.delta) {
        const absl::int128 least{static_cast<S>(static_cast<U>(block.step))};
        const auto most = least + mask;
        first = base + std::min(least * from, least * to);
        last = base + std::max(most * from, most * to);
        if (first < kLeast || last > kMost) {
          return false;
        }
      }
      lo = std::min(lo, first);
      hi = std::max(hi, last);
      if (hi - lo > limit) {
        return false;
      }
      at += to + 1 - from;
    }
    const absl::int128 scale{std::max<uint64_t>(self._f.scale, 1)};
    min = static_cast<T>(std::max(lo * scale, kLeast));
    max = static_cast<T>(std::min(hi * scale, kMost));
    return true;
  }

  IRS_NO_INLINE duckdb::idx_t FetchSummed(this auto& self,
                                          const duckdb::idx_t* rows,
                                          duckdb::idx_t i, duckdb::idx_t count,
                                          T* out,
                                          const duckdb::SelectionVector& sel,
                                          duckdb::idx_t offset) {
    const auto row = static_cast<uint64_t>(rows[i]);
    duckdb::idx_t end = i + 1;
    while (end < count && (rows[end] ^ row) < kBlockValues) {
      SDB_ASSERT(rows[end] < self._f.row_count);
      ++end;
    }
    const bool decoded =
      (end - i) * 2 >= kBlockValues || NearInBlock(self._fetched, row);
    self._fetched = rows[end - 1];
    if (decoded) {
      self.Seek(row);
      for (auto k = i; k < end; ++k) {
        out[offset + sel.get_index(k)] = self.At(rows[k]);
      }
      return end - i;
    }
    if (self._steps_block != row - row % kBlockValues) {
      self.LoadSteps(row);
    }
    for (auto k = i; k < end; ++k) {
      out[offset + sel.get_index(k)] = self.Scaled(self.SummedTo(rows[k]));
    }
    return end - i;
  }

  IRS_NO_INLINE void LoadSteps(uint64_t row) {
    const uint64_t first = row - row % kBlockValues;
    const auto block = BlockAt(row);
    _block.resize(kBlockValues);
    Unpack<U>(
      block.bits,
      BlockWords<U>(block.packed, block.bits,
                    std::min<uint64_t>(kBlockValues, _f.row_count - first)),
      _block.data(), U{0});
    _steps_block = first;
    _steps_summed = 0;
    _steps_sum = 0;
  }

  U SummedTo(uint64_t row) {
    SDB_ASSERT(_steps_block == row - row % kBlockValues);
    const auto block = BlockAt(row);
    const auto index = static_cast<size_t>(row % kBlockValues);
    if (index + 1 < _steps_summed) {
      _steps_summed = 0;
      _steps_sum = 0;
    }
    U sum = _steps_sum;
    const U* steps = _block.data();
    for (size_t j = _steps_summed; j <= index; ++j) {
      sum = static_cast<U>(sum + steps[j]);
    }
    _steps_sum = sum;
    _steps_summed = index + 1;
    return static_cast<U>(static_cast<U>(block.base) +
                          static_cast<U>(index) * static_cast<U>(block.step) +
                          sum);
  }

  void UnpackBlock(uint64_t first, uint64_t rows, U* out) {
    const auto block = BlockAt(first);
    const auto base = static_cast<U>(block.base);
    if (!block.delta) {
      const auto* words = BlockWords<U>(block.packed, block.bits, rows);
      if (rows == kBlockValues) {
        Unpack<U>(block.bits, words, out, base);
        return;
      }
      _steps_block = kNoRow;
      _block.resize(kBlockValues);
      Unpack<U>(block.bits, words, _block.data(), base);
      std::memcpy(out, _block.data(), rows * sizeof(U));
      return;
    }
    const auto step = static_cast<U>(block.step);
    if (block.bits == 0) {
      auto value = base;
      for (uint64_t i = 0; i < rows; ++i) {
        out[i] = value;
        value = static_cast<U>(value + step);
      }
      return;
    }
    _steps_block = kNoRow;
    _block.resize(kBlockValues);
    Unpack<U>(block.bits, BlockWords<U>(block.packed, block.bits, rows),
              _block.data(), U{0});
    WidenPrefixSumAs<U, U, true>(
      reinterpret_cast<const uint8_t*>(_block.data()), rows,
      static_cast<U>(base - step), step, out);
  }

  IRS_FORCE_INLINE T FetchPacked(this auto& self, uint64_t row, uint64_t next) {
    const auto block = self.BlockAt(row);
    const auto index = static_cast<uint32_t>(row % kBlockValues);
    const auto base = static_cast<U>(block.base);
    if (block.bits == 0) {
      return self.Scaled(static_cast<U>(base + static_cast<U>(index) *
                                                 static_cast<U>(block.step)));
    }
    if (block.delta) {
      const bool decoded = (next != kNoRow && NearInBlock(row, next)) ||
                           NearInBlock(self._fetched, row);
      self._fetched = row;
      if (decoded) {
        return self.Get(row);
      }
      if (self._steps_block != row - index) {
        self.LoadSteps(row);
      }
      return self.Scaled(self.SummedTo(row));
    }
    if (next != kNoRow && NearInBlock(row, next)) {
      return self.Get(row);
    }
    return self.Scaled(UnpackValue<U>(block.packed, block.bits, index, base));
  }

  template<typename W>
  static const W* Words(const uint8_t* packed) noexcept {
    SDB_ASSERT(reinterpret_cast<uintptr_t>(packed) % alignof(W) == 0);
    return reinterpret_cast<const W*>(packed);
  }

  template<typename V>
  const LaneWord<V>* BlockWords(const uint8_t* packed, unsigned bits,
                                size_t values) {
    if (values == kBlockValues) {
      return Words<LaneWord<V>>(packed);
    }
    if (bits == 0) {
      return nullptr;
    }
    const auto stored = PackedBytes<V>(bits, values);
    const auto whole = PackedBytes(bits);
    if (_words.size() * sizeof(uint64_t) < whole) {
      _words.resize(whole / sizeof(uint64_t));
    }
    auto* words = reinterpret_cast<uint8_t*>(_words.data());
    std::memcpy(words, packed, stored);
    std::memset(words + stored, 0, whole - stored);
    return reinterpret_cast<const LaneWord<V>*>(words);
  }

  template<typename V>
  size_t UnpackStream(const uint8_t* src, size_t len, size_t off, size_t values,
                      V* out) {
    const size_t blocks = (values + kBlockValues - 1) / kBlockValues;
    const auto* bases = src + off;
    const auto* widths = bases + blocks * sizeof(V);
    off += BlockMetaBytes(blocks, sizeof(V));
    if (off > len) [[unlikely]] {
      ThrowCorrupted("numeric", "blocks are inconsistent");
    }
    for (size_t b = 0; b < blocks; ++b) {
      const uint8_t kind = widths[b];
      const size_t count = std::min(kBlockValues, values - b * kBlockValues);
      auto* block = out + b * kBlockValues;
      const auto base = duckdb::Load<V>(bases + b * sizeof(V));
      const bool stepped = (kind & kBlockDelta) != 0;
      unsigned bits = kind;
      V from = base;
      if (stepped) {
        if (!(count > 1 && off + sizeof(uint64_t) <= len)) [[unlikely]] {
          ThrowCorrupted("numeric", "blocks are inconsistent");
        }
        from = static_cast<V>(duckdb::Load<uint64_t>(src + off));
        off += sizeof(uint64_t);
        if (kind & kBlockOwnStep) {
          if (kind != kStreamAlternates) [[unlikely]] {
            ThrowCorrupted("numeric", "blocks are inconsistent");
          }
          for (size_t i = 0; i < count; ++i) {
            block[i] = static_cast<V>(base + (i % 2 == 0 ? V{0} : from));
          }
          continue;
        }
        bits = kind & kBlockDeltaBits;
      }
      const auto stored =
        count == kBlockValues
          ? PackedBytes(bits)
          : duckdb::BitpackingPrimitives::GetRequiredSize(count, bits);
      if (!(bits <= kMaxBits<V> && off + stored <= len)) [[unlikely]] {
        ThrowCorrupted("numeric", "blocks are inconsistent");
      }
      if (count == kBlockValues) {
        Unpack<V>(bits, Words<LaneWord<V>>(src + off), block, from);
      } else if (bits == 0) {
        std::fill_n(block, count, from);
      } else {
        duckdb::BitpackingPrimitives::UnPackBuffer<V>(
          reinterpret_cast<duckdb::data_ptr_t>(block), src + off, count, bits,
          true);
        for (size_t i = 0; i < count; ++i) {
          block[i] = static_cast<V>(block[i] + from);
        }
      }
      if (stepped) {
        V value = base;
        block[0] = value;
        for (size_t i = 1; i < count; ++i) {
          value = static_cast<V>(value + block[i]);
          block[i] = value;
        }
      }
      off += stored;
    }
    return off;
  }

  duckdb::const_data_ptr_t _base;
  FrameHeader _f;
  Shape _shape;
  ByteCodec _leaf;
  uint32_t _meta_size;
  uint32_t _per_frame = 0;
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
  TrackedScratchVector<U> _block;
  uint64_t _steps_block = kNoRow;
  size_t _steps_summed = 0;
  U _steps_sum = 0;
  uint8_t _code_base_bytes = sizeof(uint32_t);

 private:
  template<typename V>
  IRS_NO_INLINE void OpenBitpacking(uint32_t f) {
    _bitpacking_frame = kNoFrame;
    auto& frame = _bitpacking;
    const auto m = Meta(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    const auto blocks = (rows + kBlockValues - 1) / kBlockValues;
    const auto* src = Data(m);
    const bool stepped = _shape == Shape::Plain || _shape == Shape::Delta;
    if (stepped && m.frame.raw_len == 0) {
      const auto step = static_cast<V>(m.base);
      auto start = static_cast<V>(
        static_cast<std::make_signed_t<V>>(step) < 0 ? m.max : m.min);
      for (size_t b = 0; b < blocks; ++b) {
        frame[b] = {
          .packed = src,
          .base = start,
          .step = step,
          .bits = 0,
          .delta = true,
        };
        start = static_cast<V>(start + uint64_t{kBlockValues} * step);
      }
      _bitpacking_frame = f;
      return;
    }
    const auto origin = stepped ? static_cast<V>(m.min) : V{0};
    const size_t base_bytes =
      stepped ? BytesFor(static_cast<V>(m.max - m.min)) : _code_base_bytes;
    size_t off = BlockMetaBytes(blocks, base_bytes);
    if (off > m.frame.raw_len) [[unlikely]] {
      ThrowCorrupted("numeric", "blocks are inconsistent");
    }
    const auto* widths = src + blocks * base_bytes;
    V bases[kBitpackingFrameRows / kBlockValues];
    WidenAdd<V>(src, blocks, static_cast<uint8_t>(base_bytes), origin, bases);
    for (size_t b = 0; b < blocks; ++b) {
      unsigned bits = widths[b];
      const bool delta = stepped && (bits & kBlockDelta) != 0;
      uint64_t step = 0;
      if (delta) {
        step = m.base;
        if ((bits & kBlockOwnStep) != 0) {
          if (off + sizeof(uint64_t) > m.frame.raw_len) [[unlikely]] {
            ThrowCorrupted("numeric", "blocks are inconsistent");
          }
          step = duckdb::Load<uint64_t>(src + off);
          off += sizeof(uint64_t);
        }
        bits &= kBlockDeltaBits;
      }
      if (bits > kMaxBits<V>) [[unlikely]] {
        ThrowCorrupted("numeric", "blocks are inconsistent");
      }
      frame[b] = {
        .packed = src + off,
        .base = bases[b],
        .step = step,
        .bits = bits,
        .delta = delta,
      };
      off += PackedBytes<V>(
        bits, std::min<uint64_t>(kBlockValues, rows - b * kBlockValues));
    }
    if (off != m.frame.raw_len) [[unlikely]] {
      ThrowCorrupted("numeric", "blocks are inconsistent");
    }
    _bitpacking_frame = f;
  }

  duckdb::ObjectCache* _cache;
  ReadContext::CacheSlot _slot;
  std::string _segment_key;
  std::string _key;
  int8_t _admit = -1;
  duckdb::shared_ptr<DecodedFrame> _source_held;
  TrackedScratchVector<U> _rows;
  TrackedScratchVector<uint8_t> _raw;
  TrackedScratchVector<uint8_t> _shuffled;
  TrackedScratchVector<uint64_t> _words;
  BitpackingFrame _bitpacking;
  uint32_t _bitpacking_frame = kNoFrame;
  uint32_t _bounds_frame = kNoFrame;
  U _bounds_limit = 0;
  std::optional<Zstd::Decompressor> _zstd;
};

}  // namespace irs::codecs::numeric
