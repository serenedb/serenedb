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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/store/unmap_behind.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs {

template<typename T>
class FrameDecoder {
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint32_t kBurst = 32 / sizeof(T);
  static constexpr uint32_t kLongBurst = 128 / sizeof(T);
  static constexpr uint32_t kNoFrame = std::numeric_limits<uint32_t>::max();

 public:
  FrameDecoder(duckdb::const_data_ptr_t base, const NumericHeader& header,
               duckdb::ObjectCache* cache = nullptr,
               const ReadContext::CacheSlot& slot = {})
    : _base{base},
      _h{header},
      _cache{slot.touched ? cache : nullptr},
      _slot{slot},
      _read_once{slot.read_once} {
    SDB_ENSURE(_h.width == sizeof(T), "numeric codec: width mismatch");
    if (Blocked()) {
      _per_frame = kFforFrameRows;
    } else if (!Runs(_h.transform)) {
      _per_frame = _h.FrameBytes() / _h.stored;
    }
    for (uint32_t f = 0; f < _h.frame_count; ++f) {
      const auto first = FirstRow(f);
      SDB_ENSURE((f == 0 ? first == 0 : first > FirstRow(f - 1)) &&
                   (_per_frame == 0 || first == uint64_t{f} * _per_frame) &&
                   first < _h.row_count,
                 "numeric codec: corrupted frame table");
    }
    SDB_ENSURE(
      _per_frame == 0 || uint64_t{_h.frame_count} * _per_frame >= _h.row_count,
      "numeric codec: corrupted frame table");
    if (_h.leaf == NumericLeaf::Lz4) {
      _lz4.emplace();
    }
    if constexpr (std::is_integral_v<T>) {
      _scale = static_cast<U>(_h.scale);
    }
  }

  uint32_t FrameOf(uint64_t row) const noexcept {
    if (_per_frame != 0) {
      return static_cast<uint32_t>(row / _per_frame);
    }
    uint32_t lo = 0;
    uint32_t hi = _h.frame_count;
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

  void Read(uint64_t row, uint64_t count, T* out) {
    if (_read_once && count != 0) {
      Trail(row);
    }
    uint64_t done = 0;
    while (done < count) {
      if constexpr (std::is_integral_v<T>) {
        if (const auto n = DecodeDirect(row + done, count - done,
                                        reinterpret_cast<U*>(out + done))) {
          Rescale(out + done, n);
          done += n;
          continue;
        }
      }
      Seek(row + done);
      const auto take = std::min<uint64_t>(count - done, _end - (row + done));
      Copy(row + done, take, out + done);
      Rescale(out + done, take);
      done += take;
    }
  }

  T Get(uint64_t row) {
    Seek(row);
    return At(row);
  }

  T Fetch(uint64_t row, uint64_t previous, uint64_t next) {
    if (Holds(row) || Runs(_h.transform) || Near(row, previous) ||
        Near(row, next)) {
      return Get(row);
    }
    if (ReadsAlone()) {
      Prefetch(next);
      return ReadAlone(row);
    }
    if (T value{}; ReadSource(row, value)) {
      return value;
    }
    return Get(row);
  }

 private:
  static constexpr uint64_t kNearShare = 32;
  static constexpr uint64_t kScanCacheDivisor = 2;

  void Trail(uint64_t row) noexcept {
    const uint64_t data = _slot.file_offset + _h.off_data;
    _behind.Advance(*_read_once, data + MetaAt(FrameOf(row)).frame.comp_off,
                    data + _h.data_size);
  }

  void Seek(uint64_t row) {
    if (row >= _begin && row < _end) {
      return;
    }
    if (Blocked()) {
      DecodeBlock(row);
    } else {
      _scan = row == _next;
      Decode(FrameOf(row));
    }
  }

  T At(uint64_t row) noexcept {
    U v;
    if (!Runs(_h.transform)) {
      std::memcpy(&v, _view + (row - _begin) * sizeof(T), sizeof(T));
      return Scaled(v);
    }
    const auto r = RunOf(static_cast<uint32_t>(row - _begin));
    _hint = r;
    return Scaled(_run_values[r]);
  }

  bool Holds(uint64_t row) const noexcept {
    return row >= _begin && row < _end;
  }

  bool ReadsAlone() const noexcept {
    return Blocked() || (_h.transform == NumericTransform::Dict &&
                         _h.leaf == NumericLeaf::None && !_h.Shuffled());
  }

  bool Near(uint64_t row, uint64_t other) const noexcept {
    const uint64_t unit = Blocked() ? numeric::kBlockValues : _per_frame;
    return row / unit == other / unit &&
           (row > other ? row - other : other - row) <= unit / kNearShare;
  }

  T ReadAlone(uint64_t row) {
    if (Blocked()) {
      if (const auto f = static_cast<uint32_t>(row / kFforFrameRows);
          f != _ffor_frame) {
        OpenFfor(f);
      }
      const auto in_frame = row % kFforFrameRows;
      const auto block = in_frame / numeric::kBlockValues;
      const auto* meta = _ffor_src + block * kFforBlockMetaBytes;
      const auto* packed = _ffor_src + _ffor_offsets[block];
      const unsigned bits = meta[sizeof(uint64_t)];
      const auto index =
        static_cast<uint32_t>(in_frame % numeric::kBlockValues);
      const auto base = duckdb::Load<uint64_t>(meta);
      if (_h.transform == NumericTransform::DictFfor) {
        return Entry(numeric::UnpackValue<uint32_t>(
          packed, bits, index, static_cast<uint32_t>(base)));
      }
      return Scaled(
        numeric::UnpackValue<U>(packed, bits, index, static_cast<U>(base)));
    }
    const auto m = Meta(FrameOf(row));
    uint32_t code;
    numeric::Widen(Data(m) + (row - m.frame.first_entry) * _h.stored, 1,
                   _h.stored, &code);
    return Entry(code);
  }

  bool ReadSource(uint64_t row, T& out) {
    if (_h.Shuffled() || (_h.transform == NumericTransform::Raw &&
                          _h.leaf == NumericLeaf::None)) {
      return false;
    }
    const auto f = FrameOf(row);
    const auto at = static_cast<uint32_t>(
      row - (f == _source_frame ? _source_first : FirstRow(f)));
    const size_t need = (size_t{at} + 1) * _h.stored;
    if (f != _source_frame || need > _source_len) {
      OpenSource(f, Meta(f), need, false);
    }
    if (_h.transform != NumericTransform::Delta) {
      out = Item(_source, at);
      return true;
    }
    if (at + 1 < _summed) {
      _summed = 0;
    }
    if (_summed == 0) {
      _sum = _source_base;
    }
    _sum = numeric::WidenPrefixSum<false>(
      _source + size_t{_summed} * _h.stored, size_t{at} + 1 - _summed,
      _h.stored, _sum, static_cast<U>(_h.base), static_cast<U*>(nullptr));
    _summed = at + 1;
    out = Scaled(_sum);
    return true;
  }

  void OpenSource(uint32_t f, const NumericFrameMeta& m, size_t need,
                  bool scan) {
    const bool prefix = _lz4 && f != _source_frame && need < m.frame.raw_len;
    _summed = 0;
    _source_first = m.frame.first_entry;
    _source_base = static_cast<U>(m.base);
    _source_len = m.frame.raw_len;
    _source_held.reset();
    if (m.frame.comp_len == m.frame.raw_len) {
      _source = Data(m);
    } else if (const auto* cached =
                 _cache && Admitted()
                   ? Lookup(FrameKey(f), m.frame.raw_len, _source_held)
                   : nullptr) {
      _source = cached;
    } else if (prefix && !Admitted()) {
      if (_raw.size() < m.frame.raw_len) {
        _raw.resize(m.frame.raw_len);
      }
      SDB_ENSURE(_lz4->DecompressPrefix(reinterpret_cast<const char*>(Data(m)),
                                        m.frame.comp_len,
                                        reinterpret_cast<char*>(_raw.data()),
                                        need, m.frame.raw_len) != 0,
                 "numeric codec: corrupted frame");
      _source = _raw.data();
      _source_len = need;
    } else {
      _scan = scan;
      _source = Inflate(m, nullptr, _source_held);
    }
    _source_frame = f;
  }

  T Item(const uint8_t* src, uint32_t at) const {
    const auto* item = src + size_t{at} * _h.stored;
    if (_h.transform == NumericTransform::Dict) {
      uint32_t code = 0;
      numeric::Widen(item, 1, _h.stored, &code);
      return Entry(code);
    }
    U v = 0;
    numeric::Widen(item, 1, _h.stored, &v);
    return Scaled(static_cast<U>(v + static_cast<U>(_h.base)));
  }

  void Prefetch(uint64_t row) const noexcept {
    if (Blocked() || row >= _h.row_count) {
      return;
    }
    const auto m = MetaAt(FrameOf(row));
    __builtin_prefetch(Data(m) + (row - m.frame.first_entry) * _h.stored);
  }

  void Copy(uint64_t row, uint64_t count, T* out) noexcept {
    if (!Runs(_h.transform)) {
      std::memcpy(out, _view + (row - _begin) * sizeof(T), count * sizeof(T));
      return;
    }
    const auto pos = static_cast<uint32_t>(row - _begin);
    if (_long_runs) {
      CopyRuns<kLongBurst>(pos, count, out);
    } else {
      CopyRuns<kBurst>(pos, count, out);
    }
  }

  template<uint32_t kSize>
  void CopyRuns(uint32_t pos, uint64_t count, T* out) noexcept {
    const auto end = pos + static_cast<uint32_t>(count);
    auto r = RunOf(pos);
    const uint64_t burst_limit = count >= kSize ? count - kSize + 1 : 0;
    uint64_t done = 0;
    while (pos < end) {
      const auto stop = std::min(_run_ends[r], end);
      const auto v = std::bit_cast<T>(_run_values[r]);
      const auto n = stop - pos;
      if (done < burst_limit) {
        for (uint32_t i = 0; i < kSize; ++i) {
          out[done + i] = v;
        }
        if (n > kSize) {
          std::fill_n(out + done + kSize, n - kSize, v);
        }
      } else {
        std::fill_n(out + done, n, v);
      }
      done += n;
      pos = stop;
      ++r;
    }
    _hint = r - 1;
  }

  T Entry(uint32_t code) const {
    SDB_ENSURE(code < _h.dict_count, "numeric codec: corrupted codes");
    return Scaled(
      duckdb::Load<U>(_base + _h.off_dict + size_t{code} * sizeof(U)));
  }

  bool Blocked() const noexcept {
    return _h.transform == NumericTransform::Ffor ||
           _h.transform == NumericTransform::DictFfor;
  }

  T Scaled(U v) const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (_scale > 1) {
        numeric::MultiplyBy(&v, 1, _scale);
      }
    }
    return std::bit_cast<T>(v);
  }

  void Rescale(T* out, uint64_t count) const noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (_scale > 1) {
        numeric::MultiplyBy(reinterpret_cast<U*>(out), count, _scale);
      }
    }
  }

  uint64_t FirstRow(uint32_t f) const noexcept {
    return duckdb::Load<uint32_t>(_base + _h.off_frames +
                                  f * kNumericFrameMetaSize);
  }

  uint64_t EndRow(uint32_t f) const noexcept {
    return f + 1 < _h.frame_count ? FirstRow(f + 1) : _h.row_count;
  }

  NumericFrameMeta MetaAt(uint32_t f) const noexcept {
    SDB_ASSERT(f < _h.frame_count);
    return duckdb::Load<NumericFrameMeta>(_base + _h.off_frames +
                                          f * kNumericFrameMetaSize);
  }

  NumericFrameMeta Meta(uint32_t f) const {
    SDB_ENSURE(f < _h.frame_count, "numeric codec: frame out of range");
    const auto m = MetaAt(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    SDB_ENSURE(
      m.frame.raw_len <= _h.FrameBytes() &&
        uint64_t{m.frame.comp_off} + m.frame.comp_len <= _h.data_size &&
        (_h.leaf != NumericLeaf::None || m.frame.comp_len == m.frame.raw_len) &&
        (Runs(_h.transform) || BitPacked(_h.transform) ||
         m.frame.raw_len == rows * _h.stored),
      "numeric codec: corrupted frame table");
    return m;
  }

  void Decode(uint32_t f) {
    const auto m = Meta(f);
    const auto end = EndRow(f);
    const auto rows = static_cast<uint32_t>(end - m.frame.first_entry);
    duckdb::shared_ptr<DecodedFrame> held;
    if (_h.transform == NumericTransform::RleFfor) {
      LoadPackedRuns(m, rows);
    } else if (_h.transform == NumericTransform::Rle) {
      LoadRuns(m, rows, f);
    } else if (_h.transform == NumericTransform::Raw && !_h.Shuffled()) {
      _view = m.frame.comp_len == m.frame.raw_len
                ? Data(m)
                : Inflated(m, reinterpret_cast<uint8_t*>(Rows(rows)), f, held);
    } else {
      DecodeInto(m, rows, Rows(rows), f);
      _view = reinterpret_cast<const uint8_t*>(_rows.data());
    }
    _view_held = std::move(held);
    _begin = m.frame.first_entry;
    _end = end;
    _next = end;
  }

  U* Rows(size_t rows) {
    if (_rows.size() < rows) {
      _rows.resize(rows);
    }
    return _rows.data();
  }

  uint64_t DecodeDirect(uint64_t row, uint64_t count, U* out) {
    if ((row >= _begin && row < _end) || Runs(_h.transform)) {
      return 0;
    }
    if (Blocked()) {
      const auto rows =
        std::min<uint64_t>(numeric::kBlockValues, _h.row_count - row);
      if (row % numeric::kBlockValues != 0 || rows > count) {
        return 0;
      }
      UnpackBlock(row, rows, out);
      return rows;
    }
    const auto f = FrameOf(row);
    const auto end = EndRow(f);
    if (Streams(row, f)) {
      const auto rows = std::min<uint64_t>(count, end - row);
      DecodeRange(f, Meta(f), row, rows, out);
      return rows;
    }
    if (FirstRow(f) != row || end - row > count) {
      return 0;
    }
    const auto rows = static_cast<uint32_t>(end - row);
    _scan = true;
    DecodeInto(Meta(f), rows, out, f);
    _next = end;
    return rows;
  }

  bool Streams(uint64_t row, uint32_t f) const noexcept {
    if (_h.Shuffled()) {
      return false;
    }
    return _h.transform == NumericTransform::For ||
           _h.transform == NumericTransform::Dict ||
           (_h.transform == NumericTransform::Delta &&
            (row == _delta_end || row == FirstRow(f)));
  }

  void DecodeRange(uint32_t f, const NumericFrameMeta& m, uint64_t row,
                   uint64_t rows, U* out) {
    const auto at = row - m.frame.first_entry;
    const uint8_t* src = Data(m);
    if (m.frame.comp_len != m.frame.raw_len) {
      if (f != _source_frame || _source_len != m.frame.raw_len) {
        OpenSource(f, m, m.frame.raw_len, true);
      }
      src = _source;
    }
    src += at * _h.stored;
    if (_h.transform == NumericTransform::For) {
      numeric::WidenAdd(src, rows, _h.stored, static_cast<U>(_h.base), out);
    } else if (_h.transform == NumericTransform::Dict) {
      DecodeCodes(src, static_cast<uint32_t>(rows), out);
    } else {
      _delta_acc = numeric::WidenPrefixSum<true>(
        src, rows, _h.stored, at == 0 ? static_cast<U>(m.base) : _delta_acc,
        static_cast<U>(_h.base), out);
      _delta_end = row + rows;
    }
  }

  const uint8_t* Data(const NumericFrameMeta& m) const noexcept {
    return _base + _h.off_data + m.frame.comp_off;
  }

  const std::string& FrameKey(uint32_t frame) {
    if (_segment_key.empty()) {
      _segment_key = DictionaryCacheKey(_slot.file_id, _slot.file_offset);
    }
    FrameCacheKey(_segment_key, frame, _key);
    return _key;
  }

  const uint8_t* Lookup(const std::string& key, uint32_t raw_len,
                        duckdb::shared_ptr<DecodedFrame>& held) {
    auto hit = _cache->Get<DecodedFrame>(key);
    if (!hit || hit->Bytes().size() != raw_len) {
      return nullptr;
    }
    held = std::move(hit);
    return held->Bytes().data();
  }

  const uint8_t* Inflated(const NumericFrameMeta& m, uint8_t* dst,
                          uint32_t frame,
                          duckdb::shared_ptr<DecodedFrame>& held) {
    if (m.frame.comp_len == m.frame.raw_len) {
      return Data(m);
    }
    if (_cache && Admitted()) {
      if (const auto* cached = Lookup(FrameKey(frame), m.frame.raw_len, held)) {
        return cached;
      }
    }
    return Inflate(m, dst, held);
  }

  const uint8_t* Inflate(const NumericFrameMeta& m, uint8_t* dst,
                         duckdb::shared_ptr<DecodedFrame>& held) {
    ScratchVector<uint8_t> admitted;
    if (_cache && Admitted()) {
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
      _lz4 ? _lz4->Decompress(in, m.frame.comp_len, out, m.frame.raw_len)
           : _h.leaf == NumericLeaf::Zstd &&
               (_zstd ? *_zstd : _zstd.emplace())
                 .Decompress(in, m.frame.comp_len, out, m.frame.raw_len);
    SDB_ENSURE(ok, "numeric codec: corrupted frame");
    if (!admitted.empty()) {
      held = duckdb::make_shared_ptr<DecodedFrame>(std::move(admitted));
      _cache->Put(_key, held);
    }
    return dst;
  }

  void OpenFfor(uint32_t f) {
    const auto m = Meta(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    const auto blocks =
      (rows + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _ffor_src = Data(m);
    size_t off = blocks * kFforBlockMetaBytes;
    SDB_ENSURE(m.frame.comp_len == m.frame.raw_len && off <= m.frame.raw_len,
               "numeric codec: corrupted blocks");
    const unsigned max_bits = _h.transform == NumericTransform::DictFfor
                                ? numeric::kMaxBits<uint32_t>
                                : numeric::kMaxBits<U>;
    for (size_t b = 0; b < blocks; ++b) {
      const unsigned bits =
        _ffor_src[b * kFforBlockMetaBytes + sizeof(uint64_t)];
      SDB_ENSURE(bits <= max_bits, "numeric codec: corrupted blocks");
      _ffor_offsets[b] = static_cast<uint32_t>(off);
      off += numeric::PackedBytes(bits);
    }
    SDB_ENSURE(off == m.frame.raw_len, "numeric codec: corrupted blocks");
    _ffor_frame = f;
  }

  void UnpackBlock(uint64_t first, uint64_t rows, U* out) {
    using Word = numeric::LaneWord<U>;
    if (const auto f = static_cast<uint32_t>(first / kFforFrameRows);
        f != _ffor_frame) {
      OpenFfor(f);
    }
    const auto b = (first % kFforFrameRows) / numeric::kBlockValues;
    const auto* meta = _ffor_src + b * kFforBlockMetaBytes;
    const unsigned bits = meta[sizeof(uint64_t)];
    const auto* packed = _ffor_src + _ffor_offsets[b];
    if (_h.transform == NumericTransform::DictFfor) {
      if (_codes.size() < numeric::kBlockValues) {
        _codes.resize(numeric::kBlockValues);
      }
      const auto code_base = duckdb::Load<uint64_t>(meta);
      numeric::kUnpack<uint32_t>[bits](Words<uint32_t>(packed, bits),
                                       _codes.data(),
                                       static_cast<uint32_t>(code_base));
      const auto largest = code_base + (uint64_t{1} << bits) - 1;
      Gather(_codes.data(), rows, out, largest >= _h.dict_count);
      return;
    }
    const auto base = static_cast<U>(duckdb::Load<uint64_t>(meta));
    const auto* words = Words<Word>(packed, bits);
    if (rows == numeric::kBlockValues) {
      numeric::kUnpack<U>[bits](words, out, base);
      return;
    }
    _block.resize(numeric::kBlockValues);
    numeric::kUnpack<U>[bits](words, _block.data(), base);
    std::memcpy(out, _block.data(), rows * sizeof(U));
  }

  void DecodeBlock(uint64_t row) {
    const auto first = row - row % numeric::kBlockValues;
    UnpackBlock(first, numeric::kBlockValues, Rows(numeric::kBlockValues));
    _view = reinterpret_cast<const uint8_t*>(_rows.data());
    _begin = first;
    _end = std::min<uint64_t>(first + numeric::kBlockValues, _h.row_count);
  }

  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out,
                  uint32_t frame) {
    auto* bytes = reinterpret_cast<uint8_t*>(out);
    if (_h.transform == NumericTransform::Raw && !_h.Shuffled()) {
      if (const auto* src = Inflated(m, bytes, frame, _held); src != bytes) {
        std::memcpy(out, src, m.frame.raw_len);
      }
      return;
    }
    const auto* src = Inflated(m, nullptr, frame, _held);
    if (_h.Shuffled()) {
      if (_h.transform == NumericTransform::Raw) {
        numeric::Unshuffle(src, rows, _h.stored, bytes);
        return;
      }
      if (_shuffled.size() < m.frame.raw_len) {
        _shuffled.resize(m.frame.raw_len);
      }
      numeric::Unshuffle(src, rows, _h.stored, _shuffled.data());
      src = _shuffled.data();
    }
    switch (_h.transform) {
      case NumericTransform::For:
        numeric::WidenAdd(src, rows, _h.stored, static_cast<U>(_h.base), out);
        break;
      case NumericTransform::Delta:
        numeric::WidenPrefixSum<true>(src, rows, _h.stored,
                                      static_cast<U>(m.base),
                                      static_cast<U>(_h.base), out);
        break;
      case NumericTransform::Dict:
        DecodeCodes(src, rows, out);
        break;
      case NumericTransform::Raw:
      case NumericTransform::Rle:
      case NumericTransform::Ffor:
      case NumericTransform::RleFfor:
      case NumericTransform::DictFfor:
        SDB_UNREACHABLE();
    }
  }

  void DecodeCodes(const uint8_t* src, uint32_t rows, U* out) {
    if (_h.stored == sizeof(uint8_t)) {
      GatherNarrow<uint8_t>(src, rows, out);
      return;
    }
    if (_h.stored == sizeof(uint16_t)) {
      GatherNarrow<uint16_t>(src, rows, out);
      return;
    }
    if (_codes.size() < rows) {
      _codes.resize(rows);
    }
    numeric::Widen(src, rows, _h.stored, _codes.data());
    Gather(_codes.data(), rows, out);
  }

  template<typename C>
  void GatherNarrow(const uint8_t* src, uint32_t rows, U* out) const {
    if (_h.dict_count <= std::numeric_limits<C>::max()) {
      C worst = 0;
      for (uint32_t i = 0; i < rows; ++i) {
        worst = std::max(worst, duckdb::Load<C>(src + size_t{i} * sizeof(C)));
      }
      SDB_ENSURE(worst < _h.dict_count, "numeric codec: corrupted codes");
    }
    const auto* dict = _base + _h.off_dict;
    for (uint32_t i = 0; i < rows; ++i) {
      const auto code = duckdb::Load<C>(src + size_t{i} * sizeof(C));
      out[i] = duckdb::Load<U>(dict + size_t{code} * sizeof(U));
    }
  }

  void Gather(const uint32_t* codes, uint64_t rows, U* out,
              bool checked = true) const {
    if (checked) {
      uint32_t worst = 0;
      for (uint64_t i = 0; i < rows; ++i) {
        worst = std::max(worst, codes[i]);
      }
      SDB_ENSURE(worst < _h.dict_count, "numeric codec: corrupted codes");
    }
    const auto* dict = _base + _h.off_dict;
    for (uint64_t i = 0; i < rows; ++i) {
      out[i] = duckdb::Load<U>(dict + size_t{codes[i]} * sizeof(U));
    }
  }

  template<typename W>
  const W* Words(const uint8_t* packed, unsigned bits) {
    if (reinterpret_cast<uintptr_t>(packed) % alignof(W) == 0) {
      return reinterpret_cast<const W*>(packed);
    }
    auto& words = [&]() -> ScratchVector<W>& {
      if constexpr (sizeof(W) == sizeof(uint32_t)) {
        return _words32;
      } else {
        return _words64;
      }
    }();
    words.resize(numeric::PackedBytes(8 * sizeof(W)) / sizeof(W));
    std::memcpy(words.data(), packed, numeric::PackedBytes(bits));
    return words.data();
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
      numeric::kUnpack<V>[bits](Words<numeric::LaneWord<V>>(src + off, bits),
                                out + b * numeric::kBlockValues,
                                static_cast<V>(duckdb::Load<uint64_t>(meta)));
      off += numeric::PackedBytes(bits);
    }
    return off;
  }

  bool Admitted() noexcept {
    if (_admit < 0) {
      _admit = _slot.touched && SecondTouch(_slot);
    }
    return _admit == 1;
  }

  void LoadRuns(const NumericFrameMeta& m, uint32_t rows, uint32_t frame) {
    const auto* src = Inflated(m, nullptr, frame, _held);
    const uint32_t item = _h.stored + _h.run_width;
    SDB_ENSURE(m.frame.raw_len % item == 0 && m.frame.raw_len != 0,
               "numeric codec: corrupted runs");
    const uint32_t runs = m.frame.raw_len / item;
    _run_values.resize(runs);
    _run_ends.resize(runs);
    numeric::WidenAdd(src, runs, _h.stored, static_cast<U>(_h.base),
                      _run_values.data());
    numeric::Widen(src + size_t{runs} * _h.stored, runs, _h.run_width,
                   _run_ends.data());
    EndRuns(rows);
  }

  void LoadPackedRuns(const NumericFrameMeta& m, uint32_t rows) {
    SDB_ENSURE(m.frame.comp_len == m.frame.raw_len && m.base != 0 &&
                 m.base <= rows && m.base <= kRleFforFrameRuns,
               "numeric codec: corrupted runs");
    const auto runs = static_cast<uint32_t>(m.base);
    const size_t blocks =
      (runs + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _run_values.resize(blocks * numeric::kBlockValues);
    _run_ends.resize(blocks * numeric::kBlockValues);
    const auto* src = Data(m);
    auto off =
      UnpackStream(src, m.frame.raw_len, 0, blocks, _run_values.data());
    off = UnpackStream(src, m.frame.raw_len, off, blocks, _run_ends.data());
    SDB_ENSURE(off == m.frame.raw_len, "numeric codec: corrupted runs");
    _run_values.resize(runs);
    _run_ends.resize(runs);
    EndRuns(rows);
  }

  void EndRuns(uint32_t rows) {
    uint64_t total = 0;
    for (auto& end : _run_ends) {
      SDB_ENSURE(end != 0, "numeric codec: corrupted runs");
      total += end;
      end = static_cast<uint32_t>(total);
    }
    SDB_ENSURE(total == rows, "numeric codec: corrupted runs");
    _long_runs = total >= uint64_t{kBurst} * _run_ends.size();
    _hint = 0;
  }

  uint32_t RunOf(uint32_t pos) const noexcept {
    if (_hint < _run_ends.size() && pos < _run_ends[_hint] &&
        (_hint == 0 || pos >= _run_ends[_hint - 1])) {
      return _hint;
    }
    if (_hint + 1 < _run_ends.size() && pos >= _run_ends[_hint] &&
        pos < _run_ends[_hint + 1]) {
      return _hint + 1;
    }
    return static_cast<uint32_t>(absl::c_upper_bound(_run_ends, pos) -
                                 _run_ends.begin());
  }

  duckdb::const_data_ptr_t _base;
  NumericHeader _h;
  duckdb::ObjectCache* _cache;
  ReadContext::CacheSlot _slot;
  const IndexInput* _read_once;
  UnmapBehind _behind;
  std::string _segment_key;
  std::string _key;
  duckdb::shared_ptr<DecodedFrame> _held;
  duckdb::shared_ptr<DecodedFrame> _view_held;
  int8_t _admit = -1;
  uint32_t _per_frame = 0;
  uint32_t _source_frame = kNoFrame;
  size_t _source_len = 0;
  const uint8_t* _source = nullptr;
  duckdb::shared_ptr<DecodedFrame> _source_held;
  uint64_t _source_first = 0;
  U _source_base = 0;
  uint32_t _summed = 0;
  U _sum = 0;
  U _scale = 0;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  uint64_t _next = 0;
  bool _scan = false;
  const uint8_t* _view = nullptr;
  uint64_t _delta_end = 0;
  U _delta_acc = 0;
  ScratchVector<U> _rows;
  ScratchVector<U> _run_values;
  ScratchVector<uint32_t> _run_ends;
  uint32_t _hint = 0;
  bool _long_runs = false;
  ScratchVector<uint8_t> _raw;
  ScratchVector<uint8_t> _shuffled;
  ScratchVector<uint32_t> _codes;
  ScratchVector<uint32_t> _words32;
  ScratchVector<uint64_t> _words64;
  ScratchVector<U> _block;
  const uint8_t* _ffor_src = nullptr;
  uint32_t _ffor_frame = std::numeric_limits<uint32_t>::max();
  std::array<uint32_t, kFforFrameRows / numeric::kBlockValues> _ffor_offsets{};
  std::optional<Lz4::Decompressor> _lz4;
  std::optional<Zstd::Decompressor> _zstd;
};

}  // namespace irs::codecs
