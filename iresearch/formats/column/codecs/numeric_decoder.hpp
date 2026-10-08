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
#include <limits>
#include <optional>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs {

template<typename T>
class FrameDecoder {
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint32_t kBurst = 32 / sizeof(T);

 public:
  FrameDecoder(duckdb::const_data_ptr_t base, const NumericHeader& header)
    : _base{base}, _h{header} {
    SDB_ENSURE(_h.width == sizeof(T), "numeric codec: width mismatch");
    if (_h.transform == NumericTransform::Ffor) {
      _per_frame = kFforFrameRows;
    } else if (_h.transform != NumericTransform::Rle) {
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
    } else if (_h.leaf == NumericLeaf::Zstd) {
      _zstd.emplace();
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

  uint64_t End() const noexcept { return _end; }

  T FrameMin(uint32_t f) const noexcept {
    return std::bit_cast<T>(static_cast<U>(MetaAt(f).min));
  }
  T FrameMax(uint32_t f) const noexcept {
    return std::bit_cast<T>(static_cast<U>(MetaAt(f).max));
  }

  void Seek(uint64_t row) {
    if (row >= _begin && row < _end) {
      return;
    }
    if (_h.transform == NumericTransform::Ffor) {
      DecodeBlock(row);
    } else {
      Decode(FrameOf(row));
    }
  }

  void Read(uint64_t row, uint64_t count, T* out) {
    uint64_t done = 0;
    while (done < count) {
      if constexpr (std::is_integral_v<T>) {
        if (const auto n = DecodeDirect(row + done, count - done,
                                        reinterpret_cast<U*>(out + done))) {
          done += n;
          continue;
        }
      }
      Seek(row + done);
      const auto take = std::min<uint64_t>(count - done, _end - (row + done));
      Copy(row + done, take, out + done);
      done += take;
    }
  }

  void Copy(uint64_t row, uint64_t count, T* out) noexcept {
    if (_h.transform != NumericTransform::Rle) {
      std::memcpy(out, _view + (row - _begin) * sizeof(T), count * sizeof(T));
      return;
    }
    auto pos = static_cast<uint32_t>(row - _begin);
    const auto end = pos + static_cast<uint32_t>(count);
    auto r = RunOf(pos);
    auto* dst = reinterpret_cast<U*>(out);
    const uint64_t burst_limit = count >= kBurst ? count - kBurst + 1 : 0;
    uint64_t done = 0;
    while (pos < end) {
      const auto stop = std::min(_run_ends[r], end);
      const U v = _run_values[r];
      const auto n = stop - pos;
      if (n <= kBurst && done < burst_limit) {
        for (uint32_t i = 0; i < kBurst; ++i) {
          dst[done + i] = v;
        }
      } else {
        std::fill_n(dst + done, n, v);
      }
      done += n;
      pos = stop;
      ++r;
    }
    _hint = r - 1;
  }

  T At(uint64_t row) noexcept {
    T v;
    if (_h.transform != NumericTransform::Rle) {
      std::memcpy(&v, _view + (row - _begin) * sizeof(T), sizeof(T));
      return v;
    }
    const auto r = RunOf(static_cast<uint32_t>(row - _begin));
    _hint = r;
    std::memcpy(&v, &_run_values[r], sizeof(T));
    return v;
  }

 private:
  uint64_t FirstRow(uint32_t f) const noexcept {
    return duckdb::Load<uint32_t>(_base + _h.off_frames +
                                  f * kNumericFrameMetaSize);
  }

  uint64_t EndRow(uint32_t f) const noexcept {
    return f + 1 < _h.frame_count ? FirstRow(f + 1) : _h.row_count;
  }

  NumericFrameMeta MetaAt(uint32_t f) const noexcept {
    SDB_ASSERT(f < _h.frame_count);
    return NumericFrameMeta::Load(_base + _h.off_frames +
                                  f * kNumericFrameMetaSize);
  }

  NumericFrameMeta Meta(uint32_t f) const {
    SDB_ENSURE(f < _h.frame_count, "numeric codec: frame out of range");
    const auto m = MetaAt(f);
    const auto rows = EndRow(f) - m.frame.first_entry;
    SDB_ENSURE(
      m.frame.raw_len <= _h.FrameBytes() &&
        uint64_t{m.frame.comp_off} + m.frame.comp_len <= _h.data_size &&
        (_h.transform == NumericTransform::Rle ||
         _h.transform == NumericTransform::Ffor ||
         m.frame.raw_len == rows * _h.stored),
      "numeric codec: corrupted frame table");
    return m;
  }

  void Decode(uint32_t f) {
    const auto m = Meta(f);
    const auto end = EndRow(f);
    const auto rows = static_cast<uint32_t>(end - m.frame.first_entry);
    if (_h.transform == NumericTransform::Rle) {
      LoadRuns(m, rows);
    } else if (_h.transform == NumericTransform::Raw && !_h.Shuffled() &&
               m.frame.comp_len == m.frame.raw_len) {
      _view = Data(m);
    } else {
      if (_rows.size() < rows) {
        _rows.resize(rows);
      }
      DecodeInto(m, rows, _rows.data());
      _view = reinterpret_cast<const uint8_t*>(_rows.data());
    }
    _begin = m.frame.first_entry;
    _end = end;
  }

  uint64_t DecodeDirect(uint64_t row, uint64_t count, U* out) {
    if ((row >= _begin && row < _end) ||
        _h.transform == NumericTransform::Rle) {
      return 0;
    }
    if (_h.transform == NumericTransform::Ffor) {
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
    if (FirstRow(f) != row || end - row > count) {
      return 0;
    }
    const auto rows = static_cast<uint32_t>(end - row);
    DecodeInto(Meta(f), rows, out);
    return rows;
  }

  const uint8_t* Data(const NumericFrameMeta& m) const noexcept {
    return _base + _h.off_data + m.frame.comp_off;
  }

  const uint8_t* Inflated(const NumericFrameMeta& m, uint8_t* dst) {
    if (m.frame.comp_len == m.frame.raw_len) {
      return Data(m);
    }
    if (!dst) {
      if (_raw.size() < m.frame.raw_len) {
        _raw.resize(m.frame.raw_len);
      }
      dst = _raw.data();
    }
    const auto* in = reinterpret_cast<const char*>(Data(m));
    auto* out = reinterpret_cast<char*>(dst);
    const bool ok =
      _lz4 ? _lz4->Decompress(in, m.frame.comp_len, out, m.frame.raw_len)
           : _zstd &&
               _zstd->Decompress(in, m.frame.comp_len, out, m.frame.raw_len);
    SDB_ENSURE(ok, "numeric codec: corrupted frame");
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
    for (size_t b = 0; b < blocks; ++b) {
      const unsigned bits =
        _ffor_src[b * kFforBlockMetaBytes + sizeof(uint64_t)];
      SDB_ENSURE(bits <= numeric::kMaxBits<U>,
                 "numeric codec: corrupted blocks");
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
    const auto base = static_cast<U>(duckdb::Load<uint64_t>(meta));
    const unsigned bits = meta[sizeof(uint64_t)];
    const auto* packed = _ffor_src + _ffor_offsets[b];
    const auto* words = reinterpret_cast<const Word*>(packed);
    if (reinterpret_cast<uintptr_t>(packed) % alignof(Word) != 0) {
      _words.resize(numeric::PackedBytes(numeric::kMaxBits<U>) / sizeof(Word));
      std::memcpy(_words.data(), packed, numeric::PackedBytes(bits));
      words = _words.data();
    }
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
    if (_rows.size() < numeric::kBlockValues) {
      _rows.resize(numeric::kBlockValues);
    }
    UnpackBlock(first, numeric::kBlockValues, _rows.data());
    _view = reinterpret_cast<const uint8_t*>(_rows.data());
    _begin = first;
    _end = std::min<uint64_t>(first + numeric::kBlockValues, _h.row_count);
  }

  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out) {
    auto* bytes = reinterpret_cast<uint8_t*>(out);
    if (_h.transform == NumericTransform::Raw && !_h.Shuffled()) {
      if (const auto* src = Inflated(m, bytes); src != bytes) {
        std::memcpy(out, src, m.frame.raw_len);
      }
      return;
    }
    const auto* src = Inflated(m, nullptr);
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
        numeric::Widen(src, rows, _h.stored, out);
        numeric::AddBase(out, rows, static_cast<U>(_h.base));
        break;
      case NumericTransform::Delta:
        numeric::Widen(src, rows, _h.stored, out);
        numeric::PrefixSum(out, rows, static_cast<U>(m.base),
                           static_cast<U>(_h.base));
        break;
      case NumericTransform::Dict:
        DecodeCodes(src, rows, out);
        break;
      case NumericTransform::Raw:
      case NumericTransform::Rle:
      case NumericTransform::Ffor:
        SDB_UNREACHABLE();
    }
  }

  void DecodeCodes(const uint8_t* src, uint32_t rows, U* out) {
    if (_codes.size() < rows) {
      _codes.resize(rows);
    }
    numeric::Widen(src, rows, _h.stored, _codes.data());
    uint32_t worst = 0;
    for (uint32_t i = 0; i < rows; ++i) {
      worst = std::max(worst, _codes[i]);
    }
    SDB_ENSURE(worst < _h.dict_count, "numeric codec: corrupted codes");
    const auto* dict = _base + _h.off_dict;
    for (uint32_t i = 0; i < rows; ++i) {
      out[i] = duckdb::Load<U>(dict + size_t{_codes[i]} * sizeof(U));
    }
  }

  void LoadRuns(const NumericFrameMeta& m, uint32_t rows) {
    const auto* src = Inflated(m, nullptr);
    const uint32_t item = _h.stored + _h.run_width;
    SDB_ENSURE(m.frame.raw_len % item == 0 && m.frame.raw_len != 0,
               "numeric codec: corrupted runs");
    const uint32_t runs = m.frame.raw_len / item;
    _run_values.resize(runs);
    _run_ends.resize(runs);
    numeric::Widen(src, runs, _h.stored, _run_values.data());
    numeric::AddBase(_run_values.data(), runs, static_cast<U>(_h.base));
    numeric::Widen(src + size_t{runs} * _h.stored, runs, _h.run_width,
                   _run_ends.data());
    uint64_t total = 0;
    for (uint32_t r = 0; r < runs; ++r) {
      SDB_ENSURE(_run_ends[r] != 0, "numeric codec: corrupted runs");
      total += _run_ends[r];
      _run_ends[r] = static_cast<uint32_t>(total);
    }
    SDB_ENSURE(total == rows, "numeric codec: corrupted runs");
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
    return static_cast<uint32_t>(
      std::upper_bound(_run_ends.begin(), _run_ends.end(), pos) -
      _run_ends.begin());
  }

  duckdb::const_data_ptr_t _base;
  NumericHeader _h;
  uint32_t _per_frame = 0;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  const uint8_t* _view = nullptr;
  std::vector<U> _rows;
  std::vector<U> _run_values;
  std::vector<uint32_t> _run_ends;
  uint32_t _hint = 0;
  std::vector<uint8_t> _raw;
  std::vector<uint8_t> _shuffled;
  std::vector<uint32_t> _codes;
  std::vector<numeric::LaneWord<U>> _words;
  std::vector<U> _block;
  const uint8_t* _ffor_src = nullptr;
  uint32_t _ffor_frame = std::numeric_limits<uint32_t>::max();
  std::array<uint32_t, kFforFrameRows / numeric::kBlockValues> _ffor_offsets{};
  std::optional<LeafDecompressor<ByteCodec::Lz4>> _lz4;
  std::optional<LeafDecompressor<ByteCodec::Zstd>> _zstd;
};

}  // namespace irs::codecs
