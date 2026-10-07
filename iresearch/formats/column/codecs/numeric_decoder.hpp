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
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/typedefs.hpp>
#include <limits>
#include <optional>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

template<typename T>
class FrameDecoder {
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint32_t kBurst = 32 / sizeof(T);

 public:
  FrameDecoder(duckdb::const_data_ptr_t base, const NumericHeader& header)
    : _base{base}, _h{header} {
    SDB_ENSURE(_h.width == sizeof(T), "numeric codec: width mismatch");
    if (_h.transform != NumericTransform::Rle) {
      _per_frame = _h.FrameBytes() / _h.stored;
    }
    if (_h.transform == NumericTransform::Dict) {
      _dict.resize(_h.dict_count);
      std::memcpy(_dict.data(), _base + _h.off_dict,
                  size_t{_h.dict_count} * sizeof(U));
    }
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

  uint64_t FirstRow(uint32_t f) const noexcept {
    return duckdb::Load<uint32_t>(_base + _h.off_frames +
                                  f * kNumericFrameMetaSize);
  }

  uint64_t EndRow(uint32_t f) const noexcept {
    return f + 1 < _h.frame_count ? FirstRow(f + 1) : _h.row_count;
  }

  uint64_t Begin() const noexcept { return _begin; }
  uint64_t End() const noexcept { return _end; }

  T FrameMin(uint32_t f) const noexcept { return Bound(f, kNumericFrameMinAt); }
  T FrameMax(uint32_t f) const noexcept { return Bound(f, kNumericFrameMaxAt); }

  void Seek(uint64_t row) {
    if (row < _begin || row >= _end) {
      Decode(FrameOf(row));
    }
  }

  void Copy(uint64_t row, uint64_t count, T* out) noexcept {
    if (_h.transform != NumericTransform::Rle) {
      std::memcpy(out, _rows.data() + (row - _begin), count * sizeof(T));
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
      std::memcpy(&v, _rows.data() + (row - _begin), sizeof(T));
      return v;
    }
    const auto r = RunOf(static_cast<uint32_t>(row - _begin));
    _hint = r;
    std::memcpy(&v, &_run_values[r], sizeof(T));
    return v;
  }

  void Decode(uint32_t f) {
    if (f == _frame) {
      return;
    }
    const auto m = Meta(f);
    const auto end = EndRow(f);
    const auto rows = static_cast<uint32_t>(end - m.frame.first_entry);
    if (_h.transform == NumericTransform::Rle) {
      LoadRuns(m, rows);
    } else {
      if (_rows.size() < rows) {
        _rows.resize(rows);
      }
      DecodeInto(m, rows, _rows.data());
    }
    _begin = m.frame.first_entry;
    _end = end;
    _frame = f;
  }

  uint64_t DecodeDirect(uint64_t row, uint64_t count, U* out) {
    if ((row >= _begin && row < _end) ||
        _h.transform == NumericTransform::Rle) {
      return 0;
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

 private:
  T Bound(uint32_t f, size_t at) const noexcept {
    const auto bits = static_cast<U>(duckdb::Load<uint64_t>(
      _base + _h.off_frames + f * kNumericFrameMetaSize + at));
    T v;
    std::memcpy(&v, &bits, sizeof(T));
    return v;
  }

  NumericFrameMeta Meta(uint32_t f) const {
    SDB_ENSURE(f < _h.frame_count, "numeric codec: frame out of range");
    const auto m =
      NumericFrameMeta::Load(_base + _h.off_frames + f * kNumericFrameMetaSize);
    const auto end = EndRow(f);
    SDB_ENSURE(m.frame.first_entry < end && end <= _h.row_count &&
                 m.frame.raw_len <= _h.FrameBytes() &&
                 uint64_t{m.frame.comp_off} + m.frame.comp_len <= _h.data_size,
               "numeric codec: corrupted frame table");
    return m;
  }

  void Inflate(const uint8_t* src, const NumericFrameMeta& m, uint8_t* dst) {
    const auto* in = reinterpret_cast<const char*>(src);
    auto* out = reinterpret_cast<char*>(dst);
    const bool ok =
      _lz4 ? _lz4->Decompress(in, m.frame.comp_len, out, m.frame.raw_len)
           : _zstd &&
               _zstd->Decompress(in, m.frame.comp_len, out, m.frame.raw_len);
    SDB_ENSURE(ok, "numeric codec: corrupted frame");
  }

  void DecodeInto(const NumericFrameMeta& m, uint32_t rows, U* out) {
    const auto* src = _base + _h.off_data + m.frame.comp_off;
    const bool compressed = m.frame.comp_len != m.frame.raw_len;
    if (_h.transform != NumericTransform::Rle) {
      SDB_ENSURE(m.frame.raw_len == rows * _h.stored,
                 "numeric codec: corrupted frame size");
    }
    if (_h.transform == NumericTransform::Raw && !_h.Shuffled()) {
      if (compressed) {
        Inflate(src, m, reinterpret_cast<uint8_t*>(out));
      } else {
        std::memcpy(out, src, m.frame.raw_len);
      }
      return;
    }
    if (compressed) {
      if (_raw.size() < m.frame.raw_len) {
        _raw.resize(m.frame.raw_len);
      }
      Inflate(src, m, _raw.data());
      src = _raw.data();
    }
    if (_h.Shuffled()) {
      if (_shuffled.size() < m.frame.raw_len) {
        _shuffled.resize(m.frame.raw_len);
      }
      numeric::Unshuffle(src, rows, _h.stored, _shuffled.data());
      src = _shuffled.data();
    }
    switch (_h.transform) {
      case NumericTransform::Raw:
        std::memcpy(out, src, size_t{rows} * sizeof(U));
        break;
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
      case NumericTransform::Rle:
        break;
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
    const auto* dict = _dict.data();
    for (uint32_t i = 0; i < rows; ++i) {
      out[i] = dict[_codes[i]];
    }
  }

  void LoadRuns(const NumericFrameMeta& m, uint32_t rows) {
    const uint8_t* src = _base + _h.off_data + m.frame.comp_off;
    if (m.frame.comp_len != m.frame.raw_len) {
      if (_raw.size() < m.frame.raw_len) {
        _raw.resize(m.frame.raw_len);
      }
      Inflate(src, m, _raw.data());
      src = _raw.data();
    }
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
  uint32_t _frame = std::numeric_limits<uint32_t>::max();
  uint64_t _begin = 0;
  uint64_t _end = 0;
  std::vector<U> _dict;
  std::vector<U> _rows;
  std::vector<U> _run_values;
  std::vector<uint32_t> _run_ends;
  uint32_t _hint = 0;
  std::vector<uint8_t> _raw;
  std::vector<uint8_t> _shuffled;
  std::vector<uint32_t> _codes;
  std::optional<LeafDecompressor<ByteCodec::Lz4>> _lz4;
  std::optional<LeafDecompressor<ByteCodec::Zstd>> _zstd;
};

}  // namespace irs::codecs
