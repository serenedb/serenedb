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
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

template<typename V>
class FforBlocks {
 public:
  template<typename Less>
  void Plan(std::span<const V> v, Less less) {
    const size_t blocks =
      (v.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.resize(blocks);
    _bits.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(v.size(), begin + numeric::kBlockValues);
      V lo = v[begin];
      V hi = lo;
      for (size_t i = begin + 1; i < end; ++i) {
        if (less(v[i], lo)) {
          lo = v[i];
        }
        if (less(hi, v[i])) {
          hi = v[i];
        }
      }
      _base[b] = lo;
      _bits[b] = static_cast<uint8_t>(std::bit_width(static_cast<V>(hi - lo)));
    }
  }

  uint64_t Bytes(size_t first, size_t count) const noexcept {
    uint64_t bytes = BlockMetaBytes(count, sizeof(V));
    for (size_t b = first; b < first + count; ++b) {
      bytes += StoredBytes(_bits[b]);
    }
    return bytes;
  }

  uint64_t Bytes(uint32_t frame_values) const noexcept {
    const size_t per = frame_values / numeric::kBlockValues;
    uint64_t bytes = 0;
    for (size_t first = 0; first < _bits.size(); first += per) {
      bytes += Bytes(first, std::min(per, _bits.size() - first));
    }
    return bytes;
  }

  V FrameStep(size_t first) const noexcept {
    return _steps[first / kFrameBlocks];
  }

  bool EqualSteps(size_t first, size_t count) const noexcept {
    const uint64_t step = FrameStep(first);
    for (size_t b = first; b < first + count; ++b) {
      const auto start = static_cast<V>(
        _first[first] + (b - first) * numeric::kBlockValues * step);
      if (_bits[b] != 0 || _first[b] != start) {
        return false;
      }
    }
    return true;
  }

  static uint64_t SampledDeltaBytes(std::span<const V> values) noexcept {
    constexpr size_t kEvery = 8;
    const size_t blocks =
      (values.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    uint64_t sampled = 0;
    size_t samples = 0;
    for (size_t b = 0; b < blocks; b += kEvery, ++samples) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(values.size(), begin + numeric::kBlockValues);
      S lo = 0;
      S hi = 0;
      for (size_t i = begin + 1; i < end; ++i) {
        const auto step = static_cast<S>(values[i] - values[i - 1]);
        lo = i == begin + 1 ? step : std::min(lo, step);
        hi = i == begin + 1 ? step : std::max(hi, step);
      }
      sampled += numeric::PackedBytes(std::bit_width(
        static_cast<V>(static_cast<V>(hi) - static_cast<V>(lo))));
    }
    return samples == 0 ? 0 : sampled * blocks / samples;
  }

  void PlanEqualSteps(std::span<const V> values, V step) {
    const size_t blocks =
      (values.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.assign(blocks, step);
    _bits.assign(blocks, 0);
    _steps.assign((blocks + kFrameBlocks - 1) / kFrameBlocks, step);
    _first.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      _first[b] = values[b * numeric::kBlockValues];
    }
  }

  void PlanDeltas(std::span<const V> values, ScratchVector<V>& deltas) {
    const size_t blocks =
      (values.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.resize(blocks);
    _top.resize(blocks);
    _bits.resize(blocks);
    _first.resize(blocks);
    _steps.resize((blocks + kFrameBlocks - 1) / kFrameBlocks);
    deltas.resize(values.size());
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(values.size(), begin + numeric::kBlockValues);
      S lo = 0;
      S hi = 0;
      for (size_t i = begin + 1; i < end; ++i) {
        const auto step = static_cast<V>(values[i] - values[i - 1]);
        deltas[i] = step;
        lo = i == begin + 1 ? static_cast<S>(step)
                            : std::min(lo, static_cast<S>(step));
        hi = i == begin + 1 ? static_cast<S>(step)
                            : std::max(hi, static_cast<S>(step));
      }
      _first[b] = values[begin];
      _base[b] = static_cast<V>(lo);
      _top[b] = static_cast<V>(hi);
    }
    for (size_t first = 0; first < blocks; first += kFrameBlocks) {
      const size_t end = std::min(blocks, first + kFrameBlocks);
      const V step = LeastBytesStep(first, end);
      _steps[first / kFrameBlocks] = step;
      for (size_t b = first; b < end; ++b) {
        const bool own = OwnStepBytes(b) < SharedStepBytes(b, step);
        if (!own) {
          _base[b] = step;
        }
        _bits[b] = static_cast<uint8_t>(
          std::bit_width(static_cast<V>(_top[b] - _base[b])) |
          (own ? kBlockOwnStep : 0));
        deltas[b * numeric::kBlockValues] = _base[b];
      }
    }
  }

  void Append(std::span<const V> v, size_t first, size_t count,
              std::string& out) {
    _scratch.resize(numeric::kBlockValues);
    const size_t at = out.size();
    out.resize_and_overwrite(at + Bytes(first, count), [&](char* buf,
                                                           size_t size) {
      auto* bases = reinterpret_cast<uint8_t*>(buf + at);
      auto* widths = bases + count * sizeof(V);
      auto* packed = bases + BlockMetaBytes(count, sizeof(V));
      std::fill(widths + count, packed, uint8_t{0});
      for (size_t b = 0; b < count; ++b) {
        const size_t g = first + b;
        duckdb::Store(_first.empty() ? _base[g] : _first[g],
                      bases + b * sizeof(V));
        widths[b] = _bits[g];
        const unsigned bits = _bits[g] & ~kBlockOwnStep;
        if (_bits[g] & kBlockOwnStep) {
          duckdb::Store(uint64_t{_base[g]}, packed);
          packed += sizeof(uint64_t);
        }
        const size_t begin = g * numeric::kBlockValues;
        const size_t n = std::min(numeric::kBlockValues, v.size() - begin);
        const V* in = v.data() + begin;
        if (n < numeric::kBlockValues) {
          std::memcpy(_scratch.data(), in, n * sizeof(V));
          std::fill_n(_scratch.data() + n, numeric::kBlockValues - n, _base[g]);
          in = _scratch.data();
        }
        numeric::Pack<V>(
          bits, in, reinterpret_cast<numeric::LaneWord<V>*>(packed), _base[g]);
        packed += numeric::PackedBytes(bits);
      }
      return size;
    });
  }

 private:
  using S = std::make_signed_t<V>;

  static constexpr size_t kFrameBlocks = kFforFrameRows / numeric::kBlockValues;
  static constexpr uint64_t kNoShare = std::numeric_limits<uint64_t>::max();

  static uint64_t StoredBytes(uint8_t bits) noexcept {
    return bits & kBlockOwnStep
             ? sizeof(uint64_t) + numeric::PackedBytes(bits ^ kBlockOwnStep)
             : numeric::PackedBytes(bits);
  }

  uint64_t OwnStepBytes(size_t b) const noexcept {
    return sizeof(uint64_t) + numeric::PackedBytes(std::bit_width(
                                static_cast<V>(_top[b] - _base[b])));
  }

  uint64_t SharedStepBytes(size_t b, V step) const noexcept {
    return static_cast<S>(_base[b]) < static_cast<S>(step)
             ? kNoShare
             : numeric::PackedBytes(
                 std::bit_width(static_cast<V>(_top[b] - step)));
  }

  V LeastBytesStep(size_t first, size_t end) const noexcept {
    V best = _base[first];
    uint64_t least = kNoShare;
    for (size_t c = first; c < end; ++c) {
      uint64_t bytes = 0;
      for (size_t b = first; b < end; ++b) {
        bytes += std::min(OwnStepBytes(b), SharedStepBytes(b, _base[c]));
      }
      if (bytes < least) {
        least = bytes;
        best = _base[c];
      }
    }
    return best;
  }

  std::vector<V> _base;
  std::vector<V> _top;
  std::vector<V> _first;
  std::vector<V> _steps;
  std::vector<uint8_t> _bits;
  ScratchVector<V> _scratch;
};

}  // namespace irs::codecs
