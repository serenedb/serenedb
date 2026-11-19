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
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

template<typename V>
class BlockRows {
 public:
  BlockRows() = default;

  explicit BlockRows(std::span<const V> rows) noexcept
    : _base{rows.data()}, _size{rows.size()} {}

  BlockRows(const V* const* blocks, size_t size) noexcept
    : _blocks{blocks}, _size{size} {}

  size_t size() const noexcept { return _size; }

  size_t Blocks() const noexcept {
    return (_size + numeric::kBlockValues - 1) / numeric::kBlockValues;
  }

  size_t BlockSize(size_t block) const noexcept {
    return std::min(numeric::kBlockValues,
                    _size - block * numeric::kBlockValues);
  }

  const V* Block(size_t block) const noexcept {
    return _base ? _base + block * numeric::kBlockValues : _blocks[block];
  }

  V operator[](size_t row) const noexcept {
    return Block(row / numeric::kBlockValues)[row % numeric::kBlockValues];
  }

 private:
  const V* _base = nullptr;
  const V* const* _blocks = nullptr;
  size_t _size = 0;
};

template<typename V>
struct BlockBounds {
  V lo = 0;
  V hi = 0;
};

template<typename V>
struct BlockBases {
  V origin = 0;
  uint8_t bytes = sizeof(V);
};

template<bool kSigned, typename V>
BlockBounds<V> BoundsOf(const V* values, size_t count) noexcept {
  using Ordered = std::conditional_t<kSigned, std::make_signed_t<V>, V>;
  auto lo = static_cast<Ordered>(values[0]);
  auto hi = lo;
  for (size_t i = 1; i < count; ++i) {
    lo = std::min(lo, static_cast<Ordered>(values[i]));
    hi = std::max(hi, static_cast<Ordered>(values[i]));
  }
  return {static_cast<V>(lo), static_cast<V>(hi)};
}

template<typename V>
class FforBlocks {
 public:
  template<bool kSigned>
  void Plan(BlockRows<V> rows) {
    Resize(rows.size());
    for (size_t b = 0; b < _bits.size(); ++b) {
      const auto [lo, hi] = BoundsOf<kSigned>(rows.Block(b), rows.BlockSize(b));
      _base[b] = lo;
      _bits[b] = static_cast<uint8_t>(std::bit_width(static_cast<V>(hi - lo)));
    }
  }

  void Plan(size_t rows, std::span<const BlockBounds<V>> bounds) {
    Resize(rows);
    SDB_ASSERT(bounds.size() == _bits.size());
    for (size_t b = 0; b < _bits.size(); ++b) {
      _base[b] = bounds[b].lo;
      _bits[b] = static_cast<uint8_t>(
        std::bit_width(static_cast<V>(bounds[b].hi - bounds[b].lo)));
    }
  }

  uint64_t Bytes(size_t first, size_t count, size_t base_bytes) const noexcept {
    uint64_t bytes = BlockMetaBytes(count, base_bytes);
    for (size_t b = first; b < first + count; ++b) {
      bytes += StoredBytes(b);
    }
    return bytes;
  }

  uint64_t Bytes(uint32_t frame_values, size_t base_bytes) const noexcept {
    const size_t per = frame_values / numeric::kBlockValues;
    uint64_t bytes = 0;
    for (size_t first = 0; first < _bits.size(); first += per) {
      bytes += Bytes(first, std::min(per, _bits.size() - first), base_bytes);
    }
    return bytes;
  }

  uint64_t StreamBytes(uint32_t frame_values) const noexcept {
    const size_t per = frame_values / numeric::kBlockValues;
    uint64_t bytes = 0;
    for (size_t first = 0; first < _bits.size(); first += per) {
      const auto count = std::min(per, _bits.size() - first);
      bytes += BlockMetaBytes(count, sizeof(V));
      for (size_t b = first; b < first + count; ++b) {
        bytes += StreamBytes(b);
      }
    }
    return bytes;
  }

  template<bool kSigned>
  void PlanStream(BlockRows<V> rows) {
    Resize(rows.size());
    _first.resize(_bits.size());
    for (size_t b = 0; b < _bits.size(); ++b) {
      const V* values = rows.Block(b);
      const size_t count = rows.BlockSize(b);
      const auto [lo, hi] = BoundsOf<kSigned>(values, count);
      _first[b] = lo;
      _base[b] = lo;
      _bits[b] = static_cast<uint8_t>(std::bit_width(static_cast<V>(hi - lo)));
      const auto offsets = StreamBlockBytes(count, _bits[b]);
      if (count < 2 || offsets <= sizeof(uint64_t)) {
        continue;
      }
      if (Alternates(values, count)) {
        _first[b] = values[0];
        _base[b] = static_cast<V>(values[1] - values[0]);
        _bits[b] = kStreamAlternates;
        continue;
      }
      const auto [least, most] = StepBounds(values, count);
      const unsigned width = std::bit_width(static_cast<V>(most - least));
      if (width <= kBlockDeltaBits &&
          sizeof(uint64_t) + StreamBlockBytes(count, width) < offsets) {
        _first[b] = values[0];
        _base[b] = least;
        _bits[b] = static_cast<uint8_t>(kBlockDelta | width);
      }
    }
  }

  void AppendStream(BlockRows<V> rows, size_t first, size_t count,
                    std::string& out) {
    _scratch.resize(numeric::kBlockValues);
    uint64_t bytes = BlockMetaBytes(count, sizeof(V));
    for (size_t b = first; b < first + count; ++b) {
      bytes += StreamBytes(b);
    }
    const size_t at = out.size();
    out.resize_and_overwrite(at + bytes, [&](char* buf, size_t size) {
      auto* bases = reinterpret_cast<uint8_t*>(buf + at);
      auto* widths = bases + count * sizeof(V);
      auto* packed = bases + BlockMetaBytes(count, sizeof(V));
      std::fill(widths + count, packed, uint8_t{0});
      for (size_t b = 0; b < count; ++b) {
        const size_t g = first + b;
        const uint8_t kind = _bits[g];
        duckdb::Store(_first.empty() ? _base[g] : _first[g],
                      bases + b * sizeof(V));
        widths[b] = kind;
        const size_t n = Values(g);
        const V* in = rows.Block(g);
        unsigned bits = kind;
        if (kind & kBlockDelta) {
          duckdb::Store(uint64_t{_base[g]}, packed);
          packed += sizeof(uint64_t);
          if (kind == kStreamAlternates) {
            continue;
          }
          bits = kind & kBlockDeltaBits;
          if (bits == 0) {
            continue;
          }
          _scratch[0] = _base[g];
          for (size_t i = 1; i < n; ++i) {
            _scratch[i] = static_cast<V>(in[i] - in[i - 1]);
          }
          in = _scratch.data();
        }
        if (n == numeric::kBlockValues) {
          numeric::Pack<V>(bits, in,
                           reinterpret_cast<numeric::LaneWord<V>*>(packed),
                           _base[g]);
        } else if (bits != 0) {
          for (size_t i = 0; i < n; ++i) {
            _scratch[i] = static_cast<V>(in[i] - _base[g]);
          }
          duckdb::BitpackingPrimitives::PackBuffer<V, false>(
            packed, _scratch.data(), n, bits);
        }
        packed += StreamBlockBytes(n, bits);
      }
      return size;
    });
  }

  V FrameStep(size_t first) const noexcept {
    return _steps.empty() ? V{0} : _steps[first / kFrameBlocks];
  }

  bool EqualSteps(size_t first, size_t count) const noexcept {
    if (_steps.empty()) {
      return false;
    }
    const uint64_t step = FrameStep(first);
    for (size_t b = first; b < first + count; ++b) {
      const auto start = static_cast<V>(
        _first[first] + (b - first) * numeric::kBlockValues * step);
      if (_bits[b] != kBlockDelta || _first[b] != start) {
        return false;
      }
    }
    return true;
  }

  static uint64_t SampledDeltaBytes(BlockRows<V> rows) noexcept {
    constexpr size_t kEvery = 8;
    uint64_t sampled = 0;
    size_t samples = 0;
    for (size_t b = 0; b < rows.Blocks(); b += kEvery, ++samples) {
      const auto [lo, hi] = StepBounds(rows.Block(b), rows.BlockSize(b));
      sampled += numeric::PackedBytes(std::bit_width(static_cast<V>(hi - lo)));
    }
    return samples == 0 ? 0 : sampled * rows.Blocks() / samples;
  }

  void PlanEqualSteps(BlockRows<V> rows, V step) {
    PlanEqualSteps(rows.size(), step);
    for (size_t b = 0; b < _first.size(); ++b) {
      _first[b] = rows.Block(b)[0];
    }
  }

  void PlanEqualSteps(size_t rows, std::span<const BlockBounds<V>> bounds,
                      V step) {
    PlanEqualSteps(rows, step);
    SDB_ASSERT(bounds.size() == _first.size());
    for (size_t b = 0; b < _first.size(); ++b) {
      _first[b] = static_cast<S>(step) < 0 ? bounds[b].hi : bounds[b].lo;
    }
  }

  template<bool kSigned>
  void PlanDeltas(BlockRows<V> rows) {
    PlanSteps(rows);
    for (size_t b = 0; b < _bits.size(); ++b) {
      if (DeltaWidth(b) > kBlockDeltaBits) {
        const auto [lo, hi] =
          BoundsOf<kSigned>(rows.Block(b), rows.BlockSize(b));
        PlanFor(b, lo,
                static_cast<uint8_t>(std::bit_width(static_cast<V>(hi - lo))));
      } else {
        PlanDelta(b);
      }
    }
  }

  void PlanMixed(BlockRows<V> rows, std::span<const BlockBounds<V>> bounds,
                 std::span<const uint8_t> offsets_only, double delta_price) {
    if (const V step =
          rows.size() < 2 ? V{0} : static_cast<V>(rows[1] - rows[0]);
        EveryStepIs(step, rows)) {
      PlanEqualSteps(rows, step);
      return;
    }
    Plan(rows.size(), bounds);
    if (!DifferencesNarrower(rows)) {
      return;
    }
    const auto lows = std::exchange(_base, {});
    const auto widths = std::exchange(_bits, {});
    PlanSteps(rows);
    for (size_t b = 0; b < _bits.size(); ++b) {
      const auto for_bytes = numeric::PackedBytes<V>(widths[b], Values(b));
      const auto delta_bytes =
        (_own[b] ? sizeof(uint64_t) : 0) +
        numeric::PackedBytes<V>(DeltaWidth(b), Values(b));
      const bool allowed =
        DeltaWidth(b) == 0 || offsets_only.empty() || offsets_only[b] == 0;
      if (allowed && DeltaWidth(b) <= kBlockDeltaBits &&
          (delta_bytes == 0 || static_cast<double>(delta_bytes) * delta_price <
                                 static_cast<double>(for_bytes))) {
        PlanDelta(b);
      } else {
        PlanFor(b, lows[b], widths[b]);
      }
    }
  }

  void Append(BlockRows<V> rows, size_t first, size_t count,
              BlockBases<V> stored, std::string& out) {
    _scratch.resize(numeric::kBlockValues);
    const size_t at = out.size();
    out.resize_and_overwrite(
      at + Bytes(first, count, stored.bytes), [&](char* buf, size_t size) {
        auto* bases = reinterpret_cast<uint8_t*>(buf + at);
        auto* widths = bases + count * stored.bytes;
        auto* packed = bases + BlockMetaBytes(count, stored.bytes);
        std::fill(widths + count, packed, uint8_t{0});
        for (size_t b = 0; b < count; ++b) {
          const size_t g = first + b;
          const auto base = static_cast<V>(
            (_first.empty() ? _base[g] : _first[g]) - stored.origin);
          SDB_ASSERT(stored.bytes == sizeof(V) ||
                     uint64_t{base} >> (8 * stored.bytes) == 0);
          std::memcpy(bases + b * stored.bytes, &base, stored.bytes);
          widths[b] = _bits[g];
          const unsigned bits = Width(_bits[g]);
          if (OwnStep(_bits[g])) {
            duckdb::Store(uint64_t{_base[g]}, packed);
            packed += sizeof(uint64_t);
          }
          if (bits == 0) {
            continue;
          }
          const size_t n = Values(g);
          const V* in = rows.Block(g);
          if (_bits[g] & kBlockDelta) {
            _scratch[0] = _base[g];
            for (size_t i = 1; i < n; ++i) {
              _scratch[i] = static_cast<V>(in[i] - in[i - 1]);
            }
            in = _scratch.data();
          } else if (n < numeric::kBlockValues) {
            std::memcpy(_scratch.data(), in, n * sizeof(V));
            in = _scratch.data();
          }
          if (n < numeric::kBlockValues) {
            std::fill_n(_scratch.data() + n, numeric::kBlockValues - n,
                        _base[g]);
            _words.resize(numeric::kBlockValues * sizeof(V) /
                          sizeof(numeric::LaneWord<V>));
            numeric::Pack<V>(bits, in, _words.data(), _base[g]);
            std::memcpy(packed, _words.data(),
                        numeric::PackedBytes<V>(bits, n));
          } else {
            numeric::Pack<V>(bits, in,
                             reinterpret_cast<numeric::LaneWord<V>*>(packed),
                             _base[g]);
          }
          packed += numeric::PackedBytes<V>(bits, n);
        }
        return size;
      });
  }

 private:
  using S = std::make_signed_t<V>;

  static constexpr size_t kFrameBlocks = kFforFrameRows / numeric::kBlockValues;
  static constexpr uint64_t kNoShare = std::numeric_limits<uint64_t>::max();

  void Resize(size_t rows) {
    const size_t blocks =
      (rows + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _size = rows;
    _base.resize(blocks);
    _bits.resize(blocks);
  }

  size_t Values(size_t block) const noexcept {
    return std::min(numeric::kBlockValues,
                    _size - block * numeric::kBlockValues);
  }

  static unsigned Width(uint8_t bits) noexcept {
    return bits & kBlockDelta ? bits & kBlockDeltaBits : bits;
  }

  static bool OwnStep(uint8_t bits) noexcept {
    return (bits & (kBlockDelta | kBlockOwnStep)) ==
           (kBlockDelta | kBlockOwnStep);
  }

  static uint64_t StreamBlockBytes(size_t values, unsigned bits) noexcept {
    return values == numeric::kBlockValues
             ? numeric::PackedBytes(bits)
             : duckdb::BitpackingPrimitives::GetRequiredSize(values, bits);
  }

  uint64_t StreamBytes(size_t block) const noexcept {
    const uint8_t kind = _bits[block];
    if (kind == kStreamAlternates) {
      return sizeof(uint64_t);
    }
    if (kind & kBlockDelta) {
      return sizeof(uint64_t) +
             StreamBlockBytes(Values(block), kind & kBlockDeltaBits);
    }
    return StreamBlockBytes(Values(block), kind);
  }

  static bool Alternates(const V* values, size_t count) noexcept {
    if (values[0] == values[1]) {
      return false;
    }
    V other = 0;
    for (size_t i = 2; i < count; ++i) {
      other = static_cast<V>(other | (values[i] ^ values[i - 2]));
    }
    return other == 0;
  }

  uint64_t StoredBytes(size_t block) const noexcept {
    const auto bits = _bits[block];
    return (OwnStep(bits) ? sizeof(uint64_t) : 0) +
           numeric::PackedBytes<V>(Width(bits), Values(block));
  }

  static BlockBounds<V> StepBounds(const V* values, size_t count) noexcept {
    S lo = 0;
    S hi = 0;
    for (size_t i = 1; i < count; ++i) {
      const auto step = static_cast<S>(values[i] - values[i - 1]);
      lo = i == 1 ? step : std::min(lo, step);
      hi = i == 1 ? step : std::max(hi, step);
    }
    return {static_cast<V>(lo), static_cast<V>(hi)};
  }

  void PlanEqualSteps(size_t rows, V step) {
    Resize(rows);
    const size_t blocks = _bits.size();
    _base.assign(blocks, step);
    _bits.assign(blocks, kBlockDelta);
    _steps.assign((blocks + kFrameBlocks - 1) / kFrameBlocks, step);
    _first.resize(blocks);
  }

  void PlanSteps(BlockRows<V> rows) {
    Resize(rows.size());
    const size_t blocks = _bits.size();
    _top.resize(blocks);
    _own.resize(blocks);
    _first.resize(blocks);
    _steps.resize((blocks + kFrameBlocks - 1) / kFrameBlocks);
    for (size_t b = 0; b < blocks; ++b) {
      const V* values = rows.Block(b);
      const auto [lo, hi] = StepBounds(values, rows.BlockSize(b));
      _first[b] = values[0];
      _base[b] = lo;
      _top[b] = hi;
    }
    for (size_t first = 0; first < blocks; first += kFrameBlocks) {
      const size_t end = std::min(blocks, first + kFrameBlocks);
      const V step = LeastBytesStep(first, end);
      _steps[first / kFrameBlocks] = step;
      for (size_t b = first; b < end; ++b) {
        _own[b] = OwnStepBytes(b) < SharedStepBytes(b, step);
        if (!_own[b]) {
          _base[b] = step;
        }
      }
    }
  }

  unsigned DeltaWidth(size_t b) const noexcept {
    return std::bit_width(static_cast<V>(_top[b] - _base[b]));
  }

  void PlanDelta(size_t b) {
    _bits[b] = static_cast<uint8_t>(
      kBlockDelta | (_own[b] ? kBlockOwnStep : 0) | DeltaWidth(b));
  }

  void PlanFor(size_t b, V lo, uint8_t bits) {
    _first[b] = lo;
    _base[b] = lo;
    _bits[b] = bits;
  }

  static bool EveryStepIs(V step, BlockRows<V> rows) noexcept {
    V last = rows.size() == 0 ? V{0} : static_cast<V>(rows[0] - step);
    for (size_t b = 0; b < rows.Blocks(); ++b) {
      const V* values = rows.Block(b);
      const size_t count = rows.BlockSize(b);
      V other = static_cast<V>(static_cast<V>(values[0] - last) ^ step);
      for (size_t i = 1; i < count; ++i) {
        other = static_cast<V>(
          other | (static_cast<V>(values[i] - values[i - 1]) ^ step));
      }
      if (other != 0) {
        return false;
      }
      last = values[count - 1];
    }
    return true;
  }

  bool DifferencesNarrower(BlockRows<V> rows) const noexcept {
    constexpr size_t kEvery = 8;
    for (size_t b = 0; b < _bits.size(); b += kEvery) {
      const auto [lo, hi] = StepBounds(rows.Block(b), rows.BlockSize(b));
      if (std::bit_width(static_cast<V>(hi - lo)) < _bits[b]) {
        return true;
      }
    }
    return false;
  }

  uint64_t OwnStepBytes(size_t b) const noexcept {
    return sizeof(uint64_t) +
           numeric::PackedBytes<V>(
             std::bit_width(static_cast<V>(_top[b] - _base[b])), Values(b));
  }

  uint64_t SharedStepBytes(size_t b, V step) const noexcept {
    return static_cast<S>(_base[b]) < static_cast<S>(step)
             ? kNoShare
             : numeric::PackedBytes<V>(
                 std::bit_width(static_cast<V>(_top[b] - step)), Values(b));
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
  std::vector<uint8_t> _own;
  size_t _size = 0;
  ScratchVector<V> _scratch;
  ScratchVector<numeric::LaneWord<V>> _words;
};

}  // namespace irs::codecs
