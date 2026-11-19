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
#include <bit>
#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <numeric>
#include <optional>
#include <span>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/ffor_blocks.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"

namespace irs::codecs {

template<typename T>
class Values final : public State {
 public:
  static constexpr StateId kId = StateId::NumericValues;

  using U = numeric::Bits<sizeof(T)>;

  struct Facts {
    U lo = 0;
    uint8_t for_stored = 0;
    uint8_t for_bits = 0;
  };

  struct Steps {
    U offset = 0;
    uint8_t stored = 0;
    uint8_t bits = 0;
  };

  explicit Values(ColumnDataCheckpointData& data) : _chunks{data.chunks} {
    if (!ReadInPlace()) {
      Gather(data.rows);
    }
    Measure();
    DivideByGcd();
  }

  static constexpr bool kSigned = std::is_integral_v<T> && std::is_signed_v<T>;

  static bool Less(U a, U b) noexcept {
    if constexpr (kSigned) {
      return static_cast<T>(a) < static_cast<T>(b);
    } else {
      return a < b;
    }
  }

  size_t Size() const noexcept { return _size; }

  BlockRows<U> Rows() const noexcept {
    return _values.size() == _size ? BlockRows<U>{_values}
                                   : BlockRows<U>{_blocks.data(), _size};
  }

  std::span<const BlockBounds<U>> Bounds() const noexcept { return _bounds; }

  BlockBounds<U> Bounds(size_t first, size_t end) const noexcept {
    const auto rows = Rows();
    BlockBounds<U> bounds{rows[first], rows[first]};
    for (size_t row = first; row < end;) {
      const size_t block = row / numeric::kBlockValues;
      const size_t stop = std::min(end, (block + 1) * numeric::kBlockValues);
      const auto part =
        row % numeric::kBlockValues == 0 && stop - row == rows.BlockSize(block)
          ? _bounds[block]
          : BoundsOf<kSigned>(rows.Block(block) + row % numeric::kBlockValues,
                              stop - row);
      if (Less(part.lo, bounds.lo)) {
        bounds.lo = part.lo;
      }
      if (Less(bounds.hi, part.hi)) {
        bounds.hi = part.hi;
      }
      row = stop;
    }
    return bounds;
  }

  std::span<const U> Get() {
    if (_values.size() != _size) {
      Own();
    }
    return _values;
  }

  bool Usable() const noexcept { return _any_valid; }

  U Scale() const noexcept { return _scale; }

  uint64_t PackedBytes() const noexcept {
    return _any_valid ? (uint64_t{_size} * GetFacts().for_bits + 7) / 8 : 0;
  }

  duckdb::BaseStatistics Stats(const duckdb::LogicalType& type) const {
    auto stats = duckdb::BaseStatistics::CreateEmpty(type);
    if (stats.GetStatsType() == duckdb::StatisticsType::NUMERIC_STATS) {
      _stats.Merge(stats);
    }
    return stats;
  }

  Facts GetFacts() const noexcept {
    const auto range = static_cast<U>(_hi - _lo);
    return {
      .lo = _lo,
      .for_stored = numeric::BytesFor(range),
      .for_bits = static_cast<uint8_t>(std::bit_width(range)),
    };
  }

  uint32_t RunCount() const noexcept { return _runs; }

  const Steps& GetSteps() {
    if (_steps) {
      return *_steps;
    }
    using S = std::make_signed_t<U>;
    S lo = std::numeric_limits<S>::max();
    S hi = std::numeric_limits<S>::min();
    const auto fold = [&](U step) {
      lo = std::min(lo, static_cast<S>(step));
      hi = std::max(hi, static_cast<S>(step));
    };
    const auto rows = Rows();
    U last = 0;
    for (size_t b = 0; b < rows.Blocks(); ++b) {
      const U* values = rows.Block(b);
      const size_t count = rows.BlockSize(b);
      if (b != 0) {
        fold(static_cast<U>(values[0] - last));
      }
      for (size_t i = 1; i < count; ++i) {
        fold(static_cast<U>(values[i] - values[i - 1]));
      }
      last = values[count - 1];
    }
    if (_size < 2) {
      lo = 0;
      hi = 0;
    }
    const auto offset = static_cast<U>(lo);
    const auto range = static_cast<U>(static_cast<U>(hi) - offset);
    return _steps.emplace(Steps{
      .offset = offset,
      .stored = numeric::BytesFor(range),
      .bits = static_cast<uint8_t>(std::bit_width(range)),
    });
  }

 private:
  bool ReadInPlace() {
    if constexpr (std::is_integral_v<T>) {
      size_t rows = 0;
      for (size_t c = 0; c < _chunks.size(); ++c) {
        const auto& input = _chunks[c].data;
        if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
            !duckdb::FlatVector::Validity(input).CheckAllValid(input.size()) ||
            (c + 1 != _chunks.size() &&
             input.size() % numeric::kBlockValues != 0)) {
          return false;
        }
        rows += input.size();
      }
      for (const auto& chunk : _chunks) {
        const auto* bits = reinterpret_cast<const U*>(
          duckdb::FlatVector::GetData<T>(chunk.data));
        if (_chunks.size() == 1) {
          _values = {bits, rows};
          break;
        }
        for (size_t at = 0; at < chunk.data.size();
             at += numeric::kBlockValues) {
          _blocks.push_back(bits + at);
        }
      }
      _size = rows;
      _any_valid = rows != 0;
      return true;
    } else {
      return false;
    }
  }

  void Gather(uint64_t rows) {
    _owned.reserve(rows);
    for (const auto& chunk : _chunks) {
      Add(chunk.data);
    }
    _values = _owned;
    _size = _owned.size();
  }

  void Own() {
    if (!_owned.empty()) {
      return;
    }
    _owned.reserve(_size);
    const auto rows = Rows();
    for (size_t b = 0; b < rows.Blocks(); ++b) {
      _owned.insert(_owned.end(), rows.Block(b),
                    rows.Block(b) + rows.BlockSize(b));
    }
    _values = _owned;
  }

  void Measure() {
    if (!_any_valid) {
      return;
    }
    const auto rows = Rows();
    _bounds.resize(rows.Blocks());
    uint32_t runs = 0;
    U last = 0;
    for (size_t b = 0; b < rows.Blocks(); ++b) {
      const U* values = rows.Block(b);
      const size_t count = rows.BlockSize(b);
      const auto bounds = BoundsOf<kSigned>(values, count);
      _bounds[b] = bounds;
      runs += b == 0 || values[0] != last;
      if (bounds.lo != bounds.hi) {
        uint32_t changes = 0;
        for (size_t i = 1; i < count; ++i) {
          changes += values[i] != values[i - 1];
        }
        runs += changes;
      }
      last = values[count - 1];
      if (b == 0 || Less(bounds.lo, _lo)) {
        _lo = bounds.lo;
      }
      if (b == 0 || Less(_hi, bounds.hi)) {
        _hi = bounds.hi;
      }
    }
    _runs = runs;
    if (_owned.empty()) {
      _stats.Update(std::bit_cast<T>(_lo));
      _stats.Update(std::bit_cast<T>(_hi));
    }
  }

  bool AddBlock(const duckdb::Vector& input) {
    if constexpr (std::is_integral_v<T>) {
      if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
          !duckdb::FlatVector::Validity(input).CheckAllValid(input.size())) {
        return false;
      }
      const std::span values{duckdb::FlatVector::GetData<T>(input),
                             input.size()};
      if (values.empty()) {
        return true;
      }
      const auto [lo, hi] = std::ranges::minmax(values);
      _stats.Update(lo);
      _stats.Update(hi);
      const auto* bits = reinterpret_cast<const U*>(values.data());
      if (!_any_valid) {
        absl::c_fill(_owned, bits[0]);
        _any_valid = true;
      }
      _owned.insert(_owned.end(), bits, bits + values.size());
      _last = _owned.back();
      return true;
    } else {
      return false;
    }
  }

  void Add(const duckdb::Vector& input) {
    if (AddBlock(input)) {
      return;
    }
    auto stats = _stats;
    for (const auto value : input.Values<T>()) {
      if (!value.IsValid()) {
        stats.SetHasNull();
        _owned.push_back(_last);
        continue;
      }
      const T v = value.GetValue();
      stats.Update(v);
      const auto bits = std::bit_cast<U>(v);
      if (!_any_valid) {
        absl::c_fill(_owned, bits);
        _any_valid = true;
      }
      _last = bits;
      _owned.push_back(bits);
    }
    _stats = stats;
  }

  static U Magnitude(U bits) noexcept {
    if constexpr (std::is_signed_v<T>) {
      if (static_cast<T>(bits) < 0) {
        return static_cast<U>(U{0} - bits);
      }
    }
    return bits;
  }

  void DivideByGcd() {
    if constexpr (std::is_integral_v<T>) {
      U gcd = 0;
      for (const auto& bounds : _bounds) {
        gcd =
          std::gcd(gcd, std::gcd(Magnitude(bounds.lo), Magnitude(bounds.hi)));
        if (gcd == 1) {
          break;
        }
      }
      if (gcd > 1) {
        const auto rows = Rows();
        U folded = 0;
        for (size_t b = 0; b < rows.Blocks() && gcd != 1; ++b) {
          if (_bounds[b].lo == _bounds[b].hi) {
            continue;
          }
          const U* values = rows.Block(b);
          const size_t count = rows.BlockSize(b);
          for (size_t i = 0; i < count && gcd != 1; ++i) {
            if (values[i] == folded) {
              continue;
            }
            folded = values[i];
            gcd = std::gcd(gcd, Magnitude(folded));
          }
        }
      }
      _scale = gcd > 1 ? gcd : 1;
      if (_scale > 1) {
        Own();
        constexpr bool kSigned = std::is_signed_v<T>;
        numeric::DivideExact<kSigned>(_owned.data(), _owned.size(), _scale);
        for (auto& bounds : _bounds) {
          numeric::DivideExact<kSigned>(&bounds.lo, 1, _scale);
          numeric::DivideExact<kSigned>(&bounds.hi, 1, _scale);
        }
        numeric::DivideExact<kSigned>(&_lo, 1, _scale);
        numeric::DivideExact<kSigned>(&_hi, 1, _scale);
      }
    }
  }

  std::span<const WriteChunk> _chunks;
  duckdb::StatsWriter<T> _stats;
  std::vector<const U*> _blocks;
  std::vector<BlockBounds<U>> _bounds;
  std::vector<U> _owned;
  std::span<const U> _values;
  std::optional<Steps> _steps;
  size_t _size = 0;
  U _lo = 0;
  U _hi = 0;
  U _last = 0;
  U _scale = 0;
  uint32_t _runs = 0;
  bool _any_valid = false;
};

}  // namespace irs::codecs
