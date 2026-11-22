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
#include <absl/numeric/int128.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <numeric>
#include <span>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/bitpacking_blocks.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"

namespace irs::codecs::numeric {

template<typename T>
class Values final : public State {
 public:
  static constexpr StateId kId = StateId::NumericValues;

  using U = Bits<sizeof(T)>;

  struct Reference {
    U lo = 0;
    uint8_t stored = 0;
  };

  explicit Values(ColumnDataCheckpointData& data) : _chunks{data.chunks} {
    if (!ReadInPlace()) {
      Gather(data.rows);
    }
    Measure();
  }

  Values(ColumnDataCheckpointData& data, std::span<const U> run_values,
         std::span<const uint32_t> run_rows)
    : _chunks{data.chunks} {
    if (!ReadInPlace()) {
      Gather(data.rows);
    }
    MeasureRuns(run_values, run_rows);
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

  bool Usable() const noexcept { return _any_valid; }

  uint32_t RunCount() const noexcept { return _runs; }

  bool Sequence() const noexcept { return _sequence; }

  BlockRows<U> Raw() const noexcept {
    return _values.size() == _size ? BlockRows<U>{_values}
                                   : BlockRows<U>{_blocks.data(), _size};
  }

  std::span<const BlockBounds<U>> RawBounds() const noexcept { return _bounds; }

  std::span<const uint8_t> NullBlocks() const noexcept { return _null_blocks; }

  std::span<const U> Contiguous() {
    if (_values.size() != _size) {
      Own();
    }
    return _values;
  }

  U Scale() noexcept {
    if (_scale == 0) {
      _scale = FoundScale();
    }
    return _scale;
  }

  U Scale(std::span<const U> every_value) noexcept {
    if (_scale == 0) {
      _scale = _sequence ? FoundScale() : ScaleOf(every_value);
    }
    return _scale;
  }

  U Step() noexcept {
    return _size < 2 ? U{0}
                     : static_cast<U>(Divided(static_cast<U>(_first + _step)) -
                                      Divided(_first));
  }

  BlockRows<U> Rows() {
    if (Scale() == 1) {
      return Raw();
    }
    if (_divided.empty()) {
      const auto rows = Raw();
      _divided.resize(_size);
      for (size_t b = 0; b < rows.Blocks(); ++b) {
        DivideExact<kSigned>(rows.Block(b), rows.BlockSize(b), _scale,
                             _divided.data() + b * kBlockValues);
      }
    }
    return BlockRows<U>{std::span<const U>{_divided}};
  }

  std::span<const U> Get() {
    if (Scale() > 1) {
      Rows();
      return _divided;
    }
    return Contiguous();
  }

  std::span<const BlockBounds<U>> Bounds() {
    if (Scale() == 1) {
      return _bounds;
    }
    if (_divided_bounds.empty()) {
      _divided_bounds.reserve(_bounds.size());
      for (const auto& bounds : _bounds) {
        _divided_bounds.emplace_back(Divided(bounds));
      }
    }
    return _divided_bounds;
  }

  BlockBounds<U> Bounds(size_t first, size_t end) noexcept {
    const auto rows = Raw();
    BlockBounds<U> bounds;
    for (size_t row = first; row < end;) {
      const size_t block = row / kBlockValues;
      const size_t stop = std::min(end, (block + 1) * kBlockValues);
      const auto part =
        row % kBlockValues == 0 && stop - row == rows.BlockSize(block)
          ? _bounds[block]
          : BoundsOf<kSigned>(rows.Block(block) + row % kBlockValues,
                              stop - row);
      if (row == first || Less(part.lo, bounds.lo)) {
        bounds.lo = part.lo;
      }
      if (row == first || Less(bounds.hi, part.hi)) {
        bounds.hi = part.hi;
      }
      row = stop;
    }
    return Divided(bounds);
  }

  Reference GetReference() noexcept {
    const auto bounds = Divided(BlockBounds<U>{_lo, _hi});
    const auto range = static_cast<U>(bounds.hi - bounds.lo);
    return {
      .lo = bounds.lo,
      .stored = BytesFor(range),
    };
  }

  Features GetFeatures() {
    if (!_any_valid) {
      return {};
    }
    uint64_t bits = 0;
    const auto bounds = Bounds();
    for (const auto& block : bounds) {
      bits += std::bit_width(static_cast<U>(block.hi - block.lo));
    }
    return {
      static_cast<float>(bits) / static_cast<float>(bounds.size()),
      static_cast<float>(_runs) / static_cast<float>(_size),
      static_cast<float>(GetReference().stored),
    };
  }

  duckdb::BaseStatistics Stats(const duckdb::LogicalType& type) const {
    auto stats = duckdb::BaseStatistics::CreateEmpty(type);
    if (stats.GetStatsType() == duckdb::StatisticsType::NUMERIC_STATS) {
      _stats.Merge(stats);
    }
    return stats;
  }

 private:
  bool ReadInPlace() {
    if constexpr (std::is_integral_v<T>) {
      size_t rows = 0;
      for (size_t c = 0; c < _chunks.size(); ++c) {
        const auto& input = _chunks[c].data;
        if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
            !duckdb::FlatVector::Validity(input).CheckAllValid(input.size()) ||
            (c + 1 != _chunks.size() && input.size() % kBlockValues != 0)) {
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
        for (size_t at = 0; at < chunk.data.size(); at += kBlockValues) {
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
    if (!_null_blocks.empty()) {
      _null_blocks.resize((_size + kBlockValues - 1) / kBlockValues);
    }
  }

  void MarkNulls(size_t row, size_t end) {
    _null_blocks.resize(
      std::max(_null_blocks.size(), (end - 1) / kBlockValues + 1));
    for (size_t block = row / kBlockValues; block <= (end - 1) / kBlockValues;
         ++block) {
      _null_blocks[block] = 1;
    }
  }

  void MarkNulls(const duckdb::ValidityMask& validity, size_t at,
                 size_t count) {
    constexpr size_t kWord = duckdb::ValidityMask::BITS_PER_VALUE;
    const auto* words = validity.GetData();
    for (size_t row = 0; row < count;) {
      const size_t stop =
        std::min(count, row + kBlockValues - (at + row) % kBlockValues);
      bool null = false;
      size_t i = row;
      for (; i % kWord != 0 && i < stop; ++i) {
        null |= !validity.RowIsValidUnsafe(i);
      }
      for (; i + kWord <= stop; i += kWord) {
        null |=
          words[i / kWord] != duckdb::ValidityMask::ValidityBuffer::MAX_ENTRY;
      }
      for (; i < stop; ++i) {
        null |= !validity.RowIsValidUnsafe(i);
      }
      if (null) {
        MarkNulls(at + row, at + stop);
      }
      row = stop;
    }
  }

  void Own() {
    if (!_owned.empty()) {
      return;
    }
    _owned.reserve(_size);
    const auto rows = Raw();
    for (size_t b = 0; b < rows.Blocks(); ++b) {
      _owned.insert(_owned.end(), rows.Block(b),
                    rows.Block(b) + rows.BlockSize(b));
    }
    _values = _owned;
  }

  static bool EveryStepIs(const U* values, size_t count, U before,
                          U step) noexcept {
    auto other = static_cast<U>(static_cast<U>(values[0] - before) ^ step);
    for (size_t i = 1; i < count; ++i) {
      other = static_cast<U>(
        other | (static_cast<U>(values[i] - values[i - 1]) ^ step));
    }
    return other == 0;
  }

  bool Descends() const noexcept {
    return static_cast<std::make_signed_t<U>>(_step) < 0;
  }

  bool Monotone() const noexcept {
    const auto wide = [](U v) {
      return kSigned ? absl::int128{static_cast<std::make_signed_t<U>>(v)}
                     : absl::int128{v};
    };
    return _first == (Descends() ? _hi : _lo) &&
           wide(_hi) - wide(_lo) ==
             absl::int128{StepMagnitude()} * absl::int128{_size - 1};
  }

  void Measure() {
    if (!_any_valid) {
      return;
    }
    const auto rows = Raw();
    _bounds.resize(rows.Blocks());
    _first = rows[0];
    _step = _size < 2 ? U{0} : static_cast<U>(rows[1] - rows[0]);
    bool stepped = true;
    uint32_t runs = 0;
    auto last = static_cast<U>(_first - _step);
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
      stepped = stepped && EveryStepIs(values, count, last, _step);
      last = values[count - 1];
      if (b == 0 || Less(bounds.lo, _lo)) {
        _lo = bounds.lo;
      }
      if (b == 0 || Less(_hi, bounds.hi)) {
        _hi = bounds.hi;
      }
      if constexpr (std::is_floating_point_v<T>) {
        MeasureFloats(values, count);
      }
    }
    _runs = runs;
    _sequence = stepped && Monotone();
    if constexpr (std::is_integral_v<T>) {
      _stats.Update(std::bit_cast<T>(_lo));
      _stats.Update(std::bit_cast<T>(_hi));
    }
  }

  void MeasureRuns(std::span<const U> run_values,
                   std::span<const uint32_t> run_rows) {
    const size_t count = run_rows.size();
    const U* values = run_values.data();
    _bounds.resize((_size + kBlockValues - 1) / kBlockValues);
    size_t r = 0;
    for (size_t b = 0; b < _bounds.size(); ++b) {
      const size_t first = b * kBlockValues;
      const size_t end = std::min(_size, first + kBlockValues);
      while (r + 1 < count && run_rows[r + 1] <= first) {
        ++r;
      }
      BlockBounds<U> bounds{values[r], values[r]};
      for (; r + 1 < count && run_rows[r + 1] < end; ++r) {
        const U value = values[r + 1];
        bounds.lo = Less(value, bounds.lo) ? value : bounds.lo;
        bounds.hi = Less(bounds.hi, value) ? value : bounds.hi;
      }
      _bounds[b] = bounds;
      if (b == 0 || Less(bounds.lo, _lo)) {
        _lo = bounds.lo;
      }
      if (b == 0 || Less(_hi, bounds.hi)) {
        _hi = bounds.hi;
      }
    }
    _first = values[0];
    _step =
      count > 1 && run_rows[1] == 1 ? static_cast<U>(values[1] - _first) : U{0};
    bool stepped = count == 1 || count == _size;
    for (size_t i = 1; i < count && count == _size && stepped; ++i) {
      stepped = static_cast<U>(values[i] - values[i - 1]) == _step;
    }
    _runs = static_cast<uint32_t>(count);
    _sequence = stepped && Monotone();
    if constexpr (std::is_integral_v<T>) {
      _stats.Update(std::bit_cast<T>(_lo));
      _stats.Update(std::bit_cast<T>(_hi));
    }
  }

  void MeasureFloats(const U* values, size_t count) {
    constexpr size_t kLanes = 8;
    constexpr auto kSign = static_cast<U>(U{1} << (8 * sizeof(U) - 1));
    constexpr auto kInfinity =
      std::bit_cast<U>(std::numeric_limits<T>::infinity());
    T lo[kLanes];
    T hi[kLanes];
    std::fill_n(lo, kLanes, std::numeric_limits<T>::infinity());
    std::fill_n(hi, kLanes, -std::numeric_limits<T>::infinity());
    size_t i = 0;
    for (; i + kLanes <= count; i += kLanes) {
      for (size_t k = 0; k < kLanes; ++k) {
        const auto v = std::bit_cast<T>(values[i + k]);
        lo[k] = v < lo[k] ? v : lo[k];
        hi[k] = v > hi[k] ? v : hi[k];
      }
    }
    for (; i < count; ++i) {
      const auto v = std::bit_cast<T>(values[i]);
      lo[0] = v < lo[0] ? v : lo[0];
      hi[0] = v > hi[0] ? v : hi[0];
    }
    for (size_t k = 1; k < kLanes; ++k) {
      lo[0] = lo[k] < lo[0] ? lo[k] : lo[0];
      hi[0] = hi[k] > hi[0] ? hi[k] : hi[0];
    }
    bool nan = false;
    bool negative_zero = false;
    bool positive_zero = false;
    for (size_t j = 0; j < count; ++j) {
      const U bits = values[j];
      nan |= static_cast<U>(bits & static_cast<U>(~kSign)) > kInfinity;
      negative_zero |= bits == kSign;
      positive_zero |= bits == 0;
    }
    if (lo[0] <= hi[0]) {
      _stats.Update(lo[0]);
      _stats.Update(hi[0]);
    }
    if (negative_zero) {
      _stats.Update(std::bit_cast<T>(kSign));
    }
    if (positive_zero) {
      _stats.Update(T{0});
    }
    if (nan) {
      size_t j = 0;
      while (static_cast<U>(values[j] & static_cast<U>(~kSign)) <= kInfinity) {
        ++j;
      }
      _stats.Update(std::bit_cast<T>(values[j]));
    }
  }

  bool AddFlat(const duckdb::Vector& input) {
    if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
      return false;
    }
    const size_t count = input.size();
    const T* data = duckdb::FlatVector::GetData<T>(input);
    const auto& validity = duckdb::FlatVector::Validity(input);
    if (validity.CheckAllValid(count)) {
      if (count == 0) {
        return true;
      }
      if (!_any_valid) {
        absl::c_fill(_owned, std::bit_cast<U>(data[0]));
        _any_valid = true;
      }
      const size_t at = _owned.size();
      _owned.resize(at + count);
      std::memcpy(_owned.data() + at, data, count * sizeof(T));
      _last = _owned.back();
      return true;
    }
    _stats.SetHasNull();
    if (!_any_valid) {
      size_t first = 0;
      while (first < count && !validity.RowIsValidUnsafe(first)) {
        ++first;
      }
      if (first == count) {
        MarkNulls(_owned.size(), _owned.size() + count);
        _owned.insert(_owned.end(), count, _last);
        return true;
      }
      _last = std::bit_cast<U>(data[first]);
      absl::c_fill(_owned, _last);
      _any_valid = true;
    }
    const size_t at = _owned.size();
    MarkNulls(validity, at, count);
    _owned.resize(at + count);
    U last = _last;
    for (size_t i = 0; i < count; ++i) {
      last = validity.RowIsValidUnsafe(i) ? std::bit_cast<U>(data[i]) : last;
      _owned[at + i] = last;
    }
    _last = last;
    return true;
  }

  void Add(const duckdb::Vector& input) {
    if (AddFlat(input)) {
      return;
    }
    for (const auto value : input.Values<T>()) {
      if (!value.IsValid()) {
        _stats.SetHasNull();
        MarkNulls(_owned.size(), _owned.size() + 1);
        _owned.push_back(_last);
        continue;
      }
      const auto bits = std::bit_cast<U>(value.GetValue());
      if (!_any_valid) {
        absl::c_fill(_owned, bits);
        _any_valid = true;
      }
      _last = bits;
      _owned.push_back(bits);
    }
  }

  static constexpr size_t kScaleSeeds = 8;

  U StepMagnitude() const noexcept {
    return Descends() ? static_cast<U>(U{0} - _step) : _step;
  }

  static U Magnitude(U bits) noexcept {
    if constexpr (std::is_signed_v<T>) {
      if (static_cast<T>(bits) < 0) {
        return static_cast<U>(U{0} - bits);
      }
    }
    return bits;
  }

  static bool Multiples(const U* values, size_t count, U divisor) noexcept {
    const auto shift = std::countr_zero(divisor);
    const auto odd = static_cast<U>(divisor >> shift);
    const U inverse = OddInverse(odd);
    const auto most = static_cast<U>(std::numeric_limits<U>::max() / odd);
    const auto low = static_cast<U>((U{1} << shift) - 1);
    U other = 0;
    for (size_t i = 0; i < count; ++i) {
      const U magnitude = Magnitude(values[i]);
      const auto quotient = static_cast<U>(
        static_cast<WideWord<U>>(static_cast<U>(magnitude >> shift)) *
        static_cast<WideWord<U>>(inverse));
      other = static_cast<U>(other | (magnitude & low) | U{quotient > most});
    }
    return other == 0;
  }

  static U Dividing(U gcd, const U* values, size_t count) noexcept {
    if (Multiples(values, count, gcd)) {
      return gcd;
    }
    for (size_t i = 0; i < count && gcd != 1; ++i) {
      gcd = std::gcd(gcd, Magnitude(values[i]));
    }
    return gcd;
  }

  static U ScaleOf(std::span<const U> values) noexcept {
    U gcd = 0;
    if constexpr (std::is_integral_v<T>) {
      size_t at = 0;
      for (; at < values.size() && at < kScaleSeeds; ++at) {
        gcd = std::gcd(gcd, Magnitude(values[at]));
      }
      for (; at < values.size() && gcd > 1; at += kBlockValues) {
        gcd = Dividing(gcd, values.data() + at,
                       std::min(kBlockValues, values.size() - at));
      }
    }
    return gcd > 1 ? gcd : U{1};
  }

  U FoundScale() const noexcept {
    U gcd = 0;
    if constexpr (std::is_integral_v<T>) {
      if (_sequence) {
        gcd = std::gcd(Magnitude(_first), StepMagnitude());
      } else {
        const std::span<const BlockBounds<U>> bounds{_bounds};
        for (const auto& block : bounds) {
          gcd =
            std::gcd(gcd, std::gcd(Magnitude(block.lo), Magnitude(block.hi)));
          if (gcd == 1) {
            break;
          }
        }
        const auto rows = Raw();
        for (size_t b = 0; b < rows.Blocks() && gcd > 1; ++b) {
          if (bounds[b].lo != bounds[b].hi) {
            gcd = Dividing(gcd, rows.Block(b), rows.BlockSize(b));
          }
        }
      }
    }
    return gcd > 1 ? gcd : U{1};
  }

  U Divided(U value) noexcept {
    if (const auto scale = Scale(); scale > 1) {
      DivideExact<kSigned>(&value, 1, scale, &value);
    }
    return value;
  }

  BlockBounds<U> Divided(BlockBounds<U> bounds) noexcept {
    return {Divided(bounds.lo), Divided(bounds.hi)};
  }

  std::span<const WriteChunk> _chunks;
  duckdb::StatsWriter<T> _stats;
  std::vector<const U*> _blocks;
  std::vector<BlockBounds<U>> _bounds;
  std::vector<uint8_t> _null_blocks;
  ScratchVector<U> _owned;
  std::span<const U> _values;
  size_t _size = 0;
  U _lo = 0;
  U _hi = 0;
  U _first = 0;
  U _step = 0;
  U _last = 0;
  uint32_t _runs = 0;
  bool _any_valid = false;
  bool _sequence = false;
  std::vector<BlockBounds<U>> _divided_bounds;
  ScratchVector<U> _divided;
  U _scale = 0;
};

}  // namespace irs::codecs::numeric
