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
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"

namespace irs::codecs {

template<typename T>
class Values final : public State {
 public:
  static constexpr StateId kId = StateId::NumericValues;

  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint8_t kWidth = sizeof(T);

  struct Facts {
    U lo = 0;
    U hi = 0;
    U delta_offset = 0;
    uint8_t for_stored = 0;
    uint8_t delta_stored = 0;
    uint8_t for_bits = 0;
    uint8_t delta_bits = 0;
    uint32_t runs = 0;
  };

  explicit Values(ColumnDataCheckpointData& data) {
    if (data.chunks.size() != 1 || !Borrow(data.chunks.front().data)) {
      _owned.reserve(data.rows);
      for (const auto& chunk : data.chunks) {
        Add(chunk.data);
      }
      _values = _owned;
    }
    DivideByGcd();
  }

  static bool Less(U a, U b) noexcept {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      return static_cast<T>(a) < static_cast<T>(b);
    } else {
      return a < b;
    }
  }

  std::span<const U> Get() const noexcept { return _values; }

  bool Usable() const noexcept { return _any_valid; }

  U Scale() const noexcept { return _scale; }

  duckdb::BaseStatistics Stats(const duckdb::LogicalType& type) const {
    auto stats = duckdb::BaseStatistics::CreateEmpty(type);
    _stats.Merge(stats);
    return stats;
  }

  const Facts& GetFacts() {
    if (_facts) {
      return *_facts;
    }
    const auto n = static_cast<uint32_t>(_values.size());
    using S = std::make_signed_t<U>;
    U lo = _values[0];
    U hi = _values[0];
    S dlo = std::numeric_limits<S>::max();
    S dhi = std::numeric_limits<S>::min();
    uint32_t runs = 1;
    for (uint32_t i = 1; i < n; ++i) {
      const U v = _values[i];
      if (Less(v, lo)) {
        lo = v;
      }
      if (Less(hi, v)) {
        hi = v;
      }
      const auto d = static_cast<S>(static_cast<U>(v - _values[i - 1]));
      dlo = std::min(dlo, d);
      dhi = std::max(dhi, d);
      runs += v != _values[i - 1];
    }
    if (n == 1) {
      dlo = 0;
      dhi = 0;
    }
    const auto delta_offset = static_cast<U>(dlo);
    const auto range = static_cast<U>(hi - lo);
    const auto delta_range = static_cast<U>(static_cast<U>(dhi) - delta_offset);
    return _facts.emplace(Facts{
      .lo = lo,
      .hi = hi,
      .delta_offset = delta_offset,
      .for_stored = numeric::BytesFor(range),
      .delta_stored = numeric::BytesFor(delta_range),
      .for_bits = static_cast<uint8_t>(std::bit_width(range)),
      .delta_bits = static_cast<uint8_t>(std::bit_width(delta_range)),
      .runs = runs,
    });
  }

 private:
  bool Borrow(const duckdb::Vector& input) {
    if constexpr (std::is_integral_v<T>) {
      if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
          !duckdb::FlatVector::Validity(input).CannotHaveNull()) {
        return false;
      }
      const auto* data = duckdb::FlatVector::GetData<T>(input);
      const auto count = input.size();
      for (duckdb::idx_t i = 0; i < count; ++i) {
        _stats.Update(data[i]);
      }
      _values = {reinterpret_cast<const U*>(data), count};
      _any_valid = count != 0;
      return true;
    } else {
      return false;
    }
  }

  bool AddBlock(const duckdb::Vector& input) {
    if constexpr (std::is_integral_v<T>) {
      if (input.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
          !duckdb::FlatVector::Validity(input).CannotHaveNull()) {
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

  void DivideByGcd() noexcept {
    if constexpr (std::is_integral_v<T>) {
      U gcd = 0;
      for (const U bits : _values) {
        U magnitude = bits;
        if constexpr (std::is_signed_v<T>) {
          if (static_cast<T>(bits) < 0) {
            magnitude = static_cast<U>(U{0} - bits);
          }
        }
        gcd = std::gcd(gcd, magnitude);
        if (gcd == 1) {
          break;
        }
      }
      _scale = gcd > 1 ? gcd : 1;
      if (_scale > 1) {
        if (_owned.empty()) {
          _owned.assign(_values.begin(), _values.end());
        }
        numeric::DivideExact<std::is_signed_v<T>>(_owned.data(), _owned.size(),
                                                  _scale);
        _values = _owned;
      }
    }
  }

  duckdb::StatsWriter<T> _stats;
  std::vector<U> _owned;
  std::span<const U> _values;
  std::optional<Facts> _facts;
  U _last = 0;
  U _scale = 0;
  bool _any_valid = false;
};

}  // namespace irs::codecs
