////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cmath>

#include "basics/debugging.h"
#include "metrics/scale.h"

namespace sdb::metrics {

template<typename T>
class LogScale final : public Scale<T> {
 public:
  using Value = T;
  static constexpr ScaleType kScaleType = ScaleType::Logarithmic;

  static constexpr T getHighFromSmallestBucket(T smallest_bucket_size, T base,
                                               T low, size_t n) {
    return static_cast<T>((smallest_bucket_size - low) * std::pow(base, n - 1) +
                          low);
  }
  struct SupplySmallestBucket {};
  static constexpr auto kSupplySmallestBucket = SupplySmallestBucket{};

  LogScale(SupplySmallestBucket, const T& base, const T& low,
           const T& smallest_bucket_size, size_t n)
    : LogScale{base, low,
               getHighFromSmallestBucket(smallest_bucket_size, base, low, n),
               n} {}

  LogScale(const T& base, const T& low, const T& high, size_t n)
    : Scale<T>{low, high, n}, _base{base} {
    SDB_ASSERT(base > T(0));
    double nn = -1.0 * (n - 1);
    for (auto& i : this->_delim) {
      i = static_cast<T>(
        static_cast<double>(high - low) *
          std::pow(static_cast<double>(base), static_cast<double>(nn++)) +
        static_cast<double>(low));
    }
    _div = this->_delim.front() - low;
    SDB_ASSERT(_div > T(0));
    _lbase = std::log(_base);
  }

  /**
   * Dump to builder
   * b Envelope
   */
  void toVPack(vpack::Builder& b) const final {
    b.add("scale-type", "logarithmic");
    b.add("base", _base);
    Scale<T>::toVPack(b);
  }

  /**
   * index for val
   * val value
   * Return    index
   */
  size_t pos(T val) const {
    return static_cast<size_t>(
      1 + std::floor(std::log((val - this->_low) / _div) / _lbase));
  }

  T base() const { return _base; }

 private:
  T _base;
  T _div;
  double _lbase;
};

}  // namespace sdb::metrics
