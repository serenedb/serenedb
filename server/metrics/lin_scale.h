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
class LinScale final : public Scale<T> {
 public:
  using Value = T;
  static constexpr ScaleType kScaleType = ScaleType::Linear;

  LinScale(const T& low, const T& high, size_t n) : Scale<T>{low, high, n} {
    this->_delim.resize(n - 1);
    _div = (high - low) / (T)n;
    SDB_ASSERT(_div > 0);
    T le = low;
    for (auto& i : this->_delim) {
      le += _div;
      i = le;
    }
  }

  /**
   * index for val
   * val value
   * Return    index
   */
  size_t pos(T val) const {
    return static_cast<size_t>(std::floor((val - this->_low) / _div));
  }

  void toVPack(vpack::Builder& b) const final {
    b.add("scale-type", "linear");
    Scale<T>::toVPack(b);
  }

 private:
  T _base;
  T _div;
};

}  // namespace sdb::metrics
