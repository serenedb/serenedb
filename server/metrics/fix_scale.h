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

#include "metrics/scale.h"

namespace sdb::metrics {

template<typename T>
class FixScale final : public Scale<T> {
 public:
  using Value = T;
  static constexpr ScaleType kScaleType = ScaleType::Fixed;

  FixScale(const T& low, const T& high, const std::initializer_list<T>& list)
    : Scale<T>(low, high, list.size() + 1) {
    this->_delim = list;
  }

  /**
   * index for val
   * val value
   * Return    index
   */
  size_t pos(const T& val) const {
    for (size_t i = 0; i < this->_delim.size(); ++i) {
      if (val <= this->_delim[i]) {
        return i;
      }
    }
    return this->_delim.size();
  }

  void toVPack(vpack::Builder& b) const final {
    b.add("scale-type", "fixed");
    Scale<T>::toVPack(b);
  }

 private:
  T _base;
  T _div;
};

}  // namespace sdb::metrics
