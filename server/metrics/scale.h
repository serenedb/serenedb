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

#include <vpack/builder.h>
#include <vpack/value.h>

#include <cstddef>
#include <iosfwd>
#include <vector>

#include "basics/debugging.h"
#include "metrics/metric.h"

namespace sdb::metrics {

template<typename T>
class Scale {
 public:
  using Value = T;

  Scale(const T& low, const T& high, size_t n) : _n{n}, _low{low}, _high{high} {
    SDB_ASSERT(n > 1);
    _delim.resize(n - 1);
  }

  virtual ~Scale() = default;

  /**
   * number of buckets
   */
  [[nodiscard]] size_t n() const noexcept { return _n; }
  [[nodiscard]] T low() const { return _low; }
  [[nodiscard]] T high() const { return _high; }
  [[nodiscard]] std::string delim(size_t s) const {
    return (s < _n - 1) ? std::to_string(_delim[s]) : "+Inf";
  }

  [[nodiscard]] const std::vector<T>& delims() const { return _delim; }

  /**
   * dump to builder
   */
  virtual void toVPack(vpack::Builder& b) const {
    SDB_ASSERT(b.isOpenObject());
    b.add("lower-limit", _low);
    b.add("upper-limit", _high);
    b.add("value-type", typeid(T).name());
    b.add("range");
    vpack::ArrayBuilder abb(&b);
    for (const auto& i : _delim) {
      b.add(i);
    }
  }

  /**
   * dump to std::ostream
   */
  std::ostream& print(std::ostream& output) const {
    vpack::Builder b;
    {
      vpack::ObjectBuilder bb(&b);
      toVPack(b);
    }
    return output << b.toJson();
  }

 protected:
  std::vector<T> _delim;
  size_t _n;
  T _low;
  T _high;
};

enum class ScaleType {
  Fixed,
  Linear,
  Logarithmic,
};

}  // namespace sdb::metrics
