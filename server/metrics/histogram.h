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

#include <absl/strings/str_format.h>

#include <atomic>
#include <ostream>  // TODO(mbkkt) replace to iosfwd, compile error now
#include <type_traits>
#include <vector>

#include "metrics/metric.h"

namespace sdb::metrics {

template<typename Scale>
class Histogram : public Metric {
 public:
  using ValueType = typename Scale::Value;

  Histogram(Scale scale, std::string_view name, std::string_view help,
            std::string_view labels)
    : Metric{name, help, labels},
      _scale{std::move(scale)},
      _n{_scale.n()},
      _c{std::allocator<std::atomic_uint64_t>{}.allocate(_n)} {
    for (size_t i = 0; i < _n; ++i) {
      new (_c + i) std::atomic_uint64_t{0};
    }
  }

  ~Histogram() override {
    std::allocator<std::atomic_uint64_t>{}.deallocate(_c, _n);
  }

  void track_extremes(ValueType val) noexcept {
#ifdef SDB_DEV
    // the value extremes are not actually required and therefore only tracked
    // in maintainer mode so they can be used when debugging.
    auto expected = _lowr.load(std::memory_order_relaxed);
    while (val < expected) {
      if (_lowr.compare_exchange_weak(expected, val,
                                      std::memory_order_relaxed)) {
        return;
      }
    }
    expected = _highr.load(std::memory_order_relaxed);
    while (val > expected) {
      if (_highr.compare_exchange_weak(expected, val,
                                       std::memory_order_relaxed)) {
        return;
      }
    }
#endif
  }

  std::string_view type() const noexcept final { return "histogram"; }

  const Scale& scale() const { return _scale; }

  size_t pos(ValueType t) const {
    auto pos = _scale.pos(t);
    SDB_ASSERT(pos < _n);
    return pos;
  }

  void count(ValueType t, uint64_t n = 1) noexcept {
    if (t < _scale.delims().front()) {
      _c[0].fetch_add(n, std::memory_order_relaxed);
    } else if (t >= _scale.delims().back()) {
      _c[_n - 1].fetch_add(n, std::memory_order_relaxed);
    } else {
      _c[pos(t)].fetch_add(n, std::memory_order_relaxed);
    }
    if constexpr (std::is_integral_v<ValueType>) {
      _sum.fetch_add(static_cast<ValueType>(n) * t, std::memory_order_relaxed);
    } else {
      ValueType tmp = _sum.load(std::memory_order_relaxed);
      do {
      } while (!_sum.compare_exchange_weak(
        tmp, tmp + static_cast<ValueType>(n) * t, std::memory_order_relaxed,
        std::memory_order_relaxed));
    }
    track_extremes(t);
  }

  ValueType low() const { return _scale.low(); }
  ValueType high() const { return _scale.high(); }

  auto& operator[](size_t n) { return _c[n]; }

  std::vector<uint64_t> load() const {
    std::vector<uint64_t> v(_n);
    for (size_t i = 0; i < _n; ++i) {
      v[i] = load(i);
    }
    return v;
  }

  uint64_t load(size_t i) const {
    SDB_ASSERT(i < _n);
    return _c[i].load(std::memory_order_relaxed);
  }

  size_t size() const { return _n; }

  void toPrometheus(std::string& result, std::string_view globals,
                    bool ensure_whitespace) const final {
    auto append_value = [&](ValueType v) {
      if (ensure_whitespace) {
        result.push_back(' ');
      }

      if constexpr (std::is_floating_point_v<ValueType>) {
        absl::StrAppendFormat(&result, "%f\n", v);
      } else {
        absl::StrAppend(&result, v, "\n");
      }
    };

    const auto globals_size = globals.size();
    const auto labels_size = labels().size();

    std::string ls;
    ls.reserve(globals_size + labels_size + 1);
    ls.append(globals);
    if (globals_size != 0 && labels_size != 0) {
      ls.push_back(',');
    }
    ls.append(labels());

    uint64_t sum = 0;
    for (size_t i = 0; i != _n; ++i) {
      sum += load(i);
      absl::StrAppend(&result, name(), "_bucket{");
      if (!ls.empty()) {
        absl::StrAppend(&result, ls, ",");
      }
      absl::StrAppend(&result, "le=\"", _scale.delim(i), "\"}");
      append_value(sum);
    }
    absl::StrAppend(&result, name(), "_count", "{", ls, "}");
    append_value(sum);
    absl::StrAppend(&result, name(), "_sum", "{", ls, "}");
    append_value(_sum.load(std::memory_order_relaxed));
  }

 private:
  const Scale _scale;
  const size_t _n;
  // TODO(mbkkt) cache line size between counters?
  // Or maybe sharded array of counters?
  std::atomic_uint64_t* _c;
  std::atomic<ValueType> _sum{0};
#ifdef SDB_DEV
  std::atomic<ValueType> _lowr{std::numeric_limits<ValueType>::max()};
  std::atomic<ValueType> _highr{std::numeric_limits<ValueType>::min()};
#endif
};

}  // namespace sdb::metrics
