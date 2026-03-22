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

#include "gauge.h"

namespace sdb::metrics {

template<typename T>
struct GaugeCounterGuard {
  GaugeCounterGuard(const GaugeCounterGuard&) = delete;
  GaugeCounterGuard& operator=(const GaugeCounterGuard&) = delete;

  GaugeCounterGuard(GaugeCounterGuard&& other) noexcept {
    *this = std::move(other);
  }
  GaugeCounterGuard& operator=(GaugeCounterGuard&& other) noexcept {
    reset();
    std::swap(other._total_value, _total_value);
    std::swap(other._metric, _metric);
    return *this;
  }

  ~GaugeCounterGuard() { reset(); }
  GaugeCounterGuard() = default;

  explicit GaugeCounterGuard(Gauge<T>& metric, T initial_value = {})
    : _metric(&metric) {
    add(initial_value);
  }

  void add(T delta) noexcept {
    if (_metric) {
      _metric->fetch_add(delta);
      _total_value += delta;
    }
  }

  void sub(T delta) noexcept {
    if (_metric) {
      _metric->fetch_sub(delta);
      _total_value -= delta;
    }
  }

  void reset(uint64_t new_value = {}) noexcept {
    if (_metric) {
      _metric->fetch_sub(_total_value - new_value);
      _total_value = new_value;
      _metric = nullptr;
    }
  }

 private:
  T _total_value{0};
  Gauge<T>* _metric = nullptr;
};

}  // namespace sdb::metrics
