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

#include "metrics/counter.h"

#include <ostream>

namespace sdb::metrics {

Counter::Counter(uint64_t n, std::string_view name, std::string_view help,
                 std::string_view labels)
  : Metric{name, help, labels}, _c{n} {}

Counter::~Counter() = default;

std::string_view Counter::type() const noexcept { return "counter"; }

uint64_t Counter::load() const noexcept {
  if (_b.load(std::memory_order_relaxed)) {
    const auto b = _b.exchange(0, std::memory_order_relaxed);
    return _c.fetch_add(b, std::memory_order_relaxed) + b;
  }
  return _c.load(std::memory_order_relaxed);
}

void Counter::store(uint64_t n) noexcept {
  _c.exchange(n, std::memory_order_relaxed);
}

void Counter::count(uint64_t n) noexcept {
  _b.fetch_add(n, std::memory_order_relaxed);
}

Counter& Counter::operator=(uint64_t n) noexcept {
  store(n);
  return *this;
}

Counter& Counter::operator+=(uint64_t n) noexcept {
  count(n);
  return *this;
}

Counter& Counter::operator++() noexcept {
  count();
  return *this;
}

std::ostream& Counter::print(std::ostream& output) const {
  return output << _c;
}

}  // namespace sdb::metrics
