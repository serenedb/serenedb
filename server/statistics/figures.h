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

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <mutex>
#include <vector>

#include "basics/assert.h"
#include "basics/common.h"

namespace sdb {
namespace statistics {

////////////////////////////////////////////////////////////////////////////////
/// a simple counter
////////////////////////////////////////////////////////////////////////////////

struct Counter {
  constexpr Counter() noexcept : _count(0) {}

  Counter& operator=(const Counter& other) {
    _count.store(other._count.load());
    return *this;
  }

  void incCounter() noexcept { ++_count; }

  void decCounter() noexcept { --_count; }

  int64_t get() const noexcept {
    return _count.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<int64_t> _count;
};

////////////////////////////////////////////////////////////////////////////////
/// a distribution with count, min, max, mean, and variance
////////////////////////////////////////////////////////////////////////////////

struct Distribution {
  Distribution() : count(0), total(0.0), cuts(), counts() {}

  explicit Distribution(const std::vector<double>& dist)
    : count(0), total(0.0), cuts(dist), counts() {
    counts.resize(cuts.size() + 1);
  }

  Distribution& operator=(Distribution& other) {
    std::lock_guard l1{_mutex};
    std::lock_guard l2{other._mutex};

    count = other.count;
    total = other.total;
    cuts = other.cuts;
    counts = other.counts;

    return *this;
  }

  void addFigure(double value) {
    SDB_ASSERT(!counts.empty());
    std::lock_guard lock{_mutex};

    ++count;
    total += value;

    std::vector<double>::iterator i = cuts.begin();
    std::vector<uint64_t>::iterator j = counts.begin();

    for (; i != cuts.end(); ++i, ++j) {
      if (value < *i) {
        ++(*j);
        return;
      }
    }

    ++(*j);
  }

  void add(Distribution& other) {
    std::lock_guard lock{_mutex};
    std::lock_guard lock2{other._mutex};
    SDB_ASSERT(counts.size() == other.counts.size() &&
               cuts.size() == other.cuts.size());
    count += other.count;
    total += other.total;
    for (size_t i = 0; i < counts.size(); ++i) {
      SDB_ASSERT(i < cuts.size() ? cuts[i] == other.cuts[i] : true);
      counts[i] += other.counts[i];
    }
  }

  uint64_t count;
  double total;
  std::vector<double> cuts;
  std::vector<uint64_t> counts;

 private:
  absl::Mutex _mutex;
};

}  // namespace statistics
}  // namespace sdb
