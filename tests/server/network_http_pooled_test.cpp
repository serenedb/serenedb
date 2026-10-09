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

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "network/http/pooled.h"

using sdb::network::http::Pooled;

namespace {

template<typename Self, bool Resettable>
struct Tracked {
  Tracked() { gCreated.fetch_add(1, std::memory_order_relaxed); }
  ~Tracked() { gDestroyed.fetch_add(1, std::memory_order_relaxed); }

  bool Reset() noexcept {
    ++resets;
    return Resettable;
  }

  static int Live() {
    return gCreated.load(std::memory_order_relaxed) -
           gDestroyed.load(std::memory_order_relaxed);
  }

  static inline std::atomic<int> gCreated{0};
  static inline std::atomic<int> gDestroyed{0};
  int resets = 0;
};

struct Counter : Tracked<Counter, true> {};
struct Broken : Tracked<Broken, false> {};
struct Bounded : Tracked<Bounded, true> {};
struct Moving : Tracked<Moving, true> {};
struct Contended : Tracked<Contended, true> {};

TEST(NetworkHttpPooled, ReusesTheReleasedStateAfterReset) {
  Counter* first = nullptr;
  int resets = 0;
  {
    Pooled<Counter> state;
    first = &*state;
    resets = state->resets;
  }
  const int created = Counter::gCreated;
  Pooled<Counter> again;
  EXPECT_EQ(&*again, first);
  EXPECT_EQ(again->resets, resets + 1);
  EXPECT_EQ(Counter::gCreated, created);
}

TEST(NetworkHttpPooled, DropsStateThatFailsToReset) {
  const int destroyed = Broken::gDestroyed;
  {
    Pooled<Broken> state;
  }
  EXPECT_EQ(Broken::gDestroyed, destroyed + 1);
}

TEST(NetworkHttpPooled, KeepsAtMostCapacity) {
  const size_t capacity = Pooled<Bounded>::Capacity();
  ASSERT_GT(capacity, 0u);
  std::vector<std::unique_ptr<Pooled<Bounded>>> states;
  for (size_t i = 0; i <= capacity; ++i) {
    states.push_back(std::make_unique<Pooled<Bounded>>());
  }
  const int destroyed = Bounded::gDestroyed;
  states.clear();
  EXPECT_EQ(Bounded::gDestroyed, destroyed + 1);
}

TEST(NetworkHttpPooled, ReleaseIsSharedAcrossThreads) {
  auto state = std::make_unique<Pooled<Moving>>();
  auto* raw = &**state;
  std::thread other{[&] { state.reset(); }};
  other.join();
  const int created = Moving::gCreated;
  Pooled<Moving> again;
  EXPECT_EQ(&*again, raw);
  EXPECT_EQ(Moving::gCreated, created);
}

TEST(NetworkHttpPooled, ConcurrentUseStaysBounded) {
  constexpr int kThreads = 8;
  constexpr int kRounds = 10000;
  const int created = Contended::gCreated;
  std::vector<std::thread> threads;
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([] {
      for (int i = 0; i < kRounds; ++i) {
        Pooled<Contended> first;
        Pooled<Contended> second;
        ++first->resets;
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_LE(Contended::Live(), static_cast<int>(Pooled<Contended>::Capacity()));
  EXPECT_LT(Contended::gCreated - created, 2 * kThreads * kRounds);
}

}  // namespace
