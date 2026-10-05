////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <thread>

#include "server/utils/thread_local_pool.h"

using sdb::Pooled;
using sdb::ThreadLocalPool;

namespace {

struct Counter {
  bool Reset() noexcept {
    ++resets;
    return true;
  }

  int resets = 0;
};

struct Broken {
  bool Reset() noexcept { return false; }
};

struct Bounded {
  bool Reset() noexcept { return true; }
};

struct Moving {
  bool Reset() noexcept { return true; }
};

TEST(ThreadLocalPool, ReusesTheReleasedStateAfterReset) {
  Counter* first = nullptr;
  {
    Pooled<Counter> state;
    first = &*state;
    EXPECT_EQ(state->resets, 0);
  }
  Pooled<Counter> again;
  EXPECT_EQ(&*again, first);
  EXPECT_EQ(again->resets, 1);
}

TEST(ThreadLocalPool, DropsStateThatFailsToReset) {
  ThreadLocalPool<Broken>::Release(ThreadLocalPool<Broken>::Acquire());
  EXPECT_EQ(ThreadLocalPool<Broken>::IdleCount(), 0u);
}

TEST(ThreadLocalPool, KeepsAtMostMaxIdle) {
  using Pool = ThreadLocalPool<Bounded, 2>;
  auto a = Pool::Acquire();
  auto b = Pool::Acquire();
  auto c = Pool::Acquire();
  Pool::Release(std::move(a));
  Pool::Release(std::move(b));
  Pool::Release(std::move(c));
  EXPECT_EQ(Pool::IdleCount(), 2u);
}

TEST(ThreadLocalPool, ReleaseLandsInTheReleasingThread) {
  using Pool = ThreadLocalPool<Moving>;
  auto state = Pool::Acquire();
  size_t other_idle = 0;
  std::thread other{[&] {
    Pool::Release(std::move(state));
    other_idle = Pool::IdleCount();
  }};
  other.join();
  EXPECT_EQ(other_idle, 1u);
  EXPECT_EQ(Pool::IdleCount(), 0u);
}

}  // namespace
