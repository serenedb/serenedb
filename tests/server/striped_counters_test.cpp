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

#include <cstdint>
#include <thread>
#include <vector>

#include "server/utils/striped_counters.h"

namespace {

using sdb::StripedCounters;

constexpr size_t kLast = StripedCounters::kCounters - 1;

TEST(StripedCounters, ConcurrentAddsSumExactly) {
  StripedCounters counters;
  constexpr int64_t kThreads = 16;
  constexpr int64_t kAdds = 10000;
  std::vector<std::jthread> threads;
  for (int64_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int64_t i = 0; i < kAdds; ++i) {
        counters.Add(0, 1);
        counters.Add(kLast, 3);
      }
    });
  }
  threads.clear();
  EXPECT_EQ(counters.Read(0), kThreads * kAdds);
  EXPECT_EQ(counters.Read(1), 0);
  EXPECT_EQ(counters.Read(kLast), 3 * kThreads * kAdds);
}

TEST(StripedCounters, ResetZeroesEveryCounter) {
  StripedCounters counters;
  counters.Add(0, -5);
  counters.Add(kLast, 7);
  EXPECT_EQ(counters.Read(0), -5);
  counters.Reset();
  EXPECT_EQ(counters.Read(0), 0);
  EXPECT_EQ(counters.Read(kLast), 0);
  counters.Add(0, 2);
  EXPECT_EQ(counters.Read(0), 2);
}

}  // namespace
