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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <latch>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "search/ann_build_pool.h"

namespace {

std::atomic<uint32_t> gHeld{0};

uint32_t TestAcquire(uint32_t want) noexcept {
  const auto grant = std::clamp(want, 1U, 4U);
  gHeld.fetch_add(grant);
  return grant;
}

void TestRelease(uint32_t workers) noexcept { gHeld.fetch_sub(workers); }

class AnnBuildPoolTest : public ::testing::Test {
 protected:
  AnnBuildPoolTest()
    : _pool{yaclib::MakeFairThreadPool(3)},
      _env{*_pool, TestAcquire, TestRelease} {}

  void TearDown() final {
    _pool->SoftStop();
    _pool->Wait();
  }

  void StopPool() {
    _pool->Stop();
    _pool->Wait();
  }

  yaclib::IntrusivePtr<yaclib::FairThreadPool> _pool;
  sdb::search::AnnBuildPool _env;
};

TEST_F(AnnBuildPoolTest, AcquireAndReleaseGoThroughTheBudget) {
  EXPECT_EQ(4U, _env.Acquire(10));
  EXPECT_EQ(4U, gHeld.load());
  _env.Release(4);
  EXPECT_EQ(0U, gHeld.load());
  EXPECT_EQ(1U, _env.Acquire(0));
  _env.Release(1);
}

TEST_F(AnnBuildPoolTest, EveryWorkerRunsUntilItIsDone) {
  constexpr uint32_t kWorkers = 4;
  constexpr uint32_t kSteps = 200;
  std::array<std::atomic<uint32_t>, kWorkers> steps{};
  std::array<std::atomic<bool>, kWorkers> on_caller{};
  const auto caller = std::this_thread::get_id();
  _env.Run(kWorkers, [&](uint32_t w) {
    if (std::this_thread::get_id() == caller) {
      on_caller[w] = true;
    }
    return steps[w].fetch_add(1) + 1 < kSteps;
  });
  for (uint32_t w = 0; w < kWorkers; ++w) {
    EXPECT_EQ(kSteps, steps[w].load()) << "worker " << w;
    EXPECT_EQ(w == 0, on_caller[w].load()) << "worker " << w;
  }
}

TEST_F(AnnBuildPoolTest, OneWorkerRunsOnlyOnTheCaller) {
  std::atomic<uint32_t> steps{0};
  std::atomic<bool> elsewhere{false};
  const auto caller = std::this_thread::get_id();
  _env.Run(1, [&](uint32_t w) {
    if (w != 0 || std::this_thread::get_id() != caller) {
      elsewhere = true;
    }
    return steps.fetch_add(1) + 1 < 10;
  });
  EXPECT_EQ(10U, steps.load());
  EXPECT_FALSE(elsewhere.load());
}

TEST_F(AnnBuildPoolTest, SharedCursorHandsOutEveryItemOnce) {
  constexpr size_t kItems = 100000;
  constexpr size_t kGranule = 64;
  std::vector<std::atomic<uint8_t>> seen(kItems);
  std::atomic<size_t> cursor{0};
  _env.Run(4, [&](uint32_t) {
    const auto begin = cursor.fetch_add(kGranule);
    if (begin >= kItems) {
      return false;
    }
    for (auto i = begin; i < std::min(begin + kGranule, kItems); ++i) {
      seen[i].fetch_add(1);
    }
    return true;
  });
  for (size_t i = 0; i < kItems; ++i) {
    ASSERT_EQ(1, seen[i].load()) << "item " << i;
  }
}

TEST_F(AnnBuildPoolTest, HelperErrorStopsTheOthersAndRethrows) {
  std::array<std::atomic<uint32_t>, 4> steps{};
  try {
    _env.Run(4, [&](uint32_t w) {
      const auto n = steps[w].fetch_add(1);
      if (w == 2 && n == 3) {
        throw std::runtime_error{"helper failed"};
      }
      std::this_thread::sleep_for(std::chrono::microseconds{200});
      return n < 100000;
    });
    FAIL() << "Run did not rethrow the helper's error";
  } catch (const std::exception& e) {
    EXPECT_NE(std::string{e.what()}.find("helper failed"), std::string::npos);
  }
  EXPECT_LT(steps[0].load(), 100000U);
}

TEST_F(AnnBuildPoolTest, CallerErrorWaitsForTheHelpers) {
  std::atomic<int> active{0};
  std::atomic<bool> helpers_started{false};
  EXPECT_THROW(
    _env.Run(4,
             [&](uint32_t w) {
               if (w == 0) {
                 while (!helpers_started.load()) {
                   std::this_thread::yield();
                 }
                 throw std::runtime_error{"caller failed"};
               }
               active.fetch_add(1);
               helpers_started = true;
               std::this_thread::sleep_for(std::chrono::milliseconds{2});
               active.fetch_sub(1);
               return true;
             }),
    std::runtime_error);
  EXPECT_EQ(0, active.load());
}

TEST_F(AnnBuildPoolTest, TwoHelperErrorsRethrowOne) {
  std::latch both_in_step{2};
  std::latch fail{1};
  try {
    _env.Run(3, [&](uint32_t w) -> bool {
      if (w == 0) {
        both_in_step.wait();
        fail.count_down();
        return false;
      }
      both_in_step.count_down();
      fail.wait();
      throw std::runtime_error{w == 1 ? "first" : "second"};
    });
    FAIL() << "Run did not rethrow a helper's error";
  } catch (const std::runtime_error& e) {
    const std::string what = e.what();
    EXPECT_TRUE(what == "first" || what == "second") << what;
  }
}

TEST_F(AnnBuildPoolTest, StoppedPoolRunsEveryWorkerOnTheCaller) {
  StopPool();
  constexpr uint32_t kWorkers = 4;
  std::array<std::atomic<uint32_t>, kWorkers> steps{};
  std::atomic<bool> elsewhere{false};
  const auto caller = std::this_thread::get_id();
  _env.Run(kWorkers, [&](uint32_t w) {
    if (std::this_thread::get_id() != caller) {
      elsewhere = true;
    }
    return steps[w].fetch_add(1) + 1 < 50;
  });
  for (uint32_t w = 0; w < kWorkers; ++w) {
    EXPECT_EQ(50U, steps[w].load()) << "worker " << w;
  }
  EXPECT_FALSE(elsewhere.load());
}

TEST_F(AnnBuildPoolTest, ErrorInADroppedWorkerSkipsTheRest) {
  StopPool();
  std::array<std::atomic<uint32_t>, 4> steps{};
  EXPECT_THROW(_env.Run(4,
                        [&](uint32_t w) -> bool {
                          steps[w].fetch_add(1);
                          if (w == 1) {
                            throw std::runtime_error{"worker 1 failed"};
                          }
                          return false;
                        }),
               std::runtime_error);
  EXPECT_EQ(1U, steps[0].load());
  EXPECT_EQ(1U, steps[1].load());
  EXPECT_EQ(0U, steps[2].load());
  EXPECT_EQ(0U, steps[3].load());
}

TEST_F(AnnBuildPoolTest, ConcurrentRunsKeepTheirOwnWorkers) {
  constexpr uint32_t kRuns = 4;
  constexpr uint32_t kWorkers = 3;
  constexpr uint32_t kSteps = 100;
  std::array<std::array<std::atomic<uint32_t>, kWorkers>, kRuns> steps{};
  std::vector<std::thread> callers;
  callers.reserve(kRuns);
  for (uint32_t r = 0; r < kRuns; ++r) {
    callers.emplace_back([&, r] {
      _env.Run(kWorkers, [&, r](uint32_t w) {
        return steps[r][w].fetch_add(1) + 1 < kSteps;
      });
    });
  }
  for (auto& caller : callers) {
    caller.join();
  }
  for (uint32_t r = 0; r < kRuns; ++r) {
    for (uint32_t w = 0; w < kWorkers; ++w) {
      EXPECT_EQ(kSteps, steps[r][w].load()) << "run " << r << " worker " << w;
    }
  }
}

TEST(AnnBuildPoolLifetimeTest, TheEnvKeepsItsPoolAlive) {
  auto pool = yaclib::MakeFairThreadPool(2);
  sdb::search::AnnBuildPool env{*pool, TestAcquire, TestRelease};
  pool->SoftStop();
  pool->Wait();
  pool = nullptr;
  std::array<std::atomic<uint32_t>, 3> steps{};
  env.Run(3, [&](uint32_t w) { return steps[w].fetch_add(1) + 1 < 10; });
  for (uint32_t w = 0; w < 3; ++w) {
    EXPECT_EQ(10U, steps[w].load()) << "worker " << w;
  }
}

}  // namespace
