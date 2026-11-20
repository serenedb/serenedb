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

#include <atomic>
#include <chrono>
#include <iresearch/utils/blocking_once.hpp>
#include <stdexcept>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

TEST(BlockingOnceTest, OneCallRunsWhileTheOthersWaitForIt) {
  irs::BlockingOnce once;
  std::atomic_bool release{false};
  std::atomic_int runs{0};
  std::atomic_int returned{0};
  std::vector<std::thread> callers;
  for (int i = 0; i != 8; ++i) {
    callers.emplace_back([&] {
      once.Call([&] {
        ++runs;
        release.wait(false);
      });
      ++returned;
    });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_EQ(returned.load(), 0);
  release = true;
  release.notify_all();
  for (auto& caller : callers) {
    caller.join();
  }
  EXPECT_EQ(runs.load(), 1);
  EXPECT_EQ(returned.load(), 8);
  once.Call([&] { ++runs; });
  EXPECT_EQ(runs.load(), 1);
}

TEST(BlockingOnceTest, ACallThatThrowsLetsAWaiterRunInstead) {
  irs::BlockingOnce once;
  std::atomic_bool fail{false};
  std::atomic_int runs{0};
  std::atomic_int failed{0};
  std::vector<std::thread> callers;
  for (int i = 0; i != 4; ++i) {
    callers.emplace_back([&] {
      try {
        once.Call([&] {
          if (++runs == 1) {
            fail.wait(false);
            throw std::runtime_error("first call fails");
          }
        });
      } catch (const std::runtime_error&) {
        ++failed;
      }
    });
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  fail = true;
  fail.notify_all();
  for (auto& caller : callers) {
    caller.join();
  }
  EXPECT_EQ(failed.load(), 1);
  EXPECT_EQ(runs.load(), 2);
}
