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
#include <chrono>
#include <thread>
#include <vector>

#include "network/server.h"
#include "scheduler/background_scheduler.h"

namespace sdb {
namespace {

TEST(BackgroundSchedulerTest, ADelayBeforeOpenDelaysParksUntilThen) {
  Server server;
  BackgroundScheduler scheduler;
  scheduler.start();
  auto parked = scheduler.Delay(std::chrono::hours{1});
  EXPECT_FALSE(parked.Ready());
  server.StartIoPool();
  scheduler.OpenDelays();
  EXPECT_TRUE(std::move(parked).Get());
  scheduler.CancelDelays();
  server.RequestStop();
  server.stop();
  scheduler.stop();
}

TEST(BackgroundSchedulerTest, DelaysRacingShutdownLeaveTheIoPoolAlone) {
  Server server;
  BackgroundScheduler scheduler;
  scheduler.start();
  server.StartIoPool();
  scheduler.OpenDelays();
  std::atomic_bool done = false;
  std::atomic_uint64_t delays = 0;
  std::vector<std::thread> loops;
  for (int i = 0; i < 4; ++i) {
    loops.emplace_back([&] {
      while (!done.load(std::memory_order_acquire)) {
        std::ignore = scheduler.Delay(std::chrono::microseconds{1}).Get();
        delays.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  while (delays.load(std::memory_order_relaxed) < 1000) {
    std::this_thread::yield();
  }
  scheduler.CancelDelays();
  server.RequestStop();
  server.stop();
  EXPECT_TRUE(scheduler.Delay(std::chrono::hours{1}).Get());
  done.store(true, std::memory_order_release);
  for (auto& loop : loops) {
    loop.join();
  }
  scheduler.stop();
}

}  // namespace
}  // namespace sdb
