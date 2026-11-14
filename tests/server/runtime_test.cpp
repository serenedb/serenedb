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
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <tuple>
#include <vector>

#include "runtime/runtime.h"

namespace sdb {
namespace {

TEST(RuntimeTest, RequestStopWakesAnArmedDelay) {
  Runtime runtime{{.io_threads = 1}};
  auto armed = runtime.Delay(std::chrono::hours{1});
  EXPECT_FALSE(armed.Ready());
  runtime.RequestStop();
  EXPECT_TRUE(std::move(armed).Get());
  EXPECT_TRUE(runtime.Delay(std::chrono::hours{1}).Get());
}

TEST(RuntimeTest, DelaysRacingRequestStopAllComplete) {
  Runtime runtime{{.io_threads = 2}};
  std::atomic_bool done = false;
  std::atomic_uint64_t delays = 0;
  std::vector<std::thread> loops;
  for (int i = 0; i < 4; ++i) {
    loops.emplace_back([&] {
      while (!done.load(std::memory_order_acquire)) {
        std::ignore = runtime.Delay(std::chrono::microseconds{1}).Get();
        delays.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  while (delays.load(std::memory_order_relaxed) < 1000) {
    std::this_thread::yield();
  }
  runtime.RequestStop();
  EXPECT_TRUE(runtime.Delay(std::chrono::hours{1}).Get());
  done.store(true, std::memory_order_release);
  for (auto& loop : loops) {
    loop.join();
  }
}

TEST(RuntimeTest, RequestStopRunsTheStopCallbacks) {
  Runtime runtime{{.io_threads = 1}};
  bool stopped = false;
  const std::stop_callback callback{runtime.StopToken(),
                                    [&] { stopped = true; }};
  EXPECT_FALSE(stopped);
  runtime.RequestStop();
  EXPECT_TRUE(stopped);
  EXPECT_TRUE(runtime.IsStopping());
}

TEST(RuntimeTest, ADelayOfNothingIsReadyAtOnce) {
  Runtime runtime{{.io_threads = 1}};
  EXPECT_TRUE(runtime.Delay(std::chrono::seconds{0}).Ready());
}

TEST(RuntimeTest, StopLoggedSwallowsWhatAStopThrows) {
  EXPECT_NO_THROW(StopLogged("std", [] { throw std::runtime_error{"boom"}; }));
  EXPECT_NO_THROW(StopLogged("other", [] { throw 1; }));
}

}  // namespace
}  // namespace sdb
