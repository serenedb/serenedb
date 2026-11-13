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

#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <thread>
#include <vector>

#include "quack_periodic_worker.hpp"

namespace {

TEST(QuackPeriodicWorker, StopWaitsForARunningInterval) {
  absl::Mutex threads_lock;
  std::vector<std::thread> threads;
  const duckdb::QuackScheduler scheduler = [&](duckdb::milliseconds delay,
                                               std::function<void()> task) {
    if (delay.count() != 0) {
      return;
    }
    absl::MutexLock guard{&threads_lock};
    threads.emplace_back(std::move(task));
  };
  absl::Notification entered;
  absl::Notification release;
  std::atomic_bool finished = false;
  int calls = 0;
  auto interval = [&] {
    if (++calls == 1) {
      return duckdb::milliseconds{0};
    }
    entered.Notify();
    release.WaitForNotificationWithTimeout(absl::Seconds(10));
    finished = true;
    return duckdb::milliseconds{1};
  };

  duckdb::QuackPeriodicWorker worker;
  worker.Start(scheduler, interval, [] {});
  entered.WaitForNotification();
  absl::Notification stopped;
  bool finished_at_stop = false;
  std::thread stopper{[&] {
    worker.Stop();
    finished_at_stop = finished;
    stopped.Notify();
  }};
  stopped.WaitForNotificationWithTimeout(absl::Milliseconds(100));
  release.Notify();
  stopper.join();
  EXPECT_TRUE(finished_at_stop);

  std::vector<std::thread> ticks;
  {
    absl::MutexLock guard{&threads_lock};
    ticks.swap(threads);
  }
  for (auto& thread : ticks) {
    thread.join();
  }
}

}  // namespace
