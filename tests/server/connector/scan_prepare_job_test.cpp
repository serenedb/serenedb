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
#include <memory>
#include <stdexcept>
#include <thread>
#include <vector>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

class Helpers {
 public:
  explicit Helpers(SegmentWork& work, uint32_t count) : _work{work} {
    _work.prepare.store(SegmentWork::kPreparing, std::memory_order_release);
    _work.job.Hire(count + 1);
    for (uint32_t i = 0; i < count; ++i) {
      _threads.emplace_back([this] {
        for (;;) {
          const auto state = _work.prepare.load(std::memory_order_acquire);
          if (state == SegmentWork::kReady) {
            return;
          }
          _work.job.Help();
          _work.prepare.wait(state, std::memory_order_acquire);
        }
      });
    }
  }

  ~Helpers() {
    _work.prepare.store(SegmentWork::kReady, std::memory_order_release);
    _work.prepare.notify_all();
    for (auto& thread : _threads) {
      thread.join();
    }
  }

 private:
  SegmentWork& _work;
  std::vector<std::thread> _threads;
};

void RunCounting(PrepareJob& job, uint32_t count) {
  auto hits = std::make_unique<std::atomic_uint32_t[]>(count);
  job.Run(count, [&](irs::PrepareTasks::Next next) {
    for (auto task = next(); task != count; task = next()) {
      hits[task].fetch_add(1, std::memory_order_relaxed);
    }
  });
  for (uint32_t task = 0; task < count; ++task) {
    ASSERT_EQ(1U, hits[task].load(std::memory_order_relaxed)) << task;
  }
}

TEST(PrepareJob, RunsEveryTaskOnceWithHelpers) {
  SegmentWork work;
  Helpers helpers{work, 3};
  ASSERT_EQ(4U, work.job.Threads());
  for (uint32_t round = 0; round < 200; ++round) {
    RunCounting(work.job, 1 + (round * 37) % 300);
  }
}

TEST(PrepareJob, RunsEveryTaskOnceAlone) {
  SegmentWork work;
  work.prepare.store(SegmentWork::kPreparing, std::memory_order_release);
  for (const uint32_t count : {0U, 1U, 2U, 1000U}) {
    RunCounting(work.job, count);
  }
}

TEST(PrepareJob, EveryHelperRunsTheWorker) {
  SegmentWork work;
  Helpers helpers{work, 3};
  for (uint32_t round = 0; round < 50; ++round) {
    std::atomic_uint32_t done{0};
    std::atomic_uint32_t workers{0};
    constexpr uint32_t kTasks = 64;
    work.job.Run(kTasks, [&](irs::PrepareTasks::Next next) {
      workers.fetch_add(1, std::memory_order_relaxed);
      for (auto task = next(); task != kTasks; task = next()) {
        std::this_thread::yield();
        done.fetch_add(1, std::memory_order_relaxed);
      }
    });
    ASSERT_EQ(kTasks, done.load(std::memory_order_relaxed));
    ASSERT_LE(1U, workers.load(std::memory_order_relaxed));
    ASSERT_GE(4U, workers.load(std::memory_order_relaxed));
  }
}

TEST(PrepareJob, RethrowsTheFirstFailureAndStaysUsable) {
  SegmentWork work;
  Helpers helpers{work, 3};
  for (uint32_t round = 0; round < 50; ++round) {
    constexpr uint32_t kTasks = 100;
    const uint32_t failing = round % kTasks;
    std::atomic_uint32_t ran{0};
    ASSERT_THROW(work.job.Run(kTasks,
                              [&](irs::PrepareTasks::Next next) {
                                for (auto task = next(); task != kTasks;
                                     task = next()) {
                                  ran.fetch_add(1, std::memory_order_relaxed);
                                  if (task == failing) {
                                    throw std::runtime_error{"task failed"};
                                  }
                                }
                              }),
                 std::runtime_error);
    ASSERT_LE(1U, ran.load(std::memory_order_relaxed));
    ASSERT_GE(kTasks, ran.load(std::memory_order_relaxed));
    RunCounting(work.job, kTasks);
  }
}

TEST(PrepareJob, WaitsForTheTasksItHandedOut) {
  SegmentWork work;
  Helpers helpers{work, 3};
  for (uint32_t round = 0; round < 20; ++round) {
    constexpr uint32_t kTasks = 16;
    std::atomic_uint32_t running{0};
    std::atomic_uint32_t finished{0};
    work.job.Run(kTasks, [&](irs::PrepareTasks::Next next) {
      for (auto task = next(); task != kTasks; task = next()) {
        running.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::microseconds{200});
        finished.fetch_add(1, std::memory_order_relaxed);
      }
    });
    ASSERT_EQ(kTasks, running.load(std::memory_order_relaxed));
    ASSERT_EQ(kTasks, finished.load(std::memory_order_relaxed));
  }
}

}  // namespace
}  // namespace sdb::connector
