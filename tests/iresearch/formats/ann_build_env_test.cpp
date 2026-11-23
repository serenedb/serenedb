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

#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/notification.h>

#include <algorithm>
#include <atomic>
#include <iresearch/formats/ann_build_env.hpp>
#include <stdexcept>
#include <thread>
#include <vector>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "tests_shared.hpp"

namespace {

TEST(AnnBuildEnvFanOut, RunsEveryTaskOnTheCallerWithoutAnEnv) {
  const auto caller = std::this_thread::get_id();
  std::vector<size_t> order;
  irs::FanOut(nullptr, 5, [&](size_t task) {
    EXPECT_EQ(std::this_thread::get_id(), caller);
    order.push_back(task);
  });
  EXPECT_EQ(order, (std::vector<size_t>{0, 1, 2, 3, 4}));
}

TEST(AnnBuildEnvFanOut, RunsEveryTaskOnceWithinItsGrant) {
  auto pool = yaclib::MakeFairThreadPool(8);
  absl::Cleanup stop = [&] {
    pool->SoftStop();
    pool->Wait();
  };
  constexpr uint32_t kGrant = 3;
  std::atomic<uint32_t> held{0};
  const auto acquire = [&](uint32_t want, uint32_t have) {
    const auto grant = std::min(want, kGrant - have);
    held.fetch_add(grant);
    return grant;
  };
  const auto release = [&](uint32_t n) { held.fetch_sub(n); };
  const irs::AnnBuildEnv env{
    .executor = *pool, .acquire = acquire, .release = release};

  constexpr size_t kTasks = 500;
  std::vector<std::atomic<uint32_t>> runs(kTasks);
  std::atomic<uint32_t> running{0};
  std::atomic<uint32_t> peak{0};
  irs::FanOut(&env, kTasks, [&](size_t task) {
    const auto now = running.fetch_add(1) + 1;
    auto seen = peak.load();
    while (seen < now && !peak.compare_exchange_weak(seen, now)) {
    }
    runs[task].fetch_add(1);
    running.fetch_sub(1);
  });
  for (size_t task = 0; task != kTasks; ++task) {
    EXPECT_EQ(runs[task].load(), 1U) << task;
  }
  EXPECT_LE(peak.load(), kGrant);
  EXPECT_EQ(held.load(), 0U);
}

TEST(AnnBuildEnvFanOut, RethrowsAWorkersError) {
  auto pool = yaclib::MakeFairThreadPool(2);
  absl::Cleanup stop = [&] {
    pool->SoftStop();
    pool->Wait();
  };
  const auto acquire = [](uint32_t want, uint32_t) { return want; };
  const auto release = [](uint32_t) {};
  const irs::AnnBuildEnv env{
    .executor = *pool, .acquire = acquire, .release = release};

  const auto caller = std::this_thread::get_id();
  std::atomic<bool> failed{false};
  absl::Notification worker_failed;
  EXPECT_THROW(irs::FanOut(&env, 4,
                           [&](size_t) {
                             if (std::this_thread::get_id() == caller) {
                               worker_failed.WaitForNotification();
                               return;
                             }
                             if (!failed.exchange(true)) {
                               worker_failed.Notify();
                             }
                             throw std::runtime_error{"worker failed"};
                           }),
               std::runtime_error);
}

}  // namespace
