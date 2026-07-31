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

#include "connector/azure_cleanup_scheduler.h"

#include <chrono>
#include <functional>
#include <utility>
#include <yaclib/algo/wait_group.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/lifecycle.h"
#include "scheduler/background_scheduler.h"

namespace Azure::Core::Http {

using CurlCleanupScheduler = std::function<void(
  std::chrono::milliseconds interval, std::function<bool()> tick)>;
void SetCurlCleanupScheduler(CurlCleanupScheduler scheduler);

}  // namespace Azure::Core::Http
namespace sdb {
namespace {

// Armed cleanup loops, joined by StopAzureCleanupScheduler().
yaclib::WaitGroup<> g_loops{1};

yaclib::Future<> CleanupLoop(std::chrono::milliseconds interval,
                             std::function<bool()> tick) {
  auto& s = BackgroundScheduler::instance();
  try {
    for (;;) {
      co_await s.Delay(interval);
      if (lifecycle::IsStopping()) {
        break;
      }
      const bool proceed = co_await s.Run([&tick] {
        try {
          return tick();
        } catch (...) {
          return false;
        }
      });
      if (!proceed) {
        break;
      }
    }
  } catch (...) {
  }
  co_return {};
}

}  // namespace

void InstallAzureCleanupScheduler() {
  Azure::Core::Http::SetCurlCleanupScheduler(
    [](std::chrono::milliseconds interval, std::function<bool()> tick) {
      if (lifecycle::IsStopping()) {
        // Pooled connections are released with the pool at exit.
        return;
      }
      g_loops.Consume(CleanupLoop(interval, std::move(tick)));
    });
}

void StopAzureCleanupScheduler() {
  g_loops.Done();
  g_loops.Wait();
}

}  // namespace sdb
