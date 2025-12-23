////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2020 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include <algorithm>
#include <atomic>
#include <functional>
#include <thread>
#include <vector>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/global_resource_monitor.h"
#include "basics/resource_usage.h"
#include "basics/thread_guard.h"
#include "gtest/gtest.h"

namespace {

constexpr size_t kNumThreads = 4;
constexpr uint64_t kNumOpsPerThread = 15 * 1000 * 1000;
constexpr size_t kAmount = 123;

constexpr size_t Bucketize(size_t value) {
  return value / sdb::ResourceMonitor::kChunkSize *
         sdb::ResourceMonitor::kChunkSize;
}

}  // namespace

using namespace sdb;

TEST(ResourceUsageTest, testEmpty) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  ASSERT_EQ(0, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());
  ASSERT_EQ(0, monitor.peak());

  monitor.memoryLimit(123456);
  ASSERT_EQ(123456, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());
  ASSERT_EQ(0, monitor.peak());
}

TEST(ResourceUsageTest, testBasicRestrictions) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  ASSERT_EQ(0, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());
  ASSERT_EQ(0, monitor.peak());

  // note: the memoryLimit has a granularity of 32kb right now!
  monitor.memoryLimit(10 * ResourceMonitor::kChunkSize);
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());
  ASSERT_EQ(0, monitor.peak());

  monitor.increaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(ResourceMonitor::kChunkSize, monitor.peak());

  monitor.decreaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(0, monitor.current());
  ASSERT_EQ(ResourceMonitor::kChunkSize, monitor.peak());

  monitor.increaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(ResourceMonitor::kChunkSize, monitor.peak());

  monitor.increaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(2 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(2 * ResourceMonitor::kChunkSize, monitor.peak());

  monitor.decreaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(2 * ResourceMonitor::kChunkSize, monitor.peak());

  monitor.increaseMemoryUsage(5 * ResourceMonitor::kChunkSize);
  ASSERT_EQ(6 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(6 * ResourceMonitor::kChunkSize, monitor.peak());

  monitor.increaseMemoryUsage(4 * ResourceMonitor::kChunkSize);
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.peak());

  ASSERT_THROW(
    {
      try {
        monitor.increaseMemoryUsage(ResourceMonitor::kChunkSize);
      } catch (const basics::Exception& ex) {
        ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
        throw;
      }
    },
    basics::Exception);
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.peak());

  monitor.decreaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(9 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.peak());

  ASSERT_THROW(
    {
      try {
        monitor.increaseMemoryUsage(2 * ResourceMonitor::kChunkSize);
      } catch (const basics::Exception& ex) {
        ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
        throw;
      }
    },
    basics::Exception);
  ASSERT_EQ(9 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.peak());

  monitor.decreaseMemoryUsage(ResourceMonitor::kChunkSize);
  ASSERT_EQ(8 * ResourceMonitor::kChunkSize, monitor.current());
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.peak());

  monitor.decreaseMemoryUsage(8 * ResourceMonitor::kChunkSize);
  ASSERT_EQ(0, monitor.current());
  ASSERT_EQ(10 * ResourceMonitor::kChunkSize, monitor.peak());
}

TEST(ResourceUsageTest, testIncreaseInStepsRestricted) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  monitor.memoryLimit(100000);

  for (size_t i = 0; i < 1000; ++i) {
    if ((i + 1) * 1000 < ::Bucketize(100000) + ResourceMonitor::kChunkSize) {
      monitor.increaseMemoryUsage(1000);
      ASSERT_EQ((i + 1) * 1000, monitor.current());
      ASSERT_EQ(::Bucketize((i + 1) * 1000), monitor.peak());
    } else {
      ASSERT_THROW(
        {
          try {
            monitor.increaseMemoryUsage(1000);
          } catch (const basics::Exception& ex) {
            ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
            throw;
          }
        },
        basics::Exception);
    }
  }

  //  ASSERT_EQ(::bucketize(100000), monitor.current());
  ASSERT_EQ(::Bucketize(100000), monitor.peak());

  monitor.decreaseMemoryUsage(monitor.current());
}

TEST(ResourceUsageTest, testIncreaseInStepsUnrestricted) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  for (size_t i = 0; i < 1000; ++i) {
    monitor.increaseMemoryUsage(1000);
  }

  ASSERT_EQ(1000000, monitor.current());
  ASSERT_EQ(::Bucketize(1000000), monitor.peak());

  monitor.decreaseMemoryUsage(monitor.current());
}

TEST(ResourceUsageTest, testConcurrencyRestricted) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  monitor.memoryLimit(123456);

  std::atomic<bool> go = false;
  std::atomic<size_t> global_rejections = 0;

  auto threads = ThreadGuard(::kNumThreads);

  for (size_t i = 0; i < ::kNumThreads; ++i) {
    threads.emplace([&]() {
      while (!go.load()) {
        // wait until all threads are created, so they can
        // start at the approximate same time
      }
      size_t total_added = 0;
      size_t rejections = 0;
      for (uint64_t i = 0; i < ::kNumOpsPerThread; ++i) {
        if (!monitor.increaseMemoryUsageNT(kAmount)) {
          total_added += kAmount;
        } else {
          ++rejections;
        }
      }

      monitor.decreaseMemoryUsage(total_added);
      global_rejections += rejections;
    });
  }

  go.store(true);

  threads.joinAll();

  // should be down to 0 now
  EXPECT_EQ(0, monitor.current());
  EXPECT_LE(monitor.peak(), ::Bucketize(monitor.memoryLimit()));

  // should be way above 0
  EXPECT_GT(global_rejections, 0);
  EXPECT_EQ(0, global.current());
}

TEST(ResourceUsageTest, testConcurrencyUnrestricted) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  std::atomic<bool> go = false;
  constexpr size_t kAmount = 123;

  auto threads = ThreadGuard(::kNumThreads);

  for (size_t i = 0; i < ::kNumThreads; ++i) {
    threads.emplace([&]() {
      while (!go.load()) {
        // wait until all threads are created, so they can
        // start at the approximate same time
      }
      for (uint64_t i = 0; i < ::kNumOpsPerThread; ++i) {
        monitor.increaseMemoryUsage(kAmount);
      }

      monitor.decreaseMemoryUsage(::kNumOpsPerThread * kAmount);
    });
  }

  go.store(true);

  threads.joinAll();

  // should be down to 0 now
  ASSERT_EQ(0, monitor.current());

  ASSERT_GE(monitor.peak(), ::Bucketize(::kNumOpsPerThread * kAmount));
}

TEST(ResourceUsageTest, testMemoryLocalLimitViolationCounter) {
  GlobalResourceMonitor global;
  ResourceMonitor monitor(global);

  monitor.memoryLimit(65535);

  auto stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  ResourceUsageScope scope(monitor);
  scope.increase(32768);
  scope.increase(32767);

  stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  try {
    scope.increase(1);
    throw "fail!";
  } catch (const basics::Exception& ex) {
    ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
  }

  stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(1, stats.local_limit_reached);

  try {
    scope.increase(1);
    throw "fail!";
  } catch (const basics::Exception& ex) {
    ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
  }

  stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(2, stats.local_limit_reached);
}

TEST(ResourceUsageTest, testGlobalMemoryLimitViolationCounter) {
  GlobalResourceMonitor global;
  global.memoryLimit(65535);

  ResourceMonitor monitor(global);

  auto stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  ResourceUsageScope scope(monitor);
  scope.increase(32768);
  scope.increase(32767);

  stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  try {
    scope.increase(1);
    throw "fail!";
  } catch (const basics::Exception& ex) {
    ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
  }

  stats = global.stats();
  ASSERT_EQ(1, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  try {
    scope.increase(1);
    throw "fail!";
  } catch (const basics::Exception& ex) {
    ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
  }

  stats = global.stats();
  ASSERT_EQ(2, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);
}

TEST(ResourceUsageTest, testGlobalMemoryLimitViolationCounterHitByMultiple) {
  GlobalResourceMonitor global;
  global.memoryLimit(65535);

  ResourceMonitor monitor1(global);
  ResourceMonitor monitor2(global);

  auto stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  ResourceUsageScope scope1(monitor1);
  ResourceUsageScope scope2(monitor2);
  scope1.increase(16384);
  scope2.increase(16384);
  scope1.increase(16384);

  stats = global.stats();
  ASSERT_EQ(0, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  try {
    scope2.increase(16384);
    throw "fail!";
  } catch (const basics::Exception& ex) {
    ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
  }

  stats = global.stats();
  ASSERT_EQ(1, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);

  try {
    scope1.increase(163841);
    throw "fail!";
  } catch (const basics::Exception& ex) {
    ASSERT_EQ(sdb::ERROR_RESOURCE_LIMIT, ex.code());
  }

  stats = global.stats();
  ASSERT_EQ(2, stats.global_limit_reached);
  ASSERT_EQ(0, stats.local_limit_reached);
}

TEST(GlobalResourceMonitorTest, testEmpty) {
  GlobalResourceMonitor monitor;

  ASSERT_EQ(0, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());

  monitor.memoryLimit(123456);
  ASSERT_EQ(123456, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());
}

TEST(GlobalResourceMonitorTest, testBasicRestrictions) {
  GlobalResourceMonitor monitor;

  ASSERT_EQ(0, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());

  monitor.memoryLimit(10000);
  ASSERT_EQ(10000, monitor.memoryLimit());
  ASSERT_EQ(0, monitor.current());

  ASSERT_FALSE(monitor.increaseMemoryUsage(10001));
  ASSERT_EQ(0, monitor.current());

  ASSERT_TRUE(monitor.increaseMemoryUsage(10000));
  ASSERT_EQ(10000, monitor.current());

  ASSERT_FALSE(monitor.increaseMemoryUsage(1));
  ASSERT_EQ(10000, monitor.current());

  monitor.decreaseMemoryUsage(1000);
  ASSERT_EQ(9000, monitor.current());

  ASSERT_TRUE(monitor.increaseMemoryUsage(1000));
  ASSERT_EQ(10000, monitor.current());

  ASSERT_FALSE(monitor.increaseMemoryUsage(1));
  ASSERT_EQ(10000, monitor.current());
}

TEST(GlobalResourceMonitorTest, testIncreaseInStepsRestricted) {
  GlobalResourceMonitor monitor;

  monitor.memoryLimit(100000);

  for (size_t i = 0; i < 1000; ++i) {
    if (i < 100) {
      ASSERT_TRUE(monitor.increaseMemoryUsage(1000));
    } else {
      ASSERT_FALSE(monitor.increaseMemoryUsage(1000));
      ASSERT_EQ(100000, monitor.current());
    }
  }

  ASSERT_EQ(100000, monitor.current());
}

TEST(GlobalResourceMonitorTest, testIncreaseInStepsUnrestricted) {
  GlobalResourceMonitor monitor;

  for (size_t i = 0; i < 1000; ++i) {
    ASSERT_TRUE(monitor.increaseMemoryUsage(1000));
  }

  ASSERT_EQ(1000000, monitor.current());
}

TEST(GlobalResourceMonitorTest, testConcurrencyRestricted) {
  GlobalResourceMonitor monitor;

  monitor.memoryLimit(123456);

  std::atomic<bool> go = false;
  std::atomic<size_t> global_rejections = 0;

  auto threads = ThreadGuard(::kNumThreads);

  for (size_t i = 0; i < ::kNumThreads; ++i) {
    threads.emplace([&]() {
      while (!go.load()) {
        // wait until all threads are created, so they can
        // start at the approximate same time
      }
      size_t total_added = 0;
      size_t rejections = 0;
      for (uint64_t i = 0; i < ::kNumOpsPerThread; ++i) {
        bool ok = monitor.increaseMemoryUsage(kAmount);
        if (ok) {
          total_added += kAmount;
        } else {
          ++rejections;
        }
      }

      monitor.decreaseMemoryUsage(total_added);
      global_rejections += rejections;
    });
  }

  go.store(true);

  threads.joinAll();

  // should be down to 0 now
  ASSERT_EQ(0, monitor.current());

  // should be way above 0
  ASSERT_GT(global_rejections, 0);
}

TEST(GlobalResourceMonitorTest, testConcurrencyUnrestricted) {
  GlobalResourceMonitor monitor;

  std::atomic<bool> go = false;

  auto threads = ThreadGuard(::kNumThreads);
  for (size_t i = 0; i < ::kNumThreads; ++i) {
    threads.emplace([&]() {
      while (!go.load()) {
        // wait until all threads are created, so they can
        // start at the approximate same time
      }
      for (uint64_t i = 0; i < ::kNumOpsPerThread; ++i) {
        ASSERT_TRUE(monitor.increaseMemoryUsage(kAmount));
      }

      monitor.decreaseMemoryUsage(::kNumOpsPerThread * kAmount);
    });
  }

  go.store(true);

  threads.joinAll();

  // should be down to 0 now
  ASSERT_EQ(0, monitor.current());
}
