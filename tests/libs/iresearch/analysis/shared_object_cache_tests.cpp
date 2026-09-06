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

#include <duckdb.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/storage/buffer/buffer_pool.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/shared_object_cache.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

struct SharedTestObject final : duckdb::ObjectCacheEntry {
  static inline std::atomic<int> alive{0};
  static inline std::atomic<int> builds{0};

  SharedTestObject(int value, duckdb::idx_t size) : value{value}, size{size} {
    ++alive;
    ++builds;
  }
  ~SharedTestObject() final { --alive; }

  static constexpr std::string_view ObjectType() { return "SharedTestObject"; }
  std::string GetObjectType() final { return std::string{ObjectType()}; }
  duckdb::optional_idx GetEstimatedCacheMemory() const final { return size; }

  int value;
  duckdb::idx_t size;
};

struct SharedObjectCacheTest : ::testing::Test {
  static constexpr duckdb::idx_t kObjectSize = 1024 * 1024;

  duckdb::DuckDB db{nullptr};
  duckdb::SharedObjectCache& cache{db.instance->GetSharedObjectCache()};
  duckdb::BufferPool& pool{
    duckdb::BufferManager::GetBufferManager(*db.instance).GetBufferPool()};
  const duckdb::idx_t initial_memory{pool.GetUsedMemory()};

  void SetUp() final {
    SharedTestObject::alive = 0;
    SharedTestObject::builds = 0;
  }

  static duckdb::unique_ptr<SharedTestObject> Build() {
    return duckdb::make_uniq<SharedTestObject>(42, kObjectSize);
  }
};

TEST_F(SharedObjectCacheTest, same_key_yields_same_object_while_referenced) {
  auto a = cache.GetOrBuild<SharedTestObject>("shared", Build);
  auto b = cache.GetOrBuild<SharedTestObject>("shared", Build);
  auto c = cache.GetOrBuild<SharedTestObject>("other", Build);
  EXPECT_EQ(a.get(), b.get());
  EXPECT_NE(a.get(), c.get());
  EXPECT_EQ(2, SharedTestObject::builds.load());
  EXPECT_EQ(2, SharedTestObject::alive.load());
  EXPECT_EQ(2u, cache.GetEntryCount());
  EXPECT_EQ(2 * kObjectSize, cache.GetMemoryUsage());
  EXPECT_EQ(initial_memory + 2 * kObjectSize, pool.GetUsedMemory());

  b.reset();
  c.reset();
  EXPECT_EQ(1, SharedTestObject::alive.load());
  EXPECT_EQ(initial_memory + kObjectSize, pool.GetUsedMemory());
  a.reset();
  EXPECT_EQ(0, SharedTestObject::alive.load());
  EXPECT_EQ(0u, cache.GetEntryCount());
  EXPECT_EQ(0u, cache.GetMemoryUsage());
  EXPECT_EQ(initial_memory, pool.GetUsedMemory());

  auto again = cache.GetOrBuild<SharedTestObject>("shared", Build);
  EXPECT_EQ(3, SharedTestObject::builds.load());
  EXPECT_EQ(1, SharedTestObject::alive.load());
}

TEST_F(SharedObjectCacheTest, get_sees_live_entries_only) {
  auto a = cache.GetOrBuild<SharedTestObject>("shared", Build);
  auto found = cache.Get<SharedTestObject>("shared");
  EXPECT_EQ(a.get(), found.get());
  a.reset();
  found.reset();
  EXPECT_EQ(nullptr, cache.Get<SharedTestObject>("shared").get());
}

TEST_F(SharedObjectCacheTest, throwing_build_caches_nothing) {
  const auto failing = []() -> duckdb::unique_ptr<SharedTestObject> {
    throw duckdb::InternalException("build failed");
  };
  EXPECT_THROW(cache.GetOrBuild<SharedTestObject>("boom", failing),
               duckdb::InternalException);
  EXPECT_EQ(0u, cache.GetEntryCount());
  EXPECT_NE(nullptr, cache.GetOrBuild<SharedTestObject>("boom", Build).get());
}

TEST_F(SharedObjectCacheTest, null_build_is_an_internal_error) {
  const auto null_build = []() -> duckdb::unique_ptr<SharedTestObject> {
    return nullptr;
  };
  EXPECT_THROW(cache.GetOrBuild<SharedTestObject>("null", null_build),
               duckdb::InternalException);
  EXPECT_EQ(0u, cache.GetEntryCount());
  EXPECT_NE(nullptr, cache.GetOrBuild<SharedTestObject>("null", Build).get());
}

TEST_F(SharedObjectCacheTest, builds_are_single_flight) {
  constexpr size_t kThreads = 8;
  std::atomic<int> started{0};
  const auto slow_build = [&] {
    ++started;
    std::this_thread::sleep_for(std::chrono::milliseconds{50});
    return Build();
  };
  std::array<duckdb::shared_ptr<SharedTestObject>, kThreads> results;
  {
    std::vector<std::jthread> threads;
    for (size_t idx = 0; idx < kThreads; ++idx) {
      threads.emplace_back([&, idx] {
        results[idx] = cache.GetOrBuild<SharedTestObject>("raced", slow_build);
      });
    }
  }
  for (size_t idx = 1; idx < kThreads; ++idx) {
    EXPECT_EQ(results[0].get(), results[idx].get());
  }
  EXPECT_EQ(1, SharedTestObject::builds.load());
  EXPECT_EQ(1, started.load());
  EXPECT_EQ(1, SharedTestObject::alive.load());
  EXPECT_EQ(initial_memory + kObjectSize, pool.GetUsedMemory());
}

TEST_F(SharedObjectCacheTest, failed_build_hands_key_to_next_waiter) {
  std::mutex m;
  std::condition_variable cv;
  bool release_failure = false;
  std::atomic<int> failing_started{0};
  bool threw = false;

  std::thread failing([&] {
    try {
      cache.GetOrBuild<SharedTestObject>(
        "handoff", [&]() -> duckdb::unique_ptr<SharedTestObject> {
          ++failing_started;
          std::unique_lock<std::mutex> lock{m};
          cv.wait(lock, [&] { return release_failure; });
          throw duckdb::InternalException("build failed");
        });
    } catch (const duckdb::InternalException&) {
      threw = true;
    }
  });
  while (failing_started == 0) {
    std::this_thread::yield();
  }

  duckdb::shared_ptr<SharedTestObject> result;
  std::thread waiting([&] {
    result = cache.GetOrBuild<SharedTestObject>("handoff", Build);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds{10});
  {
    const std::lock_guard<std::mutex> lock{m};
    release_failure = true;
  }
  cv.notify_all();
  failing.join();
  waiting.join();
  EXPECT_TRUE(threw);
  ASSERT_NE(nullptr, result.get());
  EXPECT_EQ(42, result->value);
  EXPECT_EQ(1, SharedTestObject::builds.load());
}

}  // namespace
