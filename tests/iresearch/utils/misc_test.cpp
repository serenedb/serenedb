////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <atomic>
#include <chrono>
#include <iresearch/utils/misc.hpp>
#include <stdexcept>
#include <thread>
#include <vector>

#include "tests_shared.hpp"

TEST(misc_test, cached_func_contexpr) {
  constexpr auto kCachedFunc =
    irs::CacheFunc<uint32_t, 3>(0, [](uint32_t v) { return v + 1; });
  static_assert(3 == kCachedFunc.size());
  static_assert(1 == kCachedFunc.get<false>(0));
  static_assert(2 == kCachedFunc.get<true>(1));
  static_assert(3 == kCachedFunc.get<true>(2));
  static_assert(4 == kCachedFunc.get<true>(3));
  static_assert(5 == kCachedFunc.get<true>(4));
}

TEST(misc_test, cached_func) {
  size_t calls = 0;
  auto cached_func = irs::CacheFunc<uint32_t, 3>(0, [&](uint32_t v) {
    ++calls;
    return v + 1;
  });
  ASSERT_EQ(calls, cached_func.size());
  ASSERT_EQ(3, cached_func.size());
  ASSERT_EQ(3, cached_func.size());
  ASSERT_EQ(1, cached_func.get<true>(0));
  ASSERT_EQ(3, calls);
  ASSERT_EQ(2, cached_func.get<true>(1));
  ASSERT_EQ(3, calls);
  ASSERT_EQ(3, cached_func.get<false>(2));
  ASSERT_EQ(3, calls);
  ASSERT_EQ(4, cached_func.get<true>(3));
  ASSERT_EQ(4, calls);
  ASSERT_EQ(5, cached_func.get<true>(4));
  ASSERT_EQ(5, calls);
}

TEST(misc_test, once_flag_runs_its_function_once) {
  irs::OnceFlag once;
  size_t calls = 0;
  for (size_t i = 0; i < 3; ++i) {
    once.Call([&] { ++calls; });
  }
  ASSERT_EQ(1, calls);
}

TEST(misc_test, once_flag_rethrows_a_failed_call) {
  irs::OnceFlag once;
  size_t calls = 0;
  const auto fail = [&] {
    ++calls;
    throw std::runtime_error{"load failed"};
  };
  ASSERT_THROW(once.Call(fail), std::runtime_error);
  ASSERT_THROW(once.Call(fail), std::runtime_error);
  ASSERT_THROW(once.Call([&] { ++calls; }), std::runtime_error);
  ASSERT_EQ(1, calls);
}

TEST(misc_test, once_flag_rethrows_to_concurrent_callers) {
  irs::OnceFlag once;
  std::atomic<size_t> calls{0};
  std::atomic<size_t> failures{0};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back([&] {
      try {
        once.Call([&] {
          calls.fetch_add(1);
          std::this_thread::sleep_for(std::chrono::milliseconds{10});
          throw std::runtime_error{"load failed"};
        });
      } catch (const std::runtime_error&) {
        failures.fetch_add(1);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_EQ(1, calls.load());
  ASSERT_EQ(8, failures.load());
}
