////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <atomic>
#include <basics/async_utils.hpp>
#include <basics/thread_utils.hpp>
#include <condition_variable>
#include <mutex>

#include "tests_shared.hpp"

using namespace std::chrono_literals;

namespace tests {

class AsyncUtilsTests : public ::testing::Test {
  void SetUp() final {
    // Code here will be called immediately after the constructor (right before
    // each test).
  }

  void TearDown() final {
    // Code here will be called immediately after each test (right before the
    // destructor).
  }
};

class NotifyingCounter {
 public:
  NotifyingCounter(std::condition_variable& cond, size_t notify_after)
    : _cond(cond), _count(0), _notify_after(notify_after) {}
  NotifyingCounter& operator++() {
    std::lock_guard<decltype(_lock)> lock(_lock);
    if (++_count >= _notify_after)
      _cond.notify_all();
    return *this;
  }
  explicit operator bool() {
    std::lock_guard<decltype(_lock)> lock(_lock);
    return _count >= _notify_after;
  }

 private:
  std::condition_variable& _cond;
  size_t _count;
  std::mutex _lock;
  size_t _notify_after;
};

template<bool UseDelay>
void RunThreadPoolBoundMt() {
  // test max threads
  {
    irs::async_utils::ThreadPool<UseDelay> pool(2);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    auto task3 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    std::unique_lock lock(mutex);

    ASSERT_EQ(2, pool.threads());
    pool.run(std::move(task1));
    pool.run(std::move(task2));
    pool.run(std::move(task3));
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (1 != pool.tasks_pending() || 2 != pool.tasks_active() ||
             count != 2) {
        std::this_thread::sleep_for(10ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(2, count);  // 2 tasks started
    ASSERT_EQ(2, pool.threads());
    ASSERT_EQ(2, pool.tasks_active());
    ASSERT_EQ(1, pool.tasks_pending());
    lock.unlock();
    pool.stop(true);
  }

  // test max threads delta grow
  {
    irs::async_utils::ThreadPool<UseDelay> pool(1);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    std::unique_lock lock(mutex);

    ASSERT_EQ(1, pool.threads());
    pool.run(std::move(task));
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (0 != pool.tasks_pending() || 1 != pool.tasks_active() ||
             count != 1) {
        std::this_thread::sleep_for(10ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(1, count);  // 1 task started
    ASSERT_EQ(1, pool.threads());
    ASSERT_EQ(1, pool.tasks_active());
    ASSERT_EQ(0, pool.tasks_pending());
    lock.unlock();
    pool.stop(true);
  }

  // test max idle
  {
    irs::async_utils::ThreadPool<UseDelay> pool(3);
    std::atomic<size_t> count(0);
    std::mutex mutex1;
    std::mutex mutex2;
    std::condition_variable start_cond;
    NotifyingCounter start_count(start_cond, 3);
    std::mutex start_mutex;
    auto task1 = [&start_mutex, &start_count, &mutex1, &count]() -> void {
      {
        std::lock_guard lock(start_mutex);
      }
      ++start_count;
      std::lock_guard lock(mutex1);
      ++count;
    };
    auto task2 = [&start_mutex, &start_count, &mutex1, &count]() -> void {
      {
        std::lock_guard lock(start_mutex);
      }
      ++start_count;
      std::lock_guard lock(mutex1);
      ++count;
    };
    auto task3 = [&start_mutex, &start_count, &mutex2, &count]() -> void {
      {
        std::lock_guard lock(start_mutex);
      }
      ++start_count;
      std::lock_guard lock(mutex2);
      ++count;
    };
    std::unique_lock lock1(mutex1);
    std::unique_lock lock2(mutex2);
    std::unique_lock start_lock(start_mutex);

    ASSERT_EQ(3, pool.threads());
    pool.run(std::move(task1));
    pool.run(std::move(task2));
    pool.run(std::move(task3));
    ASSERT_TRUE(start_count ||
                std::cv_status::no_timeout ==
                  start_cond.wait_for(start_lock, 10000ms) ||
                start_count);  // wait for all 3 tasks to start
    ASSERT_EQ(0, count);       // 0 tasks complete
    ASSERT_EQ(3, pool.threads());
    ASSERT_EQ(3, pool.tasks_active());
    ASSERT_EQ(0, pool.tasks_pending());
    ASSERT_EQ(std::make_tuple(size_t(3), size_t(0), size_t(3)), pool.stats());
    lock1.unlock();
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (count != 2) {
        std::this_thread::sleep_for(10ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(2, count);  // 2 tasks complete
    lock2.unlock();
    pool.stop(true);
  }
}

template<bool UseDelay>
void RunTestThreadPoolRunMt() {
  // test schedule 1 task
  {
    irs::async_utils::ThreadPool<UseDelay> pool(1);
    std::condition_variable cond;
    std::mutex mutex;
    std::unique_lock lock(mutex);
    auto task = [&mutex, &cond]() -> void {
      std::lock_guard lock(mutex);
      cond.notify_all();
    };

    pool.run(std::move(task));
    ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
  }

  // test schedule 3 task sequential
  {
    irs::async_utils::ThreadPool<UseDelay> pool(1);
    std::condition_variable cond;
    NotifyingCounter count(cond, 3);
    std::mutex mutex;
    std::mutex sync_mutex;
    auto task1 = [&mutex, &sync_mutex, &count]() -> void {
      {
        std::lock_guard lock(mutex);
      }
      std::unique_lock lock(sync_mutex, std::try_to_lock);
      if (lock.owns_lock())
        ++count;
      std::this_thread::sleep_for(300ms);
    };
    auto task2 = [&mutex, &sync_mutex, &count]() -> void {
      {
        std::lock_guard lock(mutex);
      }
      std::unique_lock lock(sync_mutex, std::try_to_lock);
      if (lock.owns_lock())
        ++count;
      std::this_thread::sleep_for(300ms);
    };
    auto task3 = [&mutex, &sync_mutex, &count]() -> void {
      {
        std::lock_guard lock(mutex);
      }
      std::unique_lock lock(sync_mutex, std::try_to_lock);
      if (lock.owns_lock())
        ++count;
      std::this_thread::sleep_for(300ms);
    };
    std::unique_lock lock(mutex);

    pool.run(std::move(task1));
    pool.run(std::move(task2));
    pool.run(std::move(task3));
    ASSERT_EQ(std::cv_status::no_timeout,
              cond.wait_for(lock, 1000ms));  // wait for all 3 tasks
    pool.stop();
  }

  // test schedule 3 task parallel
  {
    irs::async_utils::ThreadPool<UseDelay> pool(3);
    std::condition_variable cond;
    NotifyingCounter count(cond, 3);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    auto task3 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    std::unique_lock lock(mutex);

    ASSERT_TRUE(pool.run(std::move(task1)));
    ASSERT_TRUE(pool.run(std::move(task2)));
    ASSERT_TRUE(pool.run(std::move(task3)));
    ASSERT_TRUE(count ||
                std::cv_status::no_timeout == cond.wait_for(lock, 1000ms) ||
                count);  // wait for all 3 tasks
    lock.unlock();
    pool.stop();
  }

  // test schedule 1 task exception + 1 task
  {
    irs::async_utils::ThreadPool<UseDelay> pool(1, IR_NATIVE_STRING("foo"));
    std::condition_variable cond;
    NotifyingCounter count(cond, 2);
    std::mutex mutex;
    auto task1 = [&count]() -> void {
      ++count;
      throw "error";
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    std::unique_lock lock(mutex);
    std::mutex dummy_mutex;
    std::unique_lock dummy_lock(dummy_mutex);

    pool.run(std::move(task1));
    pool.run(std::move(task2));
    ASSERT_TRUE(count ||
                std::cv_status::no_timeout ==
                  cond.wait_for(dummy_lock, 10000ms) ||
                count);  // wait for all 2 tasks (exception trace is slow on
                         // MSVC and even slower on *NIX with gdb)
    ASSERT_EQ(1, pool.threads());
    lock.unlock();
    pool.stop(true);
  }
}

TEST_F(AsyncUtilsTests, test_thread_pool_run_mt) {
  RunTestThreadPoolRunMt<true>();
}

TEST_F(AsyncUtilsTests, test_thread_pool_bound_mt) {
  RunThreadPoolBoundMt<true>();
}

TEST_F(AsyncUtilsTests, test_thread_pool_stop_delay_mt) {
  // test stop run pending
  {
    irs::async_utils::ThreadPool<> pool(1);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
      std::this_thread::sleep_for(300ms);
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
      std::this_thread::sleep_for(300ms);
    };
    std::unique_lock lock(mutex);

    pool.run(std::move(task1), absl::Milliseconds(30));
    pool.run(std::move(task2), absl::Milliseconds(500));
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (1 != pool.tasks_pending() && 1 != pool.tasks_active()) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(1, pool.tasks_pending());
    ASSERT_EQ(1, pool.tasks_active());
    ASSERT_EQ(1, count);
    lock.unlock();
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (count.load() < 2) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (pool.tasks_active()) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(0, pool.tasks_active());
    ASSERT_EQ(0, pool.tasks_pending());
    ASSERT_EQ(1, pool.threads());
    ASSERT_EQ(std::make_tuple(size_t(0), size_t(0), size_t(1)), pool.stats());
    pool.stop();  // blocking call (thread runtime duration simulated via sleep)
    ASSERT_EQ(2, count);  // all tasks ran
    ASSERT_EQ(0, pool.tasks_active());
    ASSERT_EQ(0, pool.tasks_pending());
    ASSERT_EQ(0, pool.threads());
    ASSERT_EQ(std::make_tuple(size_t(0), size_t(0), size_t(0)), pool.stats());
  }
}

TEST_F(AsyncUtilsTests, test_thread_pool_max_idle_mt) {
  // test stop run pending
  {
    irs::async_utils::ThreadPool<> pool(4);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
    };
    auto task3 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
    };
    auto task4 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
    };
    std::unique_lock lock(mutex);

    pool.run(std::move(task1));
    pool.run(std::move(task2));
    pool.run(std::move(task3), absl::Milliseconds(30));
    pool.run(std::move(task4), absl::Milliseconds(500));
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (count.load() < 4) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(0, pool.tasks_pending());
    ASSERT_EQ(4, pool.tasks_active());
    ASSERT_EQ(4, count);
    lock.unlock();
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (pool.tasks_active()) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    {
      const auto end = std::chrono::steady_clock::now() +
                       10s;  // assume 10s is more than enough
      while (pool.tasks_pending()) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(0, pool.tasks_active());
    ASSERT_EQ(0, pool.tasks_pending());
    ASSERT_EQ(4, pool.threads());
    ASSERT_EQ(std::make_tuple(size_t(0), size_t(0), size_t(4)), pool.stats());
    pool.stop();  // blocking call (thread runtime duration simulated via sleep)
    ASSERT_EQ(4, count);  // all tasks ran
    ASSERT_EQ(0, pool.tasks_active());
    ASSERT_EQ(0, pool.tasks_pending());
    ASSERT_EQ(0, pool.threads());
    ASSERT_EQ(std::make_tuple(size_t(0), size_t(0), size_t(0)), pool.stats());
  }
}

TEST_F(AsyncUtilsTests, test_thread_pool_stop_mt) {
  // test stop run pending
  {
    irs::async_utils::ThreadPool<> pool(1);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
      std::this_thread::sleep_for(300ms);
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
      std::this_thread::sleep_for(300ms);
    };
    std::unique_lock lock(mutex);

    pool.run(std::move(task1));
    pool.run(std::move(task2));
    lock.unlock();
    pool.stop();  // blocking call (thread runtime duration simulated via sleep)
    ASSERT_EQ(2, count);  // all tasks ran
  }

  // test stop skip pending
  {
    irs::async_utils::ThreadPool<> pool(1);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
      std::this_thread::sleep_for(300ms);
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      {
        std::lock_guard lock(mutex);
      }
      std::this_thread::sleep_for(300ms);
    };
    std::unique_lock lock(mutex);

    pool.run(std::move(task1));
    pool.run(std::move(task2));
    {
      // assume 10s is more than enough to start first thread
      const auto end = std::chrono::steady_clock::now() + 10s;
      while (count.load() == 0) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    lock.unlock();
    pool.stop(
      true);  // blocking call (thread runtime duration simulated via sleep)
    ASSERT_EQ(1, count);  // only 1 task ran
  }

  // test pool stop + run
  {
    irs::async_utils::ThreadPool<> pool(1);
    std::atomic<size_t> count(0);
    std::mutex mutex;
    auto task1 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    auto task2 = [&mutex, &count]() -> void {
      ++count;
      std::lock_guard lock(mutex);
    };
    std::unique_lock lock(mutex);

    ASSERT_EQ(1, pool.threads());
    pool.run(std::move(task1));
    {
      // assume 10s is more than enough to start first thread
      const auto end = std::chrono::steady_clock::now() + 10s;
      while (count.load() == 0) {
        std::this_thread::sleep_for(100ms);
        ASSERT_LE(std::chrono::steady_clock::now(), end);
      }
    }
    ASSERT_EQ(1, count);  // 1 task started
    ASSERT_EQ(1, pool.threads());
    lock.unlock();
    pool.stop(true);
    ASSERT_FALSE(pool.run(std::move(task2)));
  }

  // test multiple calls to stop will all block
  {
    irs::async_utils::ThreadPool<> pool(1);
    std::condition_variable cond;
    std::mutex mutex;
    std::unique_lock lock(mutex);
    auto task = [&mutex, &cond]() -> void {
      std::lock_guard lock(mutex);
      cond.notify_all();
    };

    ASSERT_EQ(1, pool.threads());
    pool.run(std::move(task));
    ASSERT_EQ(1, pool.threads());

    std::condition_variable cond2;
    std::mutex mutex2;
    std::unique_lock lock2(mutex2);
    std::atomic<bool> stop(false);
    std::thread thread1([&pool, &mutex2, &cond2, &stop]() -> void {
      pool.stop();
      stop = true;
      std::lock_guard lock(mutex2);
      cond2.notify_all();
    });
    std::thread thread2([&pool, &mutex2, &cond2, &stop]() -> void {
      pool.stop();
      stop = true;
      std::lock_guard lock(mutex2);
      cond2.notify_all();
    });

    auto result =
      cond.wait_for(lock2, 1000ms);  // assume thread blocks in 1000ms

    // As declaration for wait_for contains "It may also be unblocked
    // spuriously." for all platforms
    while (!stop && result == std::cv_status::no_timeout)
      result = cond2.wait_for(lock2, 1000ms);

    ASSERT_EQ(std::cv_status::timeout, result);
    // ^^^ expecting timeout because pool should block indefinitely
    lock2.unlock();
    ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 1000ms));
    thread1.join();
    thread2.join();
  }

  // test stop with a single thread will stop threads
  {
    irs::async_utils::ThreadPool<> pool(1);

    pool.run([]() -> void {});  // start a single thread
    ASSERT_EQ(1, pool.threads());
    pool.stop();
    ASSERT_EQ(0, pool.threads());
  }
}

TEST_F(AsyncUtilsTests, test_queue_thread_pool_run_mt) {
  RunTestThreadPoolRunMt<false>();
}

TEST_F(AsyncUtilsTests, test_queue_thread_pool_bound_mt) {
  RunThreadPoolBoundMt<false>();
}

TEST_F(AsyncUtilsTests, test_queue_thread_pool_delay_mt) {
  {
    uint64_t counter{0};
    uint64_t counter_start{0};
    irs::async_utils::ThreadPool<false> pool(1);
    std::condition_variable cond;
    std::mutex mutex;
    std::unique_lock lock(mutex);
    auto task = [&]() -> void {
      std::lock_guard lock(mutex);
      ++counter;
      ++counter_start;
      if (counter_start == 2) {
        cond.notify_all();
      }
    };
    auto task2 = [&]() -> void {
      std::lock_guard lock(mutex);
      if (counter > 0) {
        --counter;
      } else {
        ++counter;
      }
      ++counter_start;
      if (counter_start == 2) {
        cond.notify_all();
      }
    };

    ASSERT_EQ(1, pool.threads());
    // delay is ignored for non priority qeue
    // tasks are executed as is
    pool.run(std::move(task), absl::Seconds(10000));
    pool.run(std::move(task2), absl::Seconds(1));
    ASSERT_EQ(std::cv_status::no_timeout, cond.wait_for(lock, 100s));
    ASSERT_EQ(0, counter);
  }
}

TEST(thread_utils_test, get_set_name) {
  const THREAD_NAME_T expected_name = IR_NATIVE_STRING("foo");
#if (defined(__linux__) || defined(__APPLE__) || \
     (defined(_WIN32) && (_WIN32_WINNT >= _WIN32_WINNT_WIN10)))
  std::basic_string<std::remove_pointer_t<THREAD_NAME_T>> actual_name;

  std::thread thread([expected_name, &actual_name]() mutable {
    EXPECT_TRUE(irs::SetThreadName(expected_name));
    EXPECT_TRUE(irs::GetThreadName(actual_name));
  });

  thread.join();
  ASSERT_EQ(expected_name, actual_name);
#else
  std::thread thread([expected_name, &actual_name]() mutable {
    EXPECT_FALSE(irs::SetThreadName(expected_name));
  });
#endif
}

}  // namespace tests
