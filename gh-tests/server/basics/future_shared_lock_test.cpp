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

#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "basics/future_shared_lock.h"
#include "gtest/gtest.h"

namespace {

using namespace std::chrono_literals;

struct MockScheduler;
using FutureSharedLock = sdb::FutureSharedLock<MockScheduler>;

struct MockScheduler {
  template<typename Fn>
  void queue(Fn func) {
    funcs.push_back({std::move(func)});
  }

  using WorkHandle = int;

  template<typename Fn>
  WorkHandle queueDelayed(Fn func, std::chrono::milliseconds delay) {
    delayed_funcs.emplace_back(std::move(func), delay);
    return 0;
  }

  void ExecuteScheduled() {
    // the called functions might queue new stuff, so we need to move out
    // everything and clear the queue beforehand.
    decltype(funcs) ff = std::move(funcs);
    funcs.clear();
    for (auto& fn : ff) {
      fn();
    }
  }

  void ExecuteNextDelayed() {
    ASSERT_FALSE(delayed_funcs.empty());
    auto f = std::move(delayed_funcs.front());
    delayed_funcs.pop_front();
    f.first(false);
  }
  std::vector<absl::AnyInvocable<void()>> funcs;
  std::deque<std::pair<std::function<void(bool)>, std::chrono::milliseconds>>
    delayed_funcs;
};

struct FutureSharedLockTest : public ::testing::Test {
  FutureSharedLockTest() : lock(scheduler) {}
  void TearDown() override {
    ASSERT_EQ(0, scheduler.funcs.size());
    ASSERT_EQ(0, scheduler.delayed_funcs.size());
  }

  MockScheduler scheduler;
  FutureSharedLock lock;
};

TEST_F(FutureSharedLockTest,
       asyncLockExclusive_should_return_resolved_future_when_unlocked) {
  int called = 0;
  lock.asyncLockExclusive().DetachInline([&](auto) { ++called; });
  EXPECT_EQ(1, called);

  lock.asyncLockExclusive().DetachInline([&](auto) { ++called; });
  EXPECT_EQ(2, called);
}

TEST_F(FutureSharedLockTest,
       asyncLockExclusive_should_return_unresolved_future_when_locked) {
  lock.asyncLockExclusive().DetachInline([&](auto) {
    // try to lock again while we hold the exclusive lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncLockExclusive();
    EXPECT_FALSE(fut.Ready());
  });
  scheduler.ExecuteScheduled();  // cleanup
}

TEST_F(FutureSharedLockTest,
       unlock_should_post_the_next_owner_on_the_scheduler) {
  int called = 0;
  lock.asyncLockExclusive().DetachInline([&](auto) {
    ++called;
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    // we still hold the lock, so nothing must be queued on the scheduler yet
    EXPECT_EQ(0, scheduler.funcs.size());
  });
  EXPECT_EQ(1, called);
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(2, called);
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(3, called);
}

TEST_F(
  FutureSharedLockTest,
  asyncLockExclusive_should_return_unresolved_future_when_predecessor_has_shared_lock) {
  lock.asyncLockShared().DetachInline([&](auto) {
    // try to acquire exclusive lock while we hold the shared lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncLockExclusive();
    EXPECT_FALSE(fut.Ready());
  });
  scheduler.ExecuteScheduled();  // cleanup
}

TEST_F(FutureSharedLockTest,
       asyncLockShared_should_return_resolved_future_when_unlocked) {
  int called = 0;
  lock.asyncLockShared().DetachInline([&](auto) { ++called; });
  EXPECT_EQ(1, called);

  lock.asyncLockShared().DetachInline([&](auto) { ++called; });
  EXPECT_EQ(2, called);
}

TEST_F(
  FutureSharedLockTest,
  asyncLockShared_should_return_resolved_future_when_predecessor_has_shared_lock_and_is_active_or_finished) {
  lock.asyncLockShared().DetachInline([&](auto) {
    // try to lock again while we hold the shared lock
    // since we use shared access, this must succeed and return a resolved
    // future
    {
      auto fut = lock.asyncLockShared();
      EXPECT_TRUE(fut.Ready());
      fut = lock.asyncLockShared();
      EXPECT_TRUE(fut.Ready());
    }
    // the previous two futures are already finished. This implies that they
    // have been active, so this must also succeed and return a resolved future
    auto fut = lock.asyncLockShared();
    EXPECT_TRUE(fut.Ready());
  });
}

TEST_F(
  FutureSharedLockTest,
  asyncLockShared_should_return_unresolved_future_when_predecessor_has_exclusive_lock) {
  lock.asyncLockExclusive().DetachInline([&](auto) {
    // try to acquire shared lock while we hold the exclusive lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncLockShared();
    EXPECT_FALSE(fut.Ready());
  });
  scheduler.ExecuteScheduled();  // cleanup
}

TEST_F(
  FutureSharedLockTest,
  asyncLockShared_should_return_unresolved_future_when_predecessor_is_blocked) {
  lock.asyncLockExclusive().DetachInline([&](auto) {
    // try to acquire shared lock while we hold the exclusive lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncLockShared();
    EXPECT_FALSE(fut.Ready());

    // try to acquire yet another shared lock
    // this will be queued after the previous one, and since that one is blocked
    // we must be blocked as well
    fut = lock.asyncLockShared();
    EXPECT_FALSE(fut.Ready());
  });
  scheduler.ExecuteScheduled();  // cleanup
}

TEST_F(FutureSharedLockTest,
       unlock_shared_should_post_the_next_exclusive_owner_on_the_scheduler) {
  int called = 0;
  lock.asyncLockShared().DetachInline([&](auto) {
    ++called;
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    // we still hold the lock, so nothing must be queued on the scheduler yet
    EXPECT_EQ(0, scheduler.funcs.size());
  });
  EXPECT_EQ(1, called);
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(2, called);
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(3, called);
}

TEST_F(FutureSharedLockTest,
       unlock_exclusive_should_post_all_next_shared_requests_on_the_scheduler) {
  int called = 0;
  lock.asyncLockExclusive().DetachInline([&](auto) {
    ++called;
    lock.asyncLockShared().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockShared().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    // we still hold the lock, so nothing must be queued on the scheduler yet
    EXPECT_EQ(0, scheduler.funcs.size());
  });
  EXPECT_EQ(1, called);
  EXPECT_EQ(2, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(3, called);
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(4, called);
}

TEST_F(FutureSharedLockTest,
       unlock_shared_should_post_next_exclusive_on_the_scheduler) {
  int called = 0;

  lock.asyncLockShared().DetachInline([&](auto) {
    ++called;
    lock.asyncLockShared().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockShared().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    EXPECT_EQ(3, called);
    // we still hold the lock, so nothing must be queued on the scheduler yet
    EXPECT_EQ(0, scheduler.funcs.size());
  });
  EXPECT_EQ(3, called);
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(4, called);
}

TEST_F(FutureSharedLockTest,
       unlock_shared_should_hand_over_ownership_to_next_active_shared) {
  int called = 0;
  FutureSharedLock::LockGuard lock_guard;
  lock.asyncLockShared().DetachInline([&](auto) {
    ++called;
    lock.asyncLockShared().DetachInline(
      [&](FutureSharedLock::LockGuard guard) {  //
        ++called;
        lock_guard = std::move(guard);
      });
    lock.asyncLockShared().DetachInline([&](auto) {  //
      ++called;
    });
    lock.asyncLockExclusive().DetachInline([&](auto) {  //
      ++called;
    });
    EXPECT_EQ(3, called);
    // we still hold the lock, so nothing must be queued on the scheduler yet
    EXPECT_EQ(0, scheduler.funcs.size());
  });

  // the first shared lock has been released, but the second one is still active
  // -> we still only have 3 calls and nothing queued
  EXPECT_EQ(3, called);
  EXPECT_EQ(0, scheduler.funcs.size());

  lock_guard.unlock();
  EXPECT_EQ(1, scheduler.funcs.size());
  scheduler.ExecuteScheduled();
  EXPECT_EQ(4, called);
}

TEST_F(FutureSharedLockTest,
       asyncTryLockExclusiveFor_should_return_resolved_future_when_unlocked) {
  int called = 0;
  lock.asyncTryLockExclusiveFor(10ms).DetachInline([&](auto) { ++called; });
  EXPECT_EQ(1, called);

  lock.asyncTryLockExclusiveFor(10ms).DetachInline([&](auto) { ++called; });
  EXPECT_EQ(2, called);
}

TEST_F(FutureSharedLockTest,
       asyncTryLockExclusiveFor_should_return_unresolved_future_when_locked) {
  lock.asyncTryLockExclusiveFor(10ms).DetachInline([&](auto) {
    // try to lock again while we hold the exclusive lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncTryLockExclusiveFor(10ms);
    EXPECT_FALSE(fut.Ready());
    std::move(fut).DetachInline([](auto result) { EXPECT_TRUE(result); });
  });
  scheduler.ExecuteScheduled();  // cleanup

  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  scheduler.ExecuteNextDelayed();  // cleanup
}

TEST_F(
  FutureSharedLockTest,
  asyncTryLockExclusiveFor_should_return_unresolved_future_when_predecessor_has_shared_lock) {
  lock.asyncLockShared().DetachInline([&](auto) {
    // try to acquire exclusive lock while we hold the shared lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncTryLockExclusiveFor(10ms);
    EXPECT_FALSE(fut.Ready());
    std::move(fut).DetachInline([](auto result) { EXPECT_TRUE(result); });
  });
  scheduler.ExecuteScheduled();  // cleanup

  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  scheduler.ExecuteNextDelayed();  // cleanup
}

TEST_F(
  FutureSharedLockTest,
  asyncTryLockExclusiveFor_should_resolve_with_exception_when_timeout_is_reached) {
  FutureSharedLock::LockGuard lock_guard;
  bool resolved_with_timeout = false;
  lock.asyncLockExclusive().DetachInline(
    [&](FutureSharedLock::LockGuard&& guard) {
      lock_guard = std::move(guard);
      lock.asyncTryLockExclusiveFor(10ms).DetachInline([&](auto result) {
        EXPECT_TRUE(result.State() == yaclib::ResultState::Exception);
        try {
          std::ignore = std::move(result).Ok();
        } catch (const ::sdb::basics::Exception& e) {
          EXPECT_EQ(e.code(), sdb::ERROR_LOCK_TIMEOUT);
          resolved_with_timeout = true;
        } catch (...) {
        }
      });
    });
  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  scheduler.ExecuteNextDelayed();  // simulate timeout
  lock_guard.unlock();
  scheduler.ExecuteScheduled();
  EXPECT_TRUE(resolved_with_timeout);
}

TEST_F(FutureSharedLockTest,
       asyncTryLockSharedFor_should_return_resolved_future_when_unlocked) {
  int called = 0;
  lock.asyncTryLockSharedFor(10ms).DetachInline([&](auto) { ++called; });
  EXPECT_EQ(1, called);

  lock.asyncTryLockSharedFor(10ms).DetachInline([&](auto) { ++called; });
  EXPECT_EQ(2, called);
}

TEST_F(
  FutureSharedLockTest,
  asyncTryLockSharedFor_should_return_resolved_future_when_predecessor_has_shared_lock_and_is_active_or_finished) {
  lock.asyncTryLockSharedFor(10ms).DetachInline([&](auto) {
    // try to lock again while we hold the shared lock
    // since we use shared access, this must succeed and return a resolved
    // future
    {
      auto fut = lock.asyncTryLockSharedFor(10ms);
      EXPECT_TRUE(fut.Ready());
      fut = lock.asyncTryLockSharedFor(10ms);
      EXPECT_TRUE(fut.Ready());
    }
    // the previous two futures are already finished. This implies that they
    // have been active, so this must also succeed and return a resolved future
    auto fut = lock.asyncTryLockSharedFor(10ms);
    EXPECT_TRUE(fut.Ready());
  });
}

TEST_F(
  FutureSharedLockTest,
  asyncTryLockSharedFor_should_return_unresolved_future_when_predecessor_has_exclusive_lock) {
  lock.asyncLockExclusive().DetachInline([&](auto) {
    // try to acquire shared lock while we hold the exclusive lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncTryLockSharedFor(10ms);
    EXPECT_FALSE(fut.Ready());
  });
  scheduler.ExecuteScheduled();  // cleanup
  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  scheduler.ExecuteNextDelayed();  // cleanup
}

TEST_F(
  FutureSharedLockTest,
  asyncTryLockSharedFor_should_return_unresolved_future_when_predecessor_is_blocked) {
  lock.asyncLockExclusive().DetachInline([&](auto) {
    // try to acquire shared lock while we hold the exclusive lock
    // this must return a future that is not yet resolved
    auto fut = lock.asyncLockShared();
    EXPECT_FALSE(fut.Ready());

    fut = lock.asyncTryLockSharedFor(10ms);
    EXPECT_FALSE(fut.Ready());
  });
  scheduler.ExecuteScheduled();  // cleanup
  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  scheduler.ExecuteNextDelayed();  // cleanup
}

TEST_F(
  FutureSharedLockTest,
  asyncTryLockSharedFor_should_resolve_with_exception_when_timeout_is_reached) {
  FutureSharedLock::LockGuard lock_guard;
  bool resolved_with_timeout = false;
  lock.asyncLockExclusive().DetachInline(
    [&](FutureSharedLock::LockGuard&& guard) {
      lock_guard = std::move(guard);
      lock.asyncTryLockSharedFor(10ms).DetachInline([&](auto result) {
        EXPECT_TRUE(result.State() == yaclib::ResultState::Exception);
        try {
          std::ignore = std::move(result).Ok();
        } catch (const ::sdb::basics::Exception& e) {
          EXPECT_EQ(e.code(), sdb::ERROR_LOCK_TIMEOUT);
          resolved_with_timeout = true;
        } catch (...) {
        }
      });
    });
  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  scheduler.ExecuteNextDelayed();  // simulate timeout
  lock_guard.unlock();
  scheduler.ExecuteScheduled();
  EXPECT_TRUE(resolved_with_timeout);
}

TEST_F(
  FutureSharedLockTest,
  unlock_should_skip_over_abandoned_node_when_scheduling_shared_lock_owners) {
  FutureSharedLock::LockGuard lock_guard;
  int called = 0;

  lock.asyncLockExclusive().DetachInline(
    [&](FutureSharedLock::LockGuard&& guard) {
      lock_guard = std::move(guard);
      // first acquire shared lock without timeout
      // -> this will become the new leader
      lock.asyncLockShared().DetachInline([&](auto) { ++called; });

      lock.asyncLockShared().DetachInline([&](auto) { ++called; });
      lock.asyncTryLockSharedFor(10ms).DetachInline(
        [&](FutureSharedLock::LockGuard&&) {
          FAIL();
          ++called;
        });
      lock.asyncLockShared().DetachInline([&](auto) { ++called; });
      lock.asyncTryLockSharedFor(10ms).DetachInline(
        [&](FutureSharedLock::LockGuard&&) {
          FAIL();
          ++called;
        });
    });
  ASSERT_EQ(2, scheduler.delayed_funcs.size());
  // simulate timeout
  scheduler.ExecuteNextDelayed();
  scheduler.ExecuteNextDelayed();

  lock_guard.unlock();
  scheduler.ExecuteScheduled();
  EXPECT_EQ(3, called);
}

TEST_F(FutureSharedLockTest,
       lock_can_be_deleted_before_timeout_callback_is_executed) {
  {
    FutureSharedLock lock(scheduler);
    lock.asyncLockExclusive().DetachInline(
      [&](FutureSharedLock::LockGuard guard) {
        std::ignore = lock.asyncTryLockSharedFor(10ms);
      });
    scheduler.ExecuteScheduled();
  }
  ASSERT_EQ(1, scheduler.delayed_funcs.size());
  // simulate timeout - this should do nothing since the lock has been deleted
  scheduler.ExecuteNextDelayed();
}

struct StressScheduler {
  struct Func {
    absl::AnyInvocable<void()> func;
  };

  StressScheduler() : scheduled(64) {}

  template<typename Fn>
  void queue(Fn&& func) {
    scheduled.push(new Func{std::forward<Fn>(func)});
  }
  using WorkHandle = int;
  template<typename Fn>
  WorkHandle queueDelayed(Fn&& func, std::chrono::milliseconds delay) {
    auto when = std::chrono::steady_clock::now() + delay;
    std::lock_guard lock(mutex);
    delayed_funcs.emplace(when, std::forward<Fn>(func));
    return 0;
  }

  void ExecuteScheduled() {
    ExecuteDelayed();

    Func* fn = nullptr;
    while (scheduled.pop(fn)) {
      fn->func();
      delete fn;
    }
  }

  void ExecuteDelayed() {
    // it is enough for one thread to process the delayedFuncs
    std::unique_lock lock(mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
      return;
    }

    // we move the functions out so we can release the lock before executing
    std::vector<std::function<void()>> funcs;
    auto now = std::chrono::steady_clock::now();
    auto it = delayed_funcs.begin();
    while (it != delayed_funcs.end()) {
      if (it->first > now) {
        break;
      }
      funcs.emplace_back([func = std::move(it->second)]() { func(false); });
      it = delayed_funcs.erase(it);
    }
    lock.unlock();

    for (auto& f : funcs) {
      f();
    }
  }

  boost::lockfree::queue<Func*> scheduled;

  std::mutex mutex;
  std::multimap<std::chrono::steady_clock::time_point,
                std::function<void(bool)>>
    delayed_funcs;
};

TEST(FutureSharedLockStressTest, parallel) {
  using FutureSharedLock = sdb::FutureSharedLock<StressScheduler>;
  StressScheduler scheduler;
  FutureSharedLock lock(scheduler);

  std::unordered_map<int, int> shared_data;

  constexpr int kNumThreads = 16;
  constexpr int kNumOpsPerThread = 4000000;

  std::vector<std::thread> threads;
  std::atomic<size_t> total_found{0};
  std::atomic<size_t> lock_timeouts{0};
  std::atomic<unsigned> num_tasks{0};
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.push_back(
      std::thread([&scheduler, &lock, &shared_data, &total_found, &num_tasks,
                   &lock_timeouts, id = i]() {
        std::mt19937 rnd(id);
        for (int j = 0; j < kNumOpsPerThread; ++j) {
          auto val = rnd();

          if ((val & 3) > 0 || num_tasks > kNumThreads * 10) {
            scheduler.ExecuteScheduled();
            continue;
          }
          num_tasks++;
          val >>= 2;

          auto exclusive_func = [id, &shared_data, rnd,
                                 &num_tasks](auto val) mutable {
            --num_tasks;
            val = (val >> 1) & 63;
            for (decltype(val) k = 0; k < val; ++k) {
              shared_data[rnd() & 1023] = id;
            }
          };

          auto shared_func = [id, &shared_data, rnd, &total_found,
                              &num_tasks](auto val) mutable {
            --num_tasks;
            val = (val >> 1) & 63;
            for (decltype(val) k = 0; k < val; ++k) {
              auto it = shared_data.find(rnd() & 1023);
              if (it != shared_data.end() && it->second == id) {
                ++total_found;
              }
            }
          };

          // perform some random write/read operations
          if (val & 1) {
            val = val >> 1;
            if (val & 1) {
              lock.asyncLockExclusive().DetachInline(
                [exclusive_func, val](auto) mutable { exclusive_func(val); });
            } else {
              auto timeout = val & 15;
              lock.asyncTryLockExclusiveFor(std::chrono::milliseconds(timeout))
                .DetachInline(
                  [exclusive_func, &lock_timeouts, val](auto res) mutable {
                    if (res) {
                      exclusive_func(val);
                    } else {
                      ++lock_timeouts;
                    }
                  });
            }
          } else {
            val = val >> 1;
            if (val & 1) {
              lock.asyncLockShared().DetachInline(
                [shared_func, val](auto) mutable { shared_func(val); });
            } else {
              auto timeout = val & 15;
              lock.asyncTryLockSharedFor(std::chrono::milliseconds(timeout))
                .DetachInline(
                  [shared_func, &lock_timeouts, val](auto res) mutable {
                    if (res) {
                      shared_func(val);
                    } else {
                      ++lock_timeouts;
                    }
                  });
            }
          }
        }

        scheduler.ExecuteScheduled();
      }));
  }

  for (auto& t : threads) {
    t.join();
  }
  EXPECT_EQ(0, num_tasks.load() - lock_timeouts.load());
  std::cout << "Found total " << total_found.load() <<  //
    "\nLock timeouts " << lock_timeouts.load() << std::endl;
}

}  // namespace
