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

#include <absl/cleanup/cleanup.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "basics/assert.h"
#include "basics/common.h"
#include "basics/random/random_generator.h"
#include "basics/read_write_lock.h"
#include "basics/system-compiler.h"
#include "gtest/gtest.h"

namespace {

class TestReadWriteLock : public sdb::basics::ReadWriteLock {};

struct Synchronizer {
  std::mutex mutex;
  std::condition_variable cv;
  bool ready = false;
  int waiting{0};

  void WaitForStart() {
    std::unique_lock lock(mutex);
    ++waiting;
    cv.wait(lock, [&] { return ready; });
  }
  void Start(int nr) {
    while (true) {
      {
        std::unique_lock lock(mutex);
        if (waiting >= nr) {
          ready = true;
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    cv.notify_all();
  }
};

}  // namespace

TEST(ReadWriteLockTest, testLockWriteParallel) {
  TestReadWriteLock lock;

  constexpr size_t kN = 4;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  for (size_t i = 0; i < kN; ++i) {
    threads.emplace_back([&]() {
      s.WaitForStart();

      for (size_t i = 0; i < kIterations; ++i) {
        lock.lockWrite();
        ++counter;
        lock.unlockWrite();
      }
    });
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(kIterations * kN, counter);
}

TEST(ReadWriteLockTest, testTryLockWriteParallel) {
  TestReadWriteLock lock;

  constexpr size_t kN = 4;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  for (size_t i = 0; i < kN; ++i) {
    threads.emplace_back([&]() {
      s.WaitForStart();
      for (size_t i = 0; i < kIterations; ++i) {
        while (!lock.tryLockWrite()) {
        }
        ++counter;
        lock.unlockWrite();
      }
    });
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(kIterations * kN, counter);
}

TEST(ReadWriteLockTest, testTryLockWriteForParallel) {
  TestReadWriteLock lock;

  constexpr size_t kN = 4;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  auto timeout = std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::duration<double>(60.0 /*seconds*/));
  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  for (size_t i = 0; i < kN; ++i) {
    threads.emplace_back([&]() {
      s.WaitForStart();
      for (size_t i = 0; i < kIterations; ++i) {
        while (!lock.tryLockWriteFor(timeout)) {
        }
        ++counter;
        lock.unlockWrite();
      }
    });
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(kIterations * kN, counter);
}

TEST(ReadWriteLockTest, testTryLockWriteForParallelLowTimeout) {
  TestReadWriteLock lock;

  constexpr size_t kN = 4;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  auto timeout = std::chrono::microseconds(1);
  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  for (size_t i = 0; i < kN; ++i) {
    threads.emplace_back([&]() {
      s.WaitForStart();
      for (size_t i = 0; i < kIterations; ++i) {
        while (!lock.tryLockWriteFor(timeout)) {
        }
        ++counter;
        lock.unlockWrite();
      }
    });
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(kIterations * kN, counter);
}

TEST(ReadWriteLockTest, testTryLockWriteForWakeUpReaders) {
  TestReadWriteLock lock;

  std::vector<std::thread> threads;
  threads.reserve(3);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  // The main thread will hold the read lock for the duration of the test
  auto got_lock = lock.tryLockRead();
  ASSERT_TRUE(got_lock) << "Failed to get the read lock without concurrency";
  bool write_lock_thread_completed = false;
  bool read_lock_thread_completed = false;

  // First thread tries to get the writeLock with a timeout
  threads.emplace_back(
    [&]() {
      s.WaitForStart();
      auto timeout = std::chrono::milliseconds(100);
      auto got_lock = lock.tryLockWriteFor(timeout);
      EXPECT_FALSE(got_lock)
        << "We got a write lock although the read lock was held";
      write_lock_thread_completed = true;
    });

  // Second thread tries to get the readLock, while the first thread is waiting
  // for the writeLock
  threads.emplace_back(
    [&]() {
      s.WaitForStart();
      // This is still a race with the write locker
      // It may happen that we try to lock read before write => we pass here.
      // If we cannot get the readlock in an instant, we know the write locker
      // is in queue.
      size_t retry_counter = 100;
      while (lock.tryLockRead()) {
        lock.unlockRead();
        std::this_thread::sleep_for(std::chrono::microseconds(1));
        retry_counter--;
        if (retry_counter == 0) {
          ASSERT_FALSE(true) << "A queued write lock did not block the reader "
                                "from getting the lock";
          return;
        }
      }
      // NOTE: This time out is **much larger** than the write timeout.
      // So we need to be woken up if the Writer is released. If not
      // (old buggy behaviour), we will still wait for 30 seconds:
      auto timeout = std::chrono::seconds(30);
      auto got_lock = lock.tryLockReadFor(timeout);
      EXPECT_TRUE(got_lock)
        << "We did not get the read lock after the write lock got into timeout";
      lock.unlockRead();
      read_lock_thread_completed = true;
    });
  s.Start(2);

  std::move(guard).Invoke();
  EXPECT_TRUE(read_lock_thread_completed)
    << "Did not complete the read lock thread";
  EXPECT_TRUE(write_lock_thread_completed)
    << "Did not complete the write lock thread";
}

TEST(ReadWriteLockTest, testLockWriteLockReadParallel) {
  TestReadWriteLock lock;

  constexpr size_t kN = 4;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  for (size_t i = 0; i < kN; ++i) {
    if (i >= kN / 2) {
      threads.emplace_back([&]() {
        s.WaitForStart();
        for (size_t i = 0; i < kIterations; ++i) {
          while (!lock.tryLockWrite()) {
          }
          ++counter;
          lock.unlockWrite();
        }
      });
    } else {
      threads.emplace_back([&]() {
        s.WaitForStart();
        [[maybe_unused]] uint64_t total = 0;
        for (size_t i = 0; i < kIterations; ++i) {
          lock.lockRead();
          total += counter;
          lock.unlockRead();
        }
      });
    }
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(kIterations * (kN / 2), counter);
}

TEST(ReadWriteLockTest, testMixedParallel) {
  TestReadWriteLock lock;

  constexpr size_t kN = 8;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  auto timeout = std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::duration<double>(60.0 /*seconds*/));
  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  for (size_t i = 0; i < kN; ++i) {
    if (i == 0 || i == 1) {
      threads.emplace_back([&]() {
        s.WaitForStart();
        for (size_t i = 0; i < kIterations; ++i) {
          lock.lockWrite();
          ++counter;
          lock.unlockWrite();
        }
      });
    } else if (i == 2 || i == 3) {
      threads.emplace_back([&]() {
        s.WaitForStart();
        for (size_t i = 0; i < kIterations; ++i) {
          while (!lock.tryLockWrite()) {
          }
          ++counter;
          lock.unlockWrite();
        }
      });
    } else if (i == 4 || i == 5) {
      threads.emplace_back([&]() {
        s.WaitForStart();
        for (size_t i = 0; i < kIterations; ++i) {
          while (!lock.tryLockWriteFor(timeout)) {
          }
          ++counter;
          lock.unlockWrite();
        }
      });
    } else {
      threads.emplace_back([&]() {
        s.WaitForStart();
        [[maybe_unused]] uint64_t total = 0;
        for (size_t i = 0; i < kIterations; ++i) {
          lock.lockRead();
          total += counter;
          lock.unlockRead();
        }
      });
    }
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(kIterations * 6, counter);
}

TEST(ReadWriteLockTest, testRandomMixedParallel) {
  TestReadWriteLock lock;

  constexpr size_t kN = 6;
  std::vector<std::thread> threads;
  threads.reserve(kN);

  absl::Cleanup guard = [&]() noexcept {
    for (auto& t : threads) {
      t.join();
    }
  };

  Synchronizer s;

  constexpr size_t kIterations = 5000000;
  uint64_t counter = 0;

  std::atomic<size_t> total = 0;

  for (size_t i = 0; i < kN; ++i) {
    threads.emplace_back([&]() {
      s.WaitForStart();
      size_t expected = 0;
      for (size_t i = 0; i < kIterations; ++i) {
        auto r = sdb::random::Interval(static_cast<uint32_t>(4));
        switch (r) {
          case 0: {
            lock.lockWrite();
            ++counter;
            ++expected;
            lock.unlockWrite();
            break;
          }
          case 1: {
            if (lock.tryLockWrite()) {
              ++counter;
              ++expected;
              lock.unlockWrite();
            }
            break;
          }
          case 2: {
            if (lock.tryLockRead()) {
              [[maybe_unused]] uint64_t value = counter;
              lock.unlockRead();
            }
            break;
          }
          case 3: {
            lock.lockRead();
            [[maybe_unused]] uint64_t value = counter;
            lock.unlockRead();
            break;
          }
          case 4: {
            if (lock.tryLockWriteFor(std::chrono::microseconds(
                  sdb::random::Interval(static_cast<uint32_t>(1000))))) {
              ++counter;
              ++expected;
              lock.unlockWrite();
            }
            break;
          }
          default: {
            SDB_UNREACHABLE();
          }
        }
      }

      total.fetch_add(expected);
    });
  }

  s.Start(kN);

  std::move(guard).Invoke();
  ASSERT_EQ(total, counter);
}

TEST(ReadWriteLockTest, testTryLockWrite) {
  TestReadWriteLock lock;

  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try lock write
  ASSERT_TRUE(lock.tryLockWrite());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try write-locking again
  ASSERT_FALSE(lock.tryLockWrite());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try write-locking again, with timeout
  ASSERT_FALSE(lock.tryLockWriteFor(std::chrono::microseconds(1000)));
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try read-locking
  ASSERT_FALSE(lock.tryLockRead());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());
}

TEST(ReadWriteLockTest, testLockWrite) {
  TestReadWriteLock lock;

  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // lock write
  lock.lockWrite();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try write-locking again
  ASSERT_FALSE(lock.tryLockWrite());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try write-locking again, with timeout
  ASSERT_FALSE(lock.tryLockWriteFor(std::chrono::microseconds(1000)));
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try read-locking
  ASSERT_FALSE(lock.tryLockRead());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());
}

TEST(ReadWriteLockTest, testTryLockRead) {
  TestReadWriteLock lock;

  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try lock read
  ASSERT_TRUE(lock.tryLockRead());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try read-locking again
  ASSERT_TRUE(lock.tryLockRead());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // read-lock again
  lock.lockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try write-locking
  ASSERT_FALSE(lock.tryLockWrite());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try write-locking again, with timeout
  ASSERT_FALSE(lock.tryLockWriteFor(std::chrono::microseconds(1000)));
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // unlock one level
  lock.unlockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
  ASSERT_FALSE(lock.tryLockWrite());

  // unlock one another level
  lock.unlockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
  ASSERT_FALSE(lock.tryLockWrite());

  // unlock final level
  lock.unlockRead();
  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
  ASSERT_TRUE(lock.tryLockWrite());
}

TEST(ReadWriteLockTest, testLockRead) {
  TestReadWriteLock lock;

  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // lock read
  lock.lockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try read-locking again
  ASSERT_TRUE(lock.tryLockRead());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // read-lock again
  lock.lockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try write-locking
  ASSERT_FALSE(lock.tryLockWrite());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // try write-locking again, with timeout
  ASSERT_FALSE(lock.tryLockWriteFor(std::chrono::microseconds(1000)));
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // unlock one level
  lock.unlockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
  ASSERT_FALSE(lock.tryLockWrite());

  // unlock one another level
  lock.unlockRead();
  ASSERT_TRUE(lock.isLocked());
  ASSERT_TRUE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
  ASSERT_FALSE(lock.tryLockWrite());

  // unlock final level
  lock.unlockRead();
  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
  ASSERT_TRUE(lock.tryLockWrite());
}

TEST(ReadWriteLockTest, testLockWriteAttempted) {
  TestReadWriteLock lock;

  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());

  // lock write
  ASSERT_TRUE(lock.tryLockWriteFor(std::chrono::microseconds(1000000)));
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  // try locking again
  ASSERT_FALSE(lock.tryLockWriteFor(std::chrono::microseconds(1000000)));
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  ASSERT_FALSE(lock.tryLockRead());
  ASSERT_TRUE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_TRUE(lock.isLockedWrite());

  lock.unlockWrite();
  ASSERT_FALSE(lock.isLocked());
  ASSERT_FALSE(lock.isLockedRead());
  ASSERT_FALSE(lock.isLockedWrite());
}

TEST(ReadWriteLockTest, readerOverflow) {
  // this is a regression test for the old version where we only used 16 bits
  // for the reader counter. Since we can have many more readers than threads,
  // this limit could easily be reached. Note that we have no similar test for a
  // writer overflow since we would actually need 2^15 threads to reach that
  // limit.
  TestReadWriteLock lock;

  for (unsigned i = 0; i < (1 << 16); ++i) {
    ASSERT_TRUE(lock.tryLockRead()) << "tryLockRead failed at iteration " << i;
  }
  ASSERT_FALSE(lock.tryLockWrite())
    << "tryLockWrite succeeded even though we have active readers";
}
