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

#include <atomic>
#include <thread>
#include <vector>

#include "basics/common.h"
#include "basics/read_write_lock.h"
#include "basics/recursive_locker.h"
#include "gtest/gtest.h"

using namespace sdb;
using namespace sdb::basics;

// RecursiveWriteLocker

TEST(RecursiveLockerTest, testRecursiveWriteLockNoAcquire) {
  sdb::basics::ReadWriteLock rwlock;
  std::atomic<std::thread::id> owner;

  RECURSIVE_WRITE_LOCKER_NAMED(locker, rwlock, owner, false);
  ASSERT_FALSE(locker.isLocked());

  locker.lock();
  ASSERT_TRUE(locker.isLocked());

  locker.unlock();
  ASSERT_FALSE(locker.isLocked());
}

TEST(RecursiveLockerTest, testRecursiveWriteLockAcquire) {
  sdb::basics::ReadWriteLock rwlock;
  std::atomic<std::thread::id> owner;

  RECURSIVE_WRITE_LOCKER_NAMED(locker, rwlock, owner, true);
  ASSERT_TRUE(locker.isLocked());

  locker.unlock();
  ASSERT_FALSE(locker.isLocked());
}

TEST(RecursiveLockerTest, testRecursiveWriteLockUnlock) {
  sdb::basics::ReadWriteLock rwlock;
  std::atomic<std::thread::id> owner;

  RECURSIVE_WRITE_LOCKER_NAMED(locker, rwlock, owner, true);
  ASSERT_TRUE(locker.isLocked());

  for (int i = 0; i < 100; ++i) {
    locker.unlock();
    ASSERT_FALSE(locker.isLocked());
    locker.lock();
    ASSERT_TRUE(locker.isLocked());
  }

  ASSERT_TRUE(locker.isLocked());
  locker.unlock();
  ASSERT_FALSE(locker.isLocked());
}

TEST(RecursiveLockerTest, testRecursiveWriteLockNested) {
  sdb::basics::ReadWriteLock rwlock;
  std::atomic<std::thread::id> owner;

  RECURSIVE_WRITE_LOCKER_NAMED(locker1, rwlock, owner, true);
  ASSERT_TRUE(locker1.isLocked());

  {
    RECURSIVE_WRITE_LOCKER_NAMED(locker2, rwlock, owner, true);
    ASSERT_TRUE(locker2.isLocked());

    {
      RECURSIVE_WRITE_LOCKER_NAMED(locker3, rwlock, owner, true);
      ASSERT_TRUE(locker3.isLocked());
    }

    ASSERT_TRUE(locker2.isLocked());
  }

  ASSERT_TRUE(locker1.isLocked());

  locker1.unlock();
  ASSERT_FALSE(locker1.isLocked());
}

TEST(RecursiveLockerTest, testRecursiveWriteLockMultiThreaded) {
  sdb::basics::ReadWriteLock rwlock;
  std::atomic<std::thread::id> owner;

  // number of threads started
  std::atomic<int> started{0};

  // shared variables, only protected by rw-locks
  uint64_t total = 0;
  uint64_t x = 0;

  constexpr int kN = 4;
  constexpr int kIterations = 100000;

  std::vector<std::thread> threads;
  threads.reserve(kN);

  for (int i = 0; i < kN; ++i) {
    threads.emplace_back([&]() {
      ++started;
      while (started < kN) { /*spin*/
      }

      for (int i = 0; i < kIterations; ++i) {
        RECURSIVE_WRITE_LOCKER_NAMED(locker1, rwlock, owner, true);
        ASSERT_TRUE(locker1.isLocked());

        total++;
        x++;

        {
          RECURSIVE_WRITE_LOCKER_NAMED(locker2, rwlock, owner, true);
          ASSERT_TRUE(locker2.isLocked());

          x++;
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(kN * kIterations, total);
  ASSERT_EQ(kN * kIterations * 2, x);
}
