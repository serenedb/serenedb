////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <thread>

#include "basics/common.h"
#include "basics/debugging.h"
#include "basics/locking.h"

/// construct locker with file and line information
#define WRITE_LOCKER(obj, lock)                                            \
  sdb::basics::WriteLocker<typename std::decay<decltype(lock)>::type> obj( \
    &lock, sdb::basics::LockerType::BLOCKING, true, __FILE__, __LINE__)

#define WRITE_LOCKER_EVENTUAL(obj, lock)                                   \
  sdb::basics::WriteLocker<typename std::decay<decltype(lock)>::type> obj( \
    &lock, sdb::basics::LockerType::EVENTUAL, true, __FILE__, __LINE__)

#define TRY_WRITE_LOCKER(obj, lock)                                        \
  sdb::basics::WriteLocker<typename std::decay<decltype(lock)>::type> obj( \
    &lock, sdb::basics::LockerType::TRY, true, __FILE__, __LINE__)

#define CONDITIONAL_WRITE_LOCKER(obj, lock, condition)                     \
  sdb::basics::WriteLocker<typename std::decay<decltype(lock)>::type> obj( \
    &lock, sdb::basics::LockerType::BLOCKING, (condition), __FILE__, __LINE__)

namespace sdb::basics {

/// write locker
/// A WriteLocker write-locks a read-write lock during its lifetime and unlocks
/// the lock when it is destroyed.
template<typename LockType>
class WriteLocker {
 public:
  WriteLocker(const WriteLocker&) = delete;
  WriteLocker& operator=(const WriteLocker&) = delete;

  /// acquires a write-lock
  /// The constructors acquire a write lock, the destructor unlocks the lock.
  WriteLocker(LockType* read_write_lock, LockerType type, bool condition,
              const char* file, int line)
    : _read_write_lock(read_write_lock),
      _file(file),
      _line(line),
      _is_locked(false) {
    if (condition) {
      if (type == LockerType::BLOCKING) {
        lock();
        SDB_ASSERT(_is_locked);
      } else if (type == LockerType::EVENTUAL) {
        lockEventual();
        SDB_ASSERT(_is_locked);
      } else if (type == LockerType::TRY) {
        _is_locked = tryLock();
      }
    }
  }

  /// releases the write-lock
  ~WriteLocker() noexcept {
    if (_is_locked) {
      static_assert(noexcept(_read_write_lock->unlockWrite()));
      _read_write_lock->unlockWrite();
    }
  }

  /// whether or not we acquired the lock
  [[nodiscard]] bool isLocked() const noexcept { return _is_locked; }

  /// eventually acquire the write lock
  void lockEventual() {
    while (!tryLock()) {
      std::this_thread::yield();
    }
    SDB_ASSERT(_is_locked);
  }

  [[nodiscard]] bool tryLock() {
    SDB_ASSERT(!_is_locked);
    if (_read_write_lock->tryLockWrite()) {
      _is_locked = true;
    }
    return _is_locked;
  }

  /// acquire the write lock, blocking
  void lock() {
    SDB_ASSERT(!_is_locked);
    _read_write_lock->lockWrite();
    _is_locked = true;
  }

  /// unlocks the lock if we own it
  bool unlock() {
    if (_is_locked) {
      _read_write_lock->unlockWrite();
      _is_locked = false;
      return true;
    }
    return false;
  }

  /// steals the lock, but does not unlock it
  bool steal() {
    if (_is_locked) {
      _is_locked = false;
      return true;
    }
    return false;
  }

 private:
  /// the read-write lock
  LockType* _read_write_lock;

  /// file
  const char* _file;

  /// line number
  int _line;

  /// whether or not the lock was acquired
  bool _is_locked;
};

}  // namespace sdb::basics
