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

#include "read_write_spin_lock.h"

#include "basics/cpu-relax.h"
#include "basics/debugging.h"

namespace {
static constexpr uint32_t kWriteLock{1};

static constexpr uint32_t kReaderIncrement{static_cast<uint32_t>(1) << 16};
static constexpr uint32_t kReaderMask{~(::kReaderIncrement - 1)};

static constexpr uint32_t kQueuedWriterIncrement{static_cast<uint32_t>(1) << 1};
static constexpr uint32_t kQueuedWriterMask{(::kReaderIncrement - 1) &
                                            ~::kWriteLock};

static_assert((::kReaderMask & ::kWriteLock) == 0,
              "::ReaderMask and ::WriteLock conflict");
static_assert((::kReaderMask & ::kQueuedWriterMask) == 0,
              "::ReaderMask and ::QueuedWriterMask conflict");
static_assert((::kQueuedWriterMask & ::kWriteLock) == 0,
              "::QueuedWriterMask and ::WriteLock conflict");

static_assert((::kReaderMask & ::kReaderIncrement) != 0 &&
                (::kReaderMask & (::kReaderIncrement >> 1)) == 0,
              "::ReaderIncrement must be first bit in ::ReaderMask");
static_assert(
  (::kQueuedWriterMask & ::kQueuedWriterIncrement) != 0 &&
    (::kQueuedWriterMask & (::kQueuedWriterIncrement >> 1)) == 0,
  "::QueuedWriterIncrement must be first bit in ::QueuedWriterMask");
}  // namespace

namespace sdb::basics {

ReadWriteSpinLock::ReadWriteSpinLock(ReadWriteSpinLock&& other) noexcept {
  auto val = other._state.load(std::memory_order_relaxed);
  SDB_ASSERT(val == 0);
  _state.store(val, std::memory_order_relaxed);
}

ReadWriteSpinLock& ReadWriteSpinLock::operator=(
  ReadWriteSpinLock&& other) noexcept {
  auto val = other._state.load(std::memory_order_relaxed);
  SDB_ASSERT(val == 0);
  val = _state.exchange(val, std::memory_order_relaxed);
  SDB_ASSERT(val == 0);
  return *this;
}

bool ReadWriteSpinLock::tryLockWrite() noexcept {
  // order_relaxed is an optimization, cmpxchg will synchronize side-effects
  auto state = _state.load(std::memory_order_relaxed);
  // try to acquire write lock as long as no readers or writers are active,
  // we might "overtake" other queued writers though.
  while ((state & ~::kQueuedWriterMask) == 0) {
    if (_state.compare_exchange_weak(state, state | ::kWriteLock,
                                     std::memory_order_acquire)) {
      return true;  // we successfully acquired the write lock!
    }
  }
  return false;
}

void ReadWriteSpinLock::lockWrite() noexcept {
  if (tryLockWrite()) {
    return;
  }

  // the lock is either hold by another writer or we have active readers
  // -> announce that we want to write
  auto state =
    _state.fetch_add(::kQueuedWriterIncrement, std::memory_order_relaxed);
  for (;;) {
    while ((state & ~::kQueuedWriterMask) == 0) {
      // try to acquire lock and perform queued writer decrement in one step
      if (_state.compare_exchange_weak(
            state, (state - ::kQueuedWriterIncrement) | ::kWriteLock,
            std::memory_order_acquire)) {
        return;
      }
    }
    CpuRelax();
    state = _state.load(std::memory_order_relaxed);
  }
}

bool ReadWriteSpinLock::lockWrite(size_t max_attempts) noexcept {
  if (tryLockWrite()) {
    return true;
  }

  uint64_t attempts = 0;

  // the lock is either hold by another writer or we have active readers
  // -> announce that we want to write
  auto state =
    _state.fetch_add(::kQueuedWriterIncrement, std::memory_order_relaxed);
  while (++attempts <= max_attempts) {
    while ((state & ~::kQueuedWriterMask) == 0) {
      // try to acquire lock and perform queued writer decrement in one step
      if (_state.compare_exchange_weak(
            state, (state - ::kQueuedWriterIncrement) | ::kWriteLock,
            std::memory_order_acquire)) {
        return true;
      }
      if (++attempts > max_attempts) {
        // Undo the counting of us as queued writer:
        _state.fetch_sub(::kQueuedWriterIncrement, std::memory_order_release);
        return false;
      }
    }
    CpuRelax();
    state = _state.load(std::memory_order_relaxed);
  }

  // Undo the counting of us as queued writer:
  _state.fetch_sub(::kQueuedWriterIncrement, std::memory_order_release);

  return false;
}

bool ReadWriteSpinLock::tryLockRead() noexcept {
  // order_relaxed is an optimization, cmpxchg will synchronize side-effects
  auto state = _state.load(std::memory_order_relaxed);
  // try to acquire read lock as long as no writers are active or queued
  while ((state & ~::kReaderMask) == 0) {
    if (_state.compare_exchange_weak(state, state + ::kReaderIncrement,
                                     std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void ReadWriteSpinLock::lockRead() noexcept {
  for (;;) {
    if (tryLockRead()) {
      return;
    }
    CpuRelax();
  }
}

bool ReadWriteSpinLock::lockRead(size_t max_attempts) noexcept {
  uint64_t attempts = 0;
  while (attempts++ <= max_attempts) {
    if (tryLockRead()) {
      return true;
    }
    CpuRelax();
  }
  return false;
}

void ReadWriteSpinLock::unlock() noexcept {
  if (isLockedWrite()) {
    unlockWrite();
  } else {
    SDB_ASSERT(isLockedRead());
    unlockRead();
  }
}

void ReadWriteSpinLock::unlockRead() noexcept {
  SDB_ASSERT(isLockedRead());
  _state.fetch_sub(::kReaderIncrement, std::memory_order_release);
}

void ReadWriteSpinLock::unlockWrite() noexcept {
  SDB_ASSERT(isLockedWrite());
  _state.fetch_sub(::kWriteLock, std::memory_order_release);
}

bool ReadWriteSpinLock::isLocked() const noexcept {
  return (_state.load(std::memory_order_relaxed) & ~::kQueuedWriterMask) != 0;
}

bool ReadWriteSpinLock::isLockedRead() const noexcept {
  return (_state.load(std::memory_order_relaxed) & ::kReaderMask) > 0;
}

bool ReadWriteSpinLock::isLockedWrite() const noexcept {
  return _state.load(std::memory_order_relaxed) & ::kWriteLock;
}

}  // namespace sdb::basics
