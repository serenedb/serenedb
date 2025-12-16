////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include "storage_engine/shard_base.h"

#include "basics/error_code.h"
#include "general_server/scheduler_feature.h"
#include "yaclib/async/future.hpp"

namespace sdb {

template<typename F>
void ShardBase::SchedulerWrapper::queue(F&& fn) {
  SchedulerFeature::gScheduler->queue(RequestLane::ClusterInternal,
                                      std::forward<F>(fn));
}

template<typename F>
ShardBase::SchedulerWrapper::WorkHandle
ShardBase::SchedulerWrapper::queueDelayed(F&& fn,
                                          std::chrono::milliseconds timeout) {
  return SchedulerFeature::gScheduler->queueDelayed(
    "shard-base-lock-timeout", RequestLane::ClusterInternal, timeout,
    std::forward<F>(fn));
}

yaclib::Future<ErrorCode> ShardBase::LockImpl(double timeout,
                                              AccessMode::Type mode) {
  // user read operations don't require any lock in RocksDB, so we won't get
  // here. user write operations will acquire the R/W lock in read mode, and
  // user exclusive operations will acquire the R/W lock in write mode.
  SDB_ASSERT(mode == AccessMode::Type::Read || mode == AccessMode::Type::Write);

  if (timeout <= 0) {
    timeout = kDefaultLockTimeout;
  }
  auto timeout_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::duration<double>(timeout));

  bool got_lock = false;
  if (mode == AccessMode::Type::Write) {
    auto f = _exclusive_lock.asyncTryLockExclusiveFor(timeout_ms);
    co_await yaclib::Await(f);
    got_lock = !!std::as_const(f).Touch();
    if (got_lock) {
      std::move(f).Touch().Ok().release();
    }
  } else {
    auto f = _exclusive_lock.asyncTryLockSharedFor(timeout_ms);
    co_await yaclib::Await(f);
    got_lock = !!std::as_const(f).Touch();
    if (got_lock) {
      std::move(f).Touch().Ok().release();
    }
  }

  if (got_lock) {
    // keep the lock and exit
    co_return ERROR_OK;
  }

  co_return ERROR_LOCK_TIMEOUT;
}

yaclib::Future<ErrorCode> ShardBase::LockWrite(double timeout) {
  return LockImpl(timeout, AccessMode::Type::Write);
}

void ShardBase::UnlockWrite() noexcept { _exclusive_lock.unlock(); }

yaclib::Future<ErrorCode> ShardBase::LockRead(double timeout) {
  return LockImpl(timeout, AccessMode::Type::Read);
}

void ShardBase::UnlockRead() { _exclusive_lock.unlock(); }

}  // namespace sdb
