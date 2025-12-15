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

#include <rocksdb/types.h>

#include "basics/error_code.h"
#include "basics/future_shared_lock.h"
#include "database/access_mode.h"
#include "general_server/scheduler.h"
#include "yaclib/async/future.hpp"

namespace sdb {

class ShardBase {
 public:
  static constexpr double kDefaultLockTimeout = 10.0 * 60.0;

  virtual ~ShardBase() = default;

  virtual uint64_t ApproxNumberDocuments() const { return 0; }

  yaclib::Future<ErrorCode> LockWrite(double timeout = 0.0);
  void UnlockWrite() noexcept;
  yaclib::Future<ErrorCode> LockRead(double timeout = 0.0);
  void UnlockRead();

 private:
  struct SchedulerWrapper {
    using WorkHandle = Scheduler::WorkHandle;
    template<typename F>
    void queue(F&&);
    template<typename F>
    WorkHandle queueDelayed(F&&, std::chrono::milliseconds);
  };

  using FutureLock = FutureSharedLock<SchedulerWrapper>;

  yaclib::Future<ErrorCode> LockImpl(double timeout, AccessMode::Type mode);

  SchedulerWrapper _scheduler_wrapper;
  mutable FutureLock _exclusive_lock{_scheduler_wrapper};
};

}  // namespace sdb
