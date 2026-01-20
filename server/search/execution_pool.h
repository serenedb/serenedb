////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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

#include "basics/async_utils.hpp"
#include "basics/shared.hpp"
#include "general_server/scheduler.h"
#include "metrics/gauge.h"

namespace sdb::search {

struct SearchExecutionPool final : public metrics::Gauge<uint64_t> {
 public:
  using Value = uint64_t;
  using metrics::Gauge<uint64_t>::Gauge;

  void setLimit(int new_limit) noexcept {
    // should not be called during execution of queries!
    SDB_ASSERT(load() == 0);
    if (auto* scheduler = GetScheduler()) {
      _cpu_executor = &scheduler->GetCPUExecutor();
    }
    _limit = new_limit;
  }

  void stop() { SDB_ASSERT(load() == 0); }

  uint64_t allocateThreads(uint64_t active, uint64_t demand);

  void releaseThreads(uint64_t active, uint64_t demand);

  template<typename Func>
  void run(Func&& func) {
    SDB_ASSERT(_cpu_executor);
    _cpu_executor->add(std::forward<Func>(func));
  }

 private:
  folly::CPUThreadPoolExecutor* _cpu_executor{nullptr};
  std::atomic_uint64_t _active{0};
  uint64_t _limit{0};
};
}  // namespace sdb::search
