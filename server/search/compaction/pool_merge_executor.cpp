////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "search/compaction/pool_merge_executor.h"

#include <algorithm>
#include <memory>
#include <mutex>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/submit.hpp>

#include "search/compaction/merge_unit_run.h"

namespace sdb::search {
namespace {

class Helper {
 public:
  Helper(std::shared_ptr<MergeUnitRun> run,
         PoolMergeExecutor::ReleaseFn release) noexcept
    : _run{std::move(run)}, _release{release} {}

  Helper(Helper&& other) noexcept
    : _run{std::move(other._run)}, _release{other._release} {}
  Helper& operator=(Helper&&) = delete;

  ~Helper() {
    if (!_run) {
      return;
    }
    {
      std::lock_guard lock{_run->mutex};
      --_run->active;
    }
    _release();
  }

  void operator()() { _run->Drain(); }

 private:
  std::shared_ptr<MergeUnitRun> _run;
  PoolMergeExecutor::ReleaseFn _release;
};

}  // namespace

bool PoolMergeExecutor::Run(irs::MergePlan& plan) {
  const auto run = std::make_shared<MergeUnitRun>(plan);
  const auto spawn = [&] {
    size_t count = 0;
    {
      std::lock_guard lock{run->mutex};
      count = run->active < _helpers
                ? std::min(_helpers - run->active, run->schedule.Ready())
                : 0;
    }
    for (; count != 0 && _acquire(); --count) {
      {
        std::lock_guard lock{run->mutex};
        ++run->active;
      }
      yaclib::Submit(_pool, Helper{run, _release});
    }
  };
  while (const auto unit = run->Next()) {
    spawn();
    run->Run(*unit);
  }
  return run->Finish();
}

}  // namespace sdb::search
