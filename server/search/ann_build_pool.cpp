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

#include "search/ann_build_pool.h"

#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <utility>
#include <vector>
#include <yaclib/exe/job.hpp>

namespace sdb::search {
namespace {

class AnnRun {
 public:
  AnnRun(irs::AnnBuildEnv::Step step, uint32_t helpers) noexcept
    : _step{step}, _pending{helpers} {}

  void Work(uint32_t worker) noexcept {
    try {
      while (!_failed.load(std::memory_order_relaxed) && _step(worker)) {
      }
    } catch (...) {
      Fail(std::current_exception());
    }
  }

  void Finish() noexcept {
    std::lock_guard lock{_mutex};
    --_pending;
    _done.notify_all();
  }

  void WaitForHelpers() noexcept {
    std::unique_lock lock{_mutex};
    _done.wait(lock, [this] { return _pending == 0; });
  }

  void RethrowError() const {
    if (_error) {
      std::rethrow_exception(_error);
    }
  }

 private:
  void Fail(std::exception_ptr error) noexcept {
    _failed.store(true, std::memory_order_relaxed);
    std::lock_guard lock{_mutex};
    if (!_error) {
      _error = std::move(error);
    }
  }

  irs::AnnBuildEnv::Step _step;
  std::atomic<bool> _failed{false};
  std::mutex _mutex;
  std::condition_variable _done;
  uint32_t _pending;
  std::exception_ptr _error;
};

class AnnHelperJob final : public yaclib::Job {
 public:
  AnnHelperJob(AnnRun& run, uint32_t worker) noexcept
    : _run{&run}, _worker{worker} {}

  void Call() noexcept final {
    _called = true;
    _run->Work(_worker);
    _run->Finish();
  }

  void Drop() noexcept final { _run->Finish(); }

  bool Called() const noexcept { return _called; }

  uint32_t Worker() const noexcept { return _worker; }

 private:
  AnnRun* _run;
  uint32_t _worker;
  bool _called = false;
};

}  // namespace

AnnBuildPool::AnnBuildPool(yaclib::IExecutor& pool, AcquireFn acquire,
                           ReleaseFn release) noexcept
  : _pool{&pool}, _acquire{acquire}, _release{release} {}

void AnnBuildPool::Run(uint32_t workers, Step step) const {
  AnnRun run{step, workers - 1};
  std::vector<AnnHelperJob> helpers;
  helpers.reserve(workers - 1);
  for (uint32_t worker = 1; worker < workers; ++worker) {
    helpers.emplace_back(run, worker);
  }
  for (auto& helper : helpers) {
    _pool->Submit(helper);
  }
  run.Work(0);
  run.WaitForHelpers();
  for (const auto& helper : helpers) {
    if (!helper.Called()) {
      run.Work(helper.Worker());
    }
  }
  run.RethrowError();
}

}  // namespace sdb::search
