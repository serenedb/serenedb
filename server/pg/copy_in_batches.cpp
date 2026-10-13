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

#include "pg/copy_in_batches.h"

#include <algorithm>
#include <iresearch/utils/assert.hpp>

namespace sdb::pg {

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;
using UniqueLock = duckdb::annotated_unique_lock<duckdb::annotated_mutex>;

namespace {

constexpr uint64_t kWindowFactor = 16;

}  // namespace

CopyInBatches::CopyInBatches(size_t capacity)
  : _capacity{std::max<size_t>(capacity, 2)} {}

bool CopyInBatches::Full() const {
  return _outstanding >= _capacity ||
         _pushed - _watermark >= _capacity * kWindowFactor;
}

bool CopyInBatches::NeedSpace() {
  Guard guard{_blocked.lock};
  if (_aborted || !Full()) {
    return false;
  }
  _space_waiter = true;
  return true;
}

void CopyInBatches::Push(std::unique_ptr<CopyBatch> batch,
                         message::Chain released) {
  std::vector<duckdb::InterruptState> wake;
  {
    Guard guard{_blocked.lock};
    if (_aborted) {
      return;
    }
    batch->seq = _pushed;
    _window.emplace_back(std::move(batch));
    _done.emplace_back(0);
    if (released.head != nullptr) {
      _released.emplace_back(_pushed, std::move(released));
    }
    ++_pushed;
    ++_outstanding;
    wake = _blocked.TakeBlockedTasks();
    for (size_t i = 1; i < wake.size(); ++i) {
      _blocked.BlockTask(wake[i]);
    }
    wake.resize(std::min<size_t>(wake.size(), 1));
  }
  _changed.notify_one();
  for (const auto& task : wake) {
    task.Callback();
  }
}

void CopyInBatches::Release(message::Chain released) {
  if (released.head == nullptr) {
    return;
  }
  Guard guard{_blocked.lock};
  if (_pushed != _watermark) {
    _released.emplace_back(_pushed - 1, std::move(released));
  }
}

void CopyInBatches::Finish() {
  std::vector<duckdb::InterruptState> wake;
  {
    Guard guard{_blocked.lock};
    _finished = true;
    wake = _blocked.TakeBlockedTasks();
  }
  _changed.notify_all();
  for (const auto& task : wake) {
    task.Callback();
  }
}

void CopyInBatches::Fail(std::exception_ptr error) {
  std::vector<duckdb::InterruptState> wake;
  {
    Guard guard{_blocked.lock};
    if (!_error) {
      _error = std::move(error);
    }
    wake = _blocked.TakeBlockedTasks();
  }
  _changed.notify_all();
  for (const auto& task : wake) {
    task.Callback();
  }
}

void CopyInBatches::Abort() {
  std::vector<duckdb::InterruptState> wake;
  bool space = false;
  {
    Guard guard{_blocked.lock};
    _aborted = true;
    space = std::exchange(_space_waiter, false);
    wake = _blocked.TakeBlockedTasks();
  }
  _changed.notify_all();
  for (const auto& task : wake) {
    task.Callback();
  }
  if (space) {
    _space.Set();
  }
}

bool CopyInBatches::Aborted() const {
  Guard guard{_blocked.lock};
  return _aborted;
}

bool CopyInBatches::TakeReady(size_t max, std::vector<CopyBatch*>& out) {
  if (_taken == _pushed) {
    return false;
  }
  const auto take = std::min<uint64_t>(max, _pushed - _taken);
  for (uint64_t i = 0; i < take; ++i) {
    out.emplace_back(_window[_taken + i - _watermark].get());
  }
  _taken += take;
  return true;
}

CopyTake CopyInBatches::Take(duckdb::TableFunctionInput& input, size_t max,
                             std::vector<CopyBatch*>& out,
                             size_t produce_below) {
  UniqueLock lock{_blocked.lock};
  for (;;) {
    if (_error) {
      auto error = _error;
      lock.unlock();
      std::rethrow_exception(error);
    }
    if (!_producing && !_finished && !_aborted &&
        _pushed - _taken < produce_below) {
      _producing = true;
      return CopyTake::Produce;
    }
    if (TakeReady(max, out)) {
      return CopyTake::Batches;
    }
    if (_finished || _aborted) {
      return CopyTake::End;
    }
    if (input.interrupt_state &&
        input.results_execution_mode ==
          duckdb::AsyncResultsExecutionMode::TASK_EXECUTOR &&
        _blocked.BlockTask(*input.interrupt_state)) {
      input.async_result = duckdb::AsyncResultType::BLOCKED;
      return CopyTake::Blocked;
    }
    _changed.wait(lock);
  }
}

void CopyInBatches::Produced() {
  std::vector<duckdb::InterruptState> wake;
  {
    Guard guard{_blocked.lock};
    _producing = false;
    wake = _blocked.TakeBlockedTasks();
  }
  _changed.notify_all();
  for (const auto& task : wake) {
    task.Callback();
  }
}

bool CopyInBatches::TryTake(size_t max, std::vector<CopyBatch*>& out) {
  UniqueLock lock{_blocked.lock};
  if (_error) {
    auto error = _error;
    lock.unlock();
    std::rethrow_exception(error);
  }
  return TakeReady(max, out);
}

CopyBatch* CopyInBatches::Next() {
  UniqueLock lock{_blocked.lock};
  for (;;) {
    if (_error) {
      auto error = _error;
      lock.unlock();
      std::rethrow_exception(error);
    }
    if (_taken != _pushed) {
      return _window[_taken++ - _watermark].get();
    }
    if (_finished || _aborted) {
      return nullptr;
    }
    _changed.wait(lock);
  }
}

void CopyInBatches::Done(uint64_t seq) {
  std::vector<std::unique_ptr<CopyBatch>> finished;
  std::vector<message::Chain> chunks;
  bool space = false;
  {
    Guard guard{_blocked.lock};
    _done[seq - _watermark] = 1;
    --_outstanding;
    while (!_done.empty() && _done.front() != 0) {
      _done.pop_front();
      finished.emplace_back(std::move(_window.front()));
      _window.pop_front();
      ++_watermark;
    }
    while (!_released.empty() && _released.front().seq < _watermark) {
      chunks.emplace_back(std::move(_released.front().chunks));
      _released.pop_front();
    }
    if (_space_waiter && !Full()) {
      _space_waiter = false;
      space = true;
    }
  }
  if (space) {
    _space.Set();
  }
}

}  // namespace sdb::pg
