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

namespace {

constexpr uint64_t kWindowFactor = 16;

void Wake(const std::vector<duckdb::InterruptState>& waiters) {
  for (const auto& waiter : waiters) {
    waiter.Callback();
  }
}

}  // namespace

CopyInBatches::CopyInBatches(size_t capacity)
  : _capacity{std::max<size_t>(capacity, 2)} {}

bool CopyInBatches::Full() const {
  return _outstanding >= _capacity ||
         _pushed - _watermark >= _capacity * kWindowFactor;
}

bool CopyInBatches::NeedSpace() {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    if (_aborted || !Full()) {
      return false;
    }
    _space_waiter = true;
    waiters = TakeWaitersIfQueued();
  }
  Wake(waiters);
  return true;
}

size_t CopyInBatches::RetainedBytes() const {
  Guard guard{_waiters.lock};
  return _retained_bytes;
}

void CopyInBatches::Push(std::unique_ptr<CopyBatch> batch,
                         message::Chain released) {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    if (_aborted) {
      return;
    }
    batch->seq = _pushed;
    _retained_bytes += batch->bytes;
    _window.emplace_back(std::move(batch));
    _done.emplace_back(0);
    if (released.head) {
      _released.emplace_back(_pushed, std::move(released));
    }
    ++_pushed;
    ++_outstanding;
    if (_pushed - _taken >= kCopyWakeBacklog) {
      waiters = TakeOneWaiter();
    }
  }
  Wake(waiters);
}

void CopyInBatches::Release(message::Chain released) {
  if (!released.head) {
    return;
  }
  Guard guard{_waiters.lock};
  if (_pushed != _watermark) {
    _released.emplace_back(_pushed - 1, std::move(released));
  }
}

void CopyInBatches::Flush() {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    waiters = TakeWaitersIfQueued();
  }
  Wake(waiters);
}

void CopyInBatches::Finish() {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    _finished = true;
    waiters = _waiters.TakeBlockedTasks();
  }
  Wake(waiters);
}

void CopyInBatches::Fail(std::exception_ptr error) {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    if (!_error) {
      _error = std::move(error);
    }
    waiters = _waiters.TakeBlockedTasks();
  }
  Wake(waiters);
}

void CopyInBatches::Abort() {
  std::vector<duckdb::InterruptState> waiters;
  bool space = false;
  {
    Guard guard{_waiters.lock};
    _aborted = true;
    space = std::exchange(_space_waiter, false);
    waiters = _waiters.TakeBlockedTasks();
  }
  Wake(waiters);
  if (space) {
    _space.Set();
  }
}

bool CopyInBatches::Aborted() const {
  Guard guard{_waiters.lock};
  return _aborted;
}

CopyTake CopyInBatches::Take(duckdb::TableFunctionInput& input, size_t max,
                             std::vector<CopyBatch*>& out) {
  Lock lock{_waiters.lock};
  for (;;) {
    ThrowIfFailed(lock);
    if (TakeReady(max, out)) {
      return CopyTake::Batches;
    }
    if (_finished || _aborted) {
      return CopyTake::End;
    }
    if (CanPark(input) && _waiters.BlockTask(*input.interrupt_state)) {
      input.async_result = duckdb::AsyncResultType::BLOCKED;
      return CopyTake::Blocked;
    }
    WaitForPush(lock);
  }
}

bool CopyInBatches::TryTake(size_t max, std::vector<CopyBatch*>& out) {
  Lock lock{_waiters.lock};
  ThrowIfFailed(lock);
  return TakeReady(max, out);
}

CopyBatch* CopyInBatches::Next() {
  Lock lock{_waiters.lock};
  for (;;) {
    ThrowIfFailed(lock);
    if (_taken != _pushed) {
      return _window[_taken++ - _watermark].get();
    }
    if (_finished || _aborted) {
      return nullptr;
    }
    WaitForPush(lock);
  }
}

void CopyInBatches::Done(uint64_t seq) {
  std::vector<std::unique_ptr<CopyBatch>> finished;
  std::vector<message::Chain> chunks;
  bool space = false;
  {
    Guard guard{_waiters.lock};
    _done[seq - _watermark] = 1;
    --_outstanding;
    while (!_done.empty() && _done.front() != 0) {
      _done.pop_front();
      _retained_bytes -= _window.front()->bytes;
      finished.emplace_back(std::move(_window.front()));
      _window.pop_front();
      ++_watermark;
    }
    while (!_released.empty() && _released.front().seq < _watermark) {
      chunks.emplace_back(std::move(_released.front().chunks));
      _released.pop_front();
    }
    if (!Full()) {
      space = std::exchange(_space_waiter, false);
    }
  }
  if (space) {
    _space.Set();
  }
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

std::vector<duckdb::InterruptState> CopyInBatches::TakeOneWaiter() {
  auto waiters = _waiters.TakeBlockedTasks();
  for (size_t i = 1; i < waiters.size(); ++i) {
    _waiters.BlockTask(waiters[i]);
  }
  waiters.resize(std::min<size_t>(waiters.size(), 1));
  return waiters;
}

std::vector<duckdb::InterruptState> CopyInBatches::TakeWaitersIfQueued() {
  if (_taken == _pushed) {
    return {};
  }
  return _waiters.TakeBlockedTasks();
}

bool CopyInBatches::CanPark(const duckdb::TableFunctionInput& input) {
  return input.interrupt_state &&
         input.results_execution_mode ==
           duckdb::AsyncResultsExecutionMode::TASK_EXECUTOR;
}

void CopyInBatches::ThrowIfFailed(Lock& lock) {
  if (_error) {
    auto error = _error;
    lock.unlock();
    std::rethrow_exception(error);
  }
}

void CopyInBatches::WaitForPush(Lock& lock) {
  auto signal = duckdb::make_shared_ptr<duckdb::InterruptDoneSignalState>();
  _waiters.BlockTask(duckdb::InterruptState{
    duckdb::weak_ptr<duckdb::InterruptDoneSignalState>{signal}});
  lock.unlock();
  signal->Await();
  lock.lock();
}

}  // namespace sdb::pg
