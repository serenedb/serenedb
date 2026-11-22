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
#include <duckdb/common/fault_points.hpp>
#include <iresearch/utils/assert.hpp>

namespace sdb::pg {

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;

namespace {

constexpr uint64_t kWindowFactor = 4;

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

bool CopyInBatches::Starving() const {
  Guard guard{_waiters.lock};
  return _parked != 0 && _taken == _pushed && !_finished && !_aborted;
}

bool CopyInBatches::Pending() const {
  Guard guard{_waiters.lock};
  return _taken != _pushed || _finished || _aborted || _error;
}

bool CopyInBatches::NeedSpace() {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    if (_aborted || !Full()) {
      return false;
    }
    _space_waiter = true;
    waiters = TakeWaiters(_pushed - _taken);
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
    waiters = TakeWaiters(1);
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
    waiters = TakeWaiters(_pushed - _taken);
  }
  Wake(waiters);
}

void CopyInBatches::Finish() {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    _finished = true;
    waiters = TakeAllWaiters();
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
    waiters = TakeAllWaiters();
  }
  Wake(waiters);
}

void CopyInBatches::Abort() {
  std::vector<duckdb::InterruptState> waiters;
  bool space = false;
  {
    Guard guard{_waiters.lock};
    _aborted = true;
    while (_pushed != _taken) {
      _retained_bytes -= _window.back()->bytes;
      _window.pop_back();
      _done.pop_back();
      --_pushed;
      --_outstanding;
    }
    if (_pushed == _watermark) {
      _released.clear();
    } else {
      for (auto& released : _released) {
        released.seq = std::min(released.seq, _pushed - 1);
      }
    }
    space = std::exchange(_space_waiter, false);
    waiters = TakeAllWaiters();
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

void CopyInBatches::Nudge() {
  std::vector<duckdb::InterruptState> waiters;
  {
    Guard guard{_waiters.lock};
    _nudged = true;
    waiters = TakeAllWaiters();
  }
  Wake(waiters);
}

bool CopyInBatches::Ended() const {
  Guard guard{_waiters.lock};
  return _finished || _aborted;
}

CopyTake CopyInBatches::Take(duckdb::TableFunctionInput& input, size_t max,
                             std::vector<CopyBatchLease>& out) {
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
      ++_parked;
      input.async_result = duckdb::AsyncResultType::BLOCKED;
      lock.unlock();
      Starve();
      SDB_PARK_ONCE_ON_FAILURE("pause_copy_take_blocked");
      return CopyTake::Blocked;
    }
    WaitForPush(lock);
  }
}

bool CopyInBatches::TryTake(size_t max, std::vector<CopyBatchLease>& out) {
  Lock lock{_waiters.lock};
  ThrowIfFailed(lock);
  return TakeReady(max, out);
}

CopyBatchLease CopyInBatches::Next() {
  Lock lock{_waiters.lock};
  for (;;) {
    ThrowIfFailed(lock);
    if (_taken != _pushed) {
      return CopyBatchLease{*this, *_window[_taken++ - _watermark]};
    }
    if (_finished || _aborted || std::exchange(_nudged, false)) {
      return {};
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

bool CopyInBatches::TakeReady(size_t max, std::vector<CopyBatchLease>& out) {
  if (_taken == _pushed) {
    return false;
  }
  const auto end = _taken + std::min<uint64_t>(max, _pushed - _taken);
  while (_taken != end) {
    out.emplace_back(*this, *_window[_taken - _watermark]);
    ++_taken;
  }
  return true;
}

std::vector<duckdb::InterruptState> CopyInBatches::TakeWaiters(uint64_t count) {
  if (_parked == 0 || count == 0) {
    return {};
  }
  auto waiters = _waiters.TakeBlockedTasks(count);
  _parked -= waiters.size();
  return waiters;
}

std::vector<duckdb::InterruptState> CopyInBatches::TakeAllWaiters() {
  _parked = 0;
  return _waiters.TakeBlockedTasks();
}

void CopyInBatches::Starve() const {
  if (_on_starve) {
    _on_starve();
  }
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
  ++_parked;
  lock.unlock();
  Starve();
  SDB_PARK_ONCE_ON_FAILURE("pause_copy_take_blocked");
  signal->Await();
  lock.lock();
}

}  // namespace sdb::pg
