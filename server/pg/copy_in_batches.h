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

#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <duckdb/function/table_function.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <exception>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/exe/executor.hpp>

#include "server/utils/message_buffer.h"

namespace sdb::pg {

inline constexpr size_t kCopyBatchBytes = 64 << 10;
inline constexpr size_t kCopyBatchesInFlight = 64;
inline constexpr size_t kCopyWakeBacklog = 4;

struct CopyBatch {
  uint64_t seq = 0;
  std::vector<std::string_view> segments;
  std::deque<std::string> owned;
  size_t bytes = 0;

  void AddView(std::string_view rows) {
    segments.emplace_back(rows);
    bytes += rows.size();
  }

  void AddOwned(std::string rows) {
    AddView(owned.emplace_back(std::move(rows)));
  }

  bool Empty() const noexcept { return segments.empty(); }
};

struct CopyInTarget {
  bool binary = false;
  char delim = '\t';
  std::string null_str = "\\N";
  bool header = false;
  std::vector<duckdb::LogicalType> types;
};

enum class CopyTake : uint8_t {
  Batches,
  End,
  Blocked,
};

class CopyBatchLease;

class CopyInBatches {
 public:
  explicit CopyInBatches(size_t capacity = kCopyBatchesInFlight);

  CopyInBatches(const CopyInBatches&) = delete;
  CopyInBatches& operator=(const CopyInBatches&) = delete;

  bool NeedSpace();
  auto SpaceReady(yaclib::IExecutor& io) noexcept { return _space.AwaitOn(io); }
  void ResetSpace() noexcept { _space.Reset(); }
  size_t RetainedBytes() const;
  void Push(std::unique_ptr<CopyBatch> batch, message::Chain released);
  void Release(message::Chain released);
  void Flush();
  void Finish();
  void Fail(std::exception_ptr error);
  void Abort();
  bool Aborted() const;

  CopyTake Take(duckdb::TableFunctionInput& input, size_t max,
                std::vector<CopyBatchLease>& out);
  bool TryTake(size_t max, std::vector<CopyBatchLease>& out);
  CopyBatchLease Next();
  void Done(uint64_t seq);

 private:
  using Lock = duckdb::annotated_unique_lock<duckdb::annotated_mutex>;

  struct Released {
    uint64_t seq;
    message::Chain chunks;
  };

  static bool CanPark(const duckdb::TableFunctionInput& input);

  bool Full() const;
  bool TakeReady(size_t max, std::vector<CopyBatchLease>& out);
  std::vector<duckdb::InterruptState> TakeOneWaiter()
    DUCKDB_REQUIRES(_waiters.lock);
  std::vector<duckdb::InterruptState> TakeWaitersIfQueued()
    DUCKDB_REQUIRES(_waiters.lock);
  void ThrowIfFailed(Lock& lock) DUCKDB_NO_THREAD_SAFETY_ANALYSIS;
  void WaitForPush(Lock& lock) DUCKDB_NO_THREAD_SAFETY_ANALYSIS;

  duckdb::StateWithBlockableTasks _waiters;
  const uint64_t _capacity;
  std::deque<std::unique_ptr<CopyBatch>> _window;
  std::deque<uint8_t> _done;
  std::deque<Released> _released;
  uint64_t _pushed = 0;
  uint64_t _taken = 0;
  uint64_t _watermark = 0;
  uint64_t _outstanding = 0;
  size_t _retained_bytes = 0;
  std::exception_ptr _error;
  bool _finished = false;
  bool _aborted = false;
  bool _space_waiter = false;
  yaclib::OneShotEvent _space;
};

class CopyBatchLease {
 public:
  CopyBatchLease() = default;
  explicit CopyBatchLease(CopyInBatches& queue, CopyBatch& batch) noexcept
    : _queue{&queue}, _batch{&batch} {}
  CopyBatchLease(CopyBatchLease&& other) noexcept
    : _queue{std::exchange(other._queue, nullptr)},
      _batch{std::exchange(other._batch, nullptr)} {}
  CopyBatchLease& operator=(CopyBatchLease&& other) noexcept {
    if (this != &other) {
      Reset();
      _queue = std::exchange(other._queue, nullptr);
      _batch = std::exchange(other._batch, nullptr);
    }
    return *this;
  }
  ~CopyBatchLease() { Reset(); }

  void Reset() noexcept {
    if (_batch) {
      std::exchange(_queue, nullptr)->Done(std::exchange(_batch, nullptr)->seq);
    }
  }

  explicit operator bool() const noexcept { return _batch != nullptr; }
  CopyBatch& operator*() const noexcept { return *_batch; }
  CopyBatch* operator->() const noexcept { return _batch; }

 private:
  CopyInBatches* _queue = nullptr;
  CopyBatch* _batch = nullptr;
};

}  // namespace sdb::pg
