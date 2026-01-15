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

#include <absl/synchronization/mutex.h>

#include <mutex>
#include <queue>
#include <string>

#include "basics/assert.h"

namespace sdb::pg {

struct CopyMsg {
  std::string data;

  bool IsDone() const { return data.empty(); }
};

// queue for CopyData / CopyDone messages which are used
// for COPY FROM STDIN implementation
class CopyMessagesQueue {
 public:
  CopyMessagesQueue(absl::Mutex& mtx) : _mtx(mtx) {}

  void AppendCopyDataMsg(std::string data) { AppendImpl(std::move(data)); }

  void AppendCopyDoneMsg() { AppendImpl({}); }

  void Abort(const std::unique_lock<absl::Mutex>& lock) {
    SDB_ASSERT(lock.owns_lock());
    SDB_ASSERT(lock.mutex() == &_mtx);

    _aborted = true;
    _queue = {};
  }

  void StartListening() {
    std::lock_guard lock{_mtx};
    _has_reader = true;
    _aborted = false;
    _queue = {};
  }

  void CloseListening() {
    std::lock_guard lock{_mtx};
    _has_reader = false;
    _aborted = false;
    _queue = {};
  }

  CopyMsg Pop(const std::unique_lock<absl::Mutex>& lock) {
    SDB_ASSERT(lock.owns_lock());
    SDB_ASSERT(lock.mutex() == &_mtx);

    auto wait_until = [&] { return !_queue.empty() || _aborted; };
    _mtx.Await(absl::Condition(&wait_until));

    if (_aborted) {
      return CopyMsg{};
    }

    CopyMsg msg{std::move(_queue.front())};
    _queue.pop();
    return msg;
  }

  absl::Mutex& Mutex() { return _mtx; }

 private:
  void AppendImpl(std::string data) {
    std::lock_guard lock{_mtx};
    if (!_has_reader) {
      return;  // PG ignores COPY data if no copy is in progress
    }
    _queue.push(std::move(data));
  }

  bool _aborted = false;
  bool _has_reader = false;
  absl::Mutex& _mtx;
  std::queue<std::string> _queue;
};

class CopyMessagesQueueIterator {
 public:
  CopyMessagesQueueIterator(CopyMessagesQueue& queue) : _queue{queue} {}

  uint64_t Next(char* pos, uint64_t length) {
    std::unique_lock lock{_queue.Mutex()};

    if (_done) {
      return 0;
    }

    uint64_t bytes_read = 0;
    while (bytes_read != length) {
      if (_cur_msg_data.empty()) {
        _cur_msg = _queue.Pop(lock);
        if (_cur_msg.IsDone()) {
          _done = true;
          return bytes_read;
        }
        _cur_msg_data = _cur_msg.data;
        // TODO: Malformed packet error (also check packet size = length)
        SDB_ASSERT(_cur_msg_data.size() >= 5);
        _cur_msg_data.remove_prefix(5);  // skip message type and length
      }
      const auto to_copy = std::min(length - bytes_read, _cur_msg_data.size());
      std::memcpy(pos, _cur_msg_data.data(), to_copy);
      pos += to_copy;
      bytes_read += to_copy;
      _cur_msg_data.remove_prefix(to_copy);
    }

    return bytes_read;
  }

 private:
  bool _done = false;
  std::string_view _cur_msg_data;
  CopyMsg _cur_msg;

  CopyMessagesQueue& _queue;
};

}  // namespace sdb::pg
