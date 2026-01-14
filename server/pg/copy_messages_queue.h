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

#include <condition_variable>
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
  void AppendCopyDataMsg(std::string data) { AppendImpl(std::move(data)); }

  void AppendCopyDoneMsg() { AppendImpl({}); }

  void Abort() {
    std::unique_lock lock{_mutex};
    _queue = {};
    _aborted = true;
    _cv.notify_one();
  }

  CopyMsg GetMsg() {
    std::unique_lock lock{_mutex};
    _cv.wait(lock, [this] { return !_queue.empty() || _aborted; });
    
    if (_aborted) {
      return CopyMsg{};
    }

    auto data = std::move(_queue.front());
    _queue.pop();
    return CopyMsg{.data = std::move(data)};
  }

 private:
  void AppendImpl(std::string data) {
    std::unique_lock lock{_mutex};
    if (_aborted) {
      return;
    }
    
    _queue.push(std::move(data));
    _cv.notify_one();
  }

  bool _aborted = false;
  std::condition_variable _cv;
  std::mutex _mutex;
  std::queue<std::string> _queue;
};

}  // namespace sdb::pg
