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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>

#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/exe/executor.hpp>

namespace sdb::pg {

// COPY FROM STDIN rendezvous between the io feeder coroutine and the DuckDB
// worker that runs the COPY. DuckDB's FileSystem::Read is a synchronous,
// un-yielding call, so the worker BLOCKS here pulling CopyData while the io
// coroutine feeds it. Strict lock-step with a single in-flight borrowed view
// (no queue, no per-packet copy): the feeder publishes a view into the recv
// buffer, the worker drains it, then the feeder advances to the next frame.
// One OneShotEvent supports both the worker's blocking Wait() and the feeder's
// co_await (AwaitOn).
class CopyInBridge {
 public:
  // ---- DuckDB worker side (synchronous, blocks a scheduler thread) ----
  // Fill up to `n` bytes; returns bytes read (0 = clean EOF after CopyDone).
  // Rethrows the feeder's error (CopyFail / lost connection) if one was set.
  int64_t Read(char* out, int64_t n) {
    int64_t total = 0;
    while (total < n) {
      if (_len == 0) {
        if (_eof) {
          break;
        }
        if (_err) {
          std::rethrow_exception(_err);
        }
        _data_ready.Wait();
        _data_ready.Reset();
        if (_len == 0) {
          if (_err) {
            std::rethrow_exception(_err);
          }
          break;  // _eof
        }
      }
      const auto take = static_cast<size_t>(
        std::min<int64_t>(n - total, static_cast<int64_t>(_len)));
      std::memcpy(out + total, _ptr, take);
      _ptr += take;
      _len -= take;
      total += static_cast<int64_t>(take);
      if (_len == 0) {
        _want_more.Set();  // ask the feeder for the next frame
      }
    }
    return total;
  }

  // ---- io feeder side (coroutine) ----
  // Publish a borrowed view of the current CopyData payload, wake the worker.
  void Publish(const char* data, size_t len) noexcept {
    _ptr = data;
    _len = len;
    _data_ready.Set();
  }
  // co_await until the worker has fully drained the published view.
  auto Drained(yaclib::IExecutor& io) noexcept { return _want_more.AwaitOn(io); }
  void ResetDrained() noexcept { _want_more.Reset(); }
  // CopyDone: signal clean end of input.
  void Finish() noexcept {
    _eof = true;
    _data_ready.Set();
  }
  // CopyFail / lost connection: the worker's Read rethrows this, aborting COPY.
  void Fail(std::exception_ptr err) noexcept {
    _err = std::move(err);
    _data_ready.Set();
  }
  // Worker errored (bad row / constraint): wake the feeder so it stops feeding
  // and switches to draining the client's remaining CopyData.
  void Abort() noexcept {
    _aborted = true;
    _want_more.Set();
  }
  bool Aborted() const noexcept { return _aborted; }

 private:
  yaclib::OneShotEvent _data_ready;
  yaclib::OneShotEvent _want_more;
  const char* _ptr = nullptr;
  size_t _len = 0;
  bool _eof = false;
  bool _aborted = false;
  std::exception_ptr _err;
};

}  // namespace sdb::pg
