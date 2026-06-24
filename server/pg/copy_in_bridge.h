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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <span>
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
      // Same per-frame latch as Window(): wait for and consume exactly one
      // _data_ready Set per frame, gated by _armed -- NOT the live _len, which
      // races (the feeder can publish the next frame before the worker
      // re-checks _len, leaving a _data_ready Set unconsumed so the next
      // Publish/Finish double-Sets and corrupts the event -> SIGSEGV).
      if (_armed) {
        if (_err) {
          std::rethrow_exception(_err);
        }
        _data_ready.Wait();
        _data_ready.Reset();
        _armed = false;
      }
      if (_len == 0) {  // EOF (Finish left _len 0) or error
        if (_err) {
          std::rethrow_exception(_err);
        }
        break;
      }
      const auto take = static_cast<size_t>(
        std::min<int64_t>(n - total, static_cast<int64_t>(_len)));
      std::memcpy(out + total, _ptr, take);
      _ptr += take;
      _len -= take;
      total += static_cast<int64_t>(take);
      if (_len == 0) {
        _armed = true;
        _want_more.Set();  // ask the feeder for the next frame
      }
    }
    return total;
  }

  // Zero-copy borrow: block until the next CopyData frame is published, then
  // hand back a view into the recv buffer. An empty span is clean EOF
  // (CopyDone). Rethrows the feeder's error if one was set. The parser decodes
  // straight out of this span and calls Consume() as it advances.
  //
  // The wait is gated by the per-frame `_armed` latch (shared with Read; set at
  // construction and whenever Consume/Read drains a frame), so it consumes the
  // feeder's `_data_ready` Set exactly once per frame REGARDLESS of how much
  // parser work runs between draining a frame and asking for the next.
  std::span<const char> Window() {
    if (_armed) {
      if (_err) {
        std::rethrow_exception(_err);
      }
      _data_ready.Wait();
      _data_ready.Reset();
      _armed = false;
      if (_len == 0 && _err) {
        std::rethrow_exception(_err);
      }
    }
    return {_ptr, _len};  // empty span == EOF (Finish set _eof, left _len 0)
  }

  // Advance past `n` borrowed bytes; when the current frame fully drains,
  // re-arm the borrow latch and fire the want-more handshake so the feeder
  // advances to the next CopyData frame (which the next Window() will wait
  // for).
  void Consume(size_t n) noexcept {
    _ptr += n;
    _len -= n;
    if (_len == 0) {
      _armed = true;
      _want_more.Set();
    }
  }

  // ---- io feeder side (coroutine) ----
  // The three feeder->worker signals (Publish/Finish/Fail) all Set _data_ready;
  // once Abort() has fired (the worker errored without consuming the last
  // signal), a further Set would be a double-Set with no intervening worker
  // Reset -> OneShotEvent corruption/SIGSEGV. So each is a no-op after Abort.
  // This is reliable, not racy: the feeder only reaches Finish/Fail after a
  // Drained() wake, and Abort() releases `_aborted` before Set()ing _want_more,
  // so the wake carries the happens-before that makes Aborted() observe true.

  // Publish a borrowed view of the current CopyData payload, wake the worker.
  void Publish(const char* data, size_t len) noexcept {
    if (Aborted()) {
      return;
    }
    _ptr = data;
    _len = len;
    _data_ready.Set();
  }
  // co_await until the worker has fully drained the published view.
  auto Drained(yaclib::IExecutor& io) noexcept {
    return _want_more.AwaitOn(io);
  }
  void ResetDrained() noexcept { _want_more.Reset(); }
  // CopyDone: signal clean end of input.
  void Finish() noexcept {
    if (Aborted()) {
      return;
    }
    _eof = true;
    _data_ready.Set();
  }
  // CopyFail / lost connection: the worker's Read rethrows this, aborting COPY.
  void Fail(std::exception_ptr err) noexcept {
    if (Aborted()) {
      return;
    }
    _err = std::move(err);
    _data_ready.Set();
  }
  // Worker errored (bad row / constraint, or failed before consuming any input
  // -- e.g. a non-seekable format like parquet from STDIN): wake the feeder so
  // it stops feeding and switches to draining the client's remaining CopyData.
  void Abort() noexcept {
    _aborted.store(true, std::memory_order_release);
    _want_more.Set();
  }
  bool Aborted() const noexcept {
    return _aborted.load(std::memory_order_acquire);
  }

 private:
  yaclib::OneShotEvent _data_ready;
  yaclib::OneShotEvent _want_more;
  const char* _ptr = nullptr;
  size_t _len = 0;
  bool _eof = false;
  // Set by the session (worker side) when the COPY worker errors; read by the
  // io feeder. Atomic so the feeder's Publish/Finish/Fail guards observe it.
  std::atomic<bool> _aborted = false;
  // Per-frame latch shared by both worker paths (Read and Window/Consume): true
  // means the next Read/Window must wait for and consume exactly one
  // `_data_ready` Set. Armed at start (first frame) and re-armed each time a
  // frame fully drains. Gating on this instead of the live `_len` is what
  // avoids the double-Set race that SIGSEGV'd under load.
  bool _armed = true;
  std::exception_ptr _err;
};

}  // namespace sdb::pg
