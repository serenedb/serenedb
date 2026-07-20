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

#include <atomic>
#include <exception>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/exe/executor.hpp>

#include "network/cpu_resumer.h"
#include "replication/pgoutput.h"

namespace sdb::replication {

// Lock-step rendezvous handing one decoded pgoutput message at a time from the
// io feeder to the duck-side apply. Exactly one message is in flight, so the
// feeder's borrowed views into the live recv frame stay valid until the apply
// consumes it -- the borrowed-view discipline of COPY FROM STDIN's
// CopyInBridge, which this mirrors (two OneShotEvents + a per-message latch).
//
// The duck side has two consumers that never run at once: the control loop
// (between batches) and the DML scan (during a batch). The control loop must
// not hog a scheduler thread while the publisher is idle, so it waits by
// PARKING its CpuResumer (woken by RequestRun). The scan cannot co_await inside
// DuckDB execution, so it BLOCKS on `_ready` (woken by Set), bounded to a batch
// like COPY's synchronous scan. `_scan_active` (set by the control loop around
// each execution) tells the feeder which consumer to wake.
//
// `_armed` is the per-message latch (as in CopyInBridge): the scan waits for
// and consumes EXACTLY ONE `_ready` Set per message, then disarms; Advance
// re-arms. So the feeder's one Set per Publish pairs 1:1 with one scan
// Wait+Reset, no matter how much execution runs between draining a row and
// asking for the next (e.g. across a chunk boundary) -- without the latch that
// Set would go unconsumed and the next one would double-fire (corrupting the
// event).
class ReplStream {
 public:
  // ---- feeder (io) ----
  // Publish the current message and wake the active consumer. Returns false if
  // the duck side aborted (stop feeding).
  bool Publish(const PgOutputMessage* msg) noexcept {
    if (_aborted.load(std::memory_order_acquire)) {
      return false;
    }
    _msg.store(msg, std::memory_order_release);
    Wake();
    return true;
  }
  // co_await until the duck side consumed the published message.
  auto Drained(yaclib::IExecutor& io) noexcept { return _consumed.AwaitOn(io); }
  void ResetDrained() noexcept { _consumed.Reset(); }
  // No more messages (clean end of stream).
  void Finish() noexcept {
    if (_aborted.load(std::memory_order_acquire)) {
      return;
    }
    _eof.store(true, std::memory_order_release);
    Wake();
  }
  // The feeder failed (lost connection / decode error); the apply rethrows.
  void Fail(std::exception_ptr err) noexcept {
    if (_aborted.load(std::memory_order_acquire)) {
      return;
    }
    _err = std::move(err);  // published by the _eof release below
    _eof.store(true, std::memory_order_release);
    Wake();
  }
  bool Aborted() const noexcept {
    return _aborted.load(std::memory_order_acquire);
  }

  // ---- duck side ----
  void SetTask(network::CpuResumer* task) noexcept { _task = task; }
  // Around a batch execution the scan (not the control loop) is the consumer.
  // The batch's first message is already current (published while the control
  // loop was consumer, via RequestRun -- not a `_ready` Set), so disarm: the
  // scan reads that first message without waiting.
  void ScanActive(bool active) noexcept {
    _scan_active.store(active, std::memory_order_release);
    if (active) {
      _armed = false;
    }
  }

  // Control loop (parks via the task between polls): true once a message/EOF is
  // available.
  bool Ready() const noexcept {
    return _msg.load(std::memory_order_acquire) != nullptr ||
           _eof.load(std::memory_order_acquire);
  }
  // The current message, or nullptr at EOF. Rethrows the feeder's error. Used
  // by the control loop after Ready() (never blocks).
  const PgOutputMessage* Current() {
    if (_eof.load(std::memory_order_acquire) && _err) {
      std::rethrow_exception(_err);
    }
    return _msg.load(std::memory_order_acquire);
  }
  // Scan: block for the next message; nullptr at EOF. Rethrows on failure.
  const PgOutputMessage* PeekBlocking() {
    if (_armed) {
      _ready.Wait();
      _ready.Reset();
      _armed = false;
    }
    if (_eof.load(std::memory_order_acquire) && _err) {
      std::rethrow_exception(_err);
    }
    return _msg.load(std::memory_order_acquire);
  }
  // Consume the current message; re-arm the scan latch and release the feeder.
  void Advance() noexcept {
    _msg.store(nullptr, std::memory_order_release);
    _armed = true;
    _consumed.Set();
  }
  // The apply errored; wake the feeder so it stops.
  void Abort() noexcept {
    _aborted.store(true, std::memory_order_release);
    _consumed.Set();
  }

 private:
  void Wake() noexcept {
    if (_scan_active.load(std::memory_order_acquire)) {
      _ready.Set();
    } else if (_task != nullptr) {
      _task->RequestRun();
    }
  }

  std::atomic<const PgOutputMessage*> _msg{nullptr};
  std::atomic<bool> _eof{false};
  bool _armed = false;  // scan-side per-message latch (duck-only; see above)
  std::exception_ptr _err;  // set before _eof release; read after _eof acquire
  std::atomic<bool> _aborted{false};
  std::atomic<bool> _scan_active{false};
  network::CpuResumer* _task = nullptr;
  yaclib::OneShotEvent _ready;     // feeder -> scan (blocking wait)
  yaclib::OneShotEvent _consumed;  // duck -> feeder (co_await)
};

}  // namespace sdb::replication
