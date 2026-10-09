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
#include <span>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/exe/executor.hpp>

#include "network/cpu_resumer.h"
#include "replication/pgoutput.h"

namespace sdb::replication {

class ReplStream {
 public:
  static bool IsIdle(const PgOutputMessage* message) noexcept {
    return message == &kIdle;
  }
  static const PgOutputMessage* Idle() noexcept { return &kIdle; }

  bool Publish(std::span<const PgOutputMessage> messages) noexcept {
    if (_aborted.load(std::memory_order_acquire)) {
      return false;
    }
    _end = messages.data() + messages.size();
    _fresh = true;
    _msg.store(messages.data(), std::memory_order_release);
    Wake();
    return true;
  }
  auto Drained(yaclib::IExecutor& io) noexcept { return _consumed.AwaitOn(io); }
  void ResetDrained() noexcept { _consumed.Reset(); }
  void Finish() noexcept {
    if (_aborted.load(std::memory_order_acquire)) {
      return;
    }
    _eof.store(true, std::memory_order_release);
    Wake();
  }
  void Fail(std::exception_ptr err) noexcept {
    if (_aborted.load(std::memory_order_acquire)) {
      return;
    }
    _err = std::move(err);
    _eof.store(true, std::memory_order_release);
    Wake();
  }
  bool Aborted() const noexcept {
    return _aborted.load(std::memory_order_acquire);
  }

  void SetTask(network::CpuResumer* task) noexcept { _task = task; }
  void ScanActive(bool active) noexcept {
    _scan_active.store(active, std::memory_order_release);
    if (active) {
      _armed = false;
    }
  }

  bool Ready() const noexcept {
    return _msg.load(std::memory_order_acquire) != nullptr ||
           _eof.load(std::memory_order_acquire);
  }
  const PgOutputMessage* Current() {
    if (_eof.load(std::memory_order_acquire) && _err) {
      std::rethrow_exception(_err);
    }
    return _msg.load(std::memory_order_acquire);
  }
  std::span<const PgOutputMessage> TakeFresh() noexcept {
    if (!_fresh) {
      return {};
    }
    _fresh = false;
    const auto* begin = _msg.load(std::memory_order_acquire);
    return {begin, _end};
  }
  void Replace(std::span<const PgOutputMessage> messages) noexcept {
    _end = messages.data() + messages.size();
    _msg.store(messages.data(), std::memory_order_relaxed);
  }
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
  void Advance() noexcept {
    const auto* next = _msg.load(std::memory_order_relaxed) + 1;
    if (next != _end) {
      _msg.store(next, std::memory_order_relaxed);
      return;
    }
    _msg.store(nullptr, std::memory_order_release);
    _armed = true;
    _consumed.Set();
  }
  void Abort() noexcept {
    _aborted.store(true, std::memory_order_release);
    _consumed.Set();
  }

 private:
  inline static const PgOutputMessage kIdle{StreamStopMessage{}};

  void Wake() noexcept {
    if (_scan_active.load(std::memory_order_acquire)) {
      _ready.Set();
    } else if (_task != nullptr) {
      _task->RequestRun();
    }
  }

  std::atomic<const PgOutputMessage*> _msg{nullptr};
  const PgOutputMessage* _end = nullptr;
  bool _fresh = false;
  std::atomic<bool> _eof{false};
  bool _armed = false;
  std::exception_ptr _err;
  std::atomic<bool> _aborted{false};
  std::atomic<bool> _scan_active{false};
  network::CpuResumer* _task = nullptr;
  yaclib::OneShotEvent _ready;
  yaclib::OneShotEvent _consumed;
};

}  // namespace sdb::replication
