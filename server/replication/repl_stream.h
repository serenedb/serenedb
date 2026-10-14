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
#include <duckdb/parallel/interrupt.hpp>
#include <span>
#include <yaclib/algo/one_shot_event.hpp>
#include <yaclib/exe/executor.hpp>

#include "network/cpu_resumer.h"
#include "replication/pgoutput.h"

namespace sdb::replication {

class ReplStream {
 public:
  static bool IsIdle(const PgOutputMessage* message) noexcept {
    return std::holds_alternative<StreamStopMessage>(*message);
  }

  bool Publish(std::span<const PgOutputMessage> messages) noexcept {
    _end = messages.data() + messages.size();
    _fresh = true;
    _msg.store(messages.data(), std::memory_order_seq_cst);
    if (_aborted.load(std::memory_order_seq_cst)) {
      return false;
    }
    Wake();
    return true;
  }
  auto Drained(yaclib::IExecutor& io) noexcept { return _consumed.AwaitOn(io); }
  bool IsDrained() const noexcept {
    return _msg.load(std::memory_order_acquire) == nullptr;
  }
  void ResetDrained() noexcept { _consumed.Reset(); }
  void Finish() noexcept {
    if (_eof.exchange(true, std::memory_order_seq_cst) ||
        _aborted.load(std::memory_order_acquire)) {
      return;
    }
    Wake();
  }
  bool Aborted() const noexcept {
    return _aborted.load(std::memory_order_acquire);
  }

  void SetTask(network::CpuResumer* task) noexcept { _task = task; }

  bool Ready() const noexcept { return Current() != nullptr || Ended(); }
  bool Ended() const noexcept { return _eof.load(std::memory_order_acquire); }
  const PgOutputMessage* Current() const noexcept {
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
  bool Park(const duckdb::InterruptState& waiter) noexcept {
    _scan_waiter = waiter;
    _scan_waiting.store(true, std::memory_order_seq_cst);
    if (_msg.load(std::memory_order_seq_cst) == nullptr &&
        !_eof.load(std::memory_order_seq_cst)) {
      return true;
    }
    return !_scan_waiting.exchange(false, std::memory_order_seq_cst);
  }
  void Advance() noexcept {
    const auto* next = _msg.load(std::memory_order_relaxed) + 1;
    if (next != _end) {
      _msg.store(next, std::memory_order_relaxed);
      return;
    }
    _msg.store(nullptr, std::memory_order_release);
    _consumed.Set();
  }
  void Abort() noexcept {
    _aborted.store(true, std::memory_order_seq_cst);
    if (_msg.load(std::memory_order_seq_cst) != nullptr) {
      _msg.store(nullptr, std::memory_order_release);
      _consumed.Set();
    }
  }

 private:
  void Wake() noexcept {
    if (_scan_waiting.exchange(false, std::memory_order_seq_cst)) {
      _scan_waiter.Callback();
    }
    _task->RequestRun();
  }

  std::atomic<const PgOutputMessage*> _msg{nullptr};
  const PgOutputMessage* _end = nullptr;
  bool _fresh = false;
  std::atomic<bool> _eof{false};
  std::atomic<bool> _aborted{false};
  std::atomic<bool> _scan_waiting{false};
  duckdb::InterruptState _scan_waiter;
  network::CpuResumer* _task = nullptr;
  yaclib::OneShotEvent _consumed;
};

}  // namespace sdb::replication
