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
#include <coroutine>
#include <cstdint>
#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

namespace sdb::network {

// Single-parker rendezvous: at most ONE coroutine may be parked at a time;
// any thread may Kick(). A kick arriving while nobody is parked is remembered
// and consumed by the next Wait (which then continues inline). Kicks coalesce,
// so a waiter must re-check its condition in a loop -- spurious wakeups are
// possible when kickers race. The kicked coroutine resumes via the executor it
// passed to Wait(); the promise IS the submitted yaclib::Job (no allocation,
// same pattern as QueryPump's DriveAwaiter).
class Gate {
  static constexpr uintptr_t kEmpty = 0;
  static constexpr uintptr_t kKicked = 1;

 public:
  class [[nodiscard]] Awaiter {
   public:
    Awaiter(Gate& gate, yaclib::IExecutor& resume) noexcept
      : _gate{gate}, _resume{resume} {}

    bool await_ready() const noexcept { return false; }

    template<typename Promise>
    bool await_suspend(std::coroutine_handle<Promise> handle) noexcept {
      _job = &handle.promise();
      const auto prev = _gate._state.exchange(reinterpret_cast<uintptr_t>(this),
                                              std::memory_order_acq_rel);
      if (prev != kKicked) {
        return true;
      }
      // A kick was pending; consume it and continue inline -- unless a racing
      // Kick() already grabbed this awaiter and is submitting it.
      auto self = reinterpret_cast<uintptr_t>(this);
      return !_gate._state.compare_exchange_strong(self, kEmpty,
                                                   std::memory_order_acq_rel);
    }

    void await_resume() const noexcept {}

   private:
    friend class Gate;
    Gate& _gate;
    yaclib::IExecutor& _resume;
    yaclib::Job* _job = nullptr;
  };

  Awaiter Wait(yaclib::IExecutor& resume) noexcept {
    return Awaiter{*this, resume};
  }

  void Kick() noexcept {
    auto prev = _state.load(std::memory_order_acquire);
    for (;;) {
      if (prev == kKicked) {
        return;
      }
      const auto next = prev == kEmpty ? kKicked : kEmpty;
      if (_state.compare_exchange_weak(prev, next, std::memory_order_acq_rel)) {
        if (prev != kEmpty) {
          auto* awaiter = reinterpret_cast<Awaiter*>(prev);
          awaiter->_resume.Submit(*awaiter->_job);
        }
        return;
      }
    }
  }

 private:
  std::atomic<uintptr_t> _state{kEmpty};
};

}  // namespace sdb::network
