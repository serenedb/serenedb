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

// Single-parker rendezvous: at most ONE coroutine may be parked at a time; any
// thread may Kick(). A kick with nobody parked is remembered (kKicked) and
// consumed by the next Wait, which then continues inline. The kicked coroutine
// resumes via the executor it passed to Wait(); the promise IS the submitted
// yaclib::Job (no allocation). Waiters re-check their condition in a loop, so
// spurious wakeups are fine.
//
// One word: kEmpty, kKicked, or the parked Awaiter* (the kicker needs it, so it
// IS the state). All acq_rel, no fence: every Kick is a single exchange (a
// successful RMW), so it reads-from the waiter's exchange and carries the
// kicker's publication across -- no store/load pair for seq_cst to guard.
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
      if (prev == kEmpty) {
        return true;  // parked; a Kick will Submit us
      }
      // prev == kKicked: a kick was pending. Take the slot back; if a racing
      // Kick already replaced us with kKicked (it Submitted us) we stay parked.
      return _gate._state.exchange(kEmpty, std::memory_order_acq_rel) ==
             kKicked;
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
    const auto prev = _state.exchange(kKicked, std::memory_order_acq_rel);
    if (prev > kKicked) {  // displaced a parked Awaiter*
      auto* awaiter = reinterpret_cast<Awaiter*>(prev);
      awaiter->_resume.Submit(*awaiter->_job);
    }
  }

 private:
  std::atomic<uintptr_t> _state{kEmpty};
};

}  // namespace sdb::network
