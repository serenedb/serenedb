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

#include "drain_gate.h"

#include <absl/time/time.h>

#include <iresearch/utils/assert.hpp>

namespace sdb::utils {
namespace {

constexpr absl::Duration kPoll = absl::Milliseconds(100);

bool IsOpen(std::atomic<bool>* closed) { return !closed->load(); }

bool IsDrained(std::atomic<uint32_t>* in_flight) {
  return in_flight->load() == 0;
}

}  // namespace

auto DrainGate::TryEnter() noexcept -> Pass {
  _in_flight.fetch_add(1);
  if (!_closed.load()) {
    return Pass{this};
  }
  Leave();
  return {};
}

auto DrainGate::Enter() -> Pass {
  for (;;) {
    if (auto pass = TryEnter()) {
      return pass;
    }
    absl::MutexLock lock{&_mutex, absl::Condition{IsOpen, &_closed}};
  }
}

bool DrainGate::Close(absl::FunctionRef<bool()> cancelled) noexcept {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(!_closed.load());
  _closed.store(true);
  while (
    !_mutex.AwaitWithTimeout(absl::Condition{IsDrained, &_in_flight}, kPoll)) {
    if (cancelled()) {
      _closed.store(false);
      return false;
    }
  }
  return true;
}

void DrainGate::Open() noexcept {
  absl::MutexLock lock{&_mutex};
  _closed.store(false);
}

bool DrainGate::WaitOpen(absl::FunctionRef<bool()> cancelled) {
  absl::MutexLock lock{&_mutex};
  while (!_mutex.AwaitWithTimeout(absl::Condition{IsOpen, &_closed}, kPoll)) {
    if (cancelled()) {
      return false;
    }
  }
  return true;
}

void DrainGate::Leave() noexcept {
  [[maybe_unused]] const auto previous = _in_flight.fetch_sub(1);
  SDB_ASSERT(previous != 0);
  if (_closed.load()) {
    absl::MutexLock lock{&_mutex};
  }
}

}  // namespace sdb::utils
