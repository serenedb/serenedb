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

}  // namespace

bool DrainGate::IsOpen(std::atomic<uint32_t>* state) noexcept {
  return !(state->load() & kClosed);
}

bool DrainGate::IsDrained(std::atomic<uint32_t>* state) noexcept {
  return state->load() == kClosed;
}

auto DrainGate::TryEnter() noexcept -> Pass {
  if (!(_state.fetch_add(kPass) & kClosed)) {
    return Pass{this};
  }
  Leave();
  return {};
}

auto DrainGate::Enter() -> Pass {
  if (auto pass = TryEnter()) {
    return pass;
  }
  absl::MutexLock lock{&_mutex, absl::Condition{IsOpen, &_state}};
  [[maybe_unused]] const auto previous = _state.fetch_add(kPass);
  SDB_ASSERT(!(previous & kClosed));
  return Pass{this};
}

bool DrainGate::Close(absl::FunctionRef<bool()> cancelled) noexcept {
  absl::MutexLock lock{&_mutex};
  [[maybe_unused]] const auto previous = _state.fetch_or(kClosed);
  SDB_ASSERT(!(previous & kClosed));
  while (!_mutex.AwaitWithTimeout(absl::Condition{IsDrained, &_state}, kPoll)) {
    if (cancelled()) {
      _state.fetch_and(~kClosed);
      return false;
    }
  }
  return true;
}

void DrainGate::Open() noexcept {
  absl::MutexLock lock{&_mutex};
  [[maybe_unused]] const auto previous = _state.fetch_and(~kClosed);
  SDB_ASSERT(previous & kClosed);
}

bool DrainGate::WaitOpen(absl::FunctionRef<bool()> cancelled) {
  absl::MutexLock lock{&_mutex};
  while (!_mutex.AwaitWithTimeout(absl::Condition{IsOpen, &_state}, kPoll)) {
    if (cancelled()) {
      return false;
    }
  }
  return true;
}

void DrainGate::Leave() noexcept {
  const auto previous = _state.fetch_sub(kPass);
  SDB_ASSERT(previous >= kPass);
  if (previous == (kClosed | kPass)) {
    absl::MutexLock lock{&_mutex};
  }
}

}  // namespace sdb::utils
