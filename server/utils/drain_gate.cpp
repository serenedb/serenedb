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

#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/mutex.h>

#include <iresearch/utils/assert.hpp>
#include <utility>

namespace sdb::utils {

auto DrainGate::Enter(duckdb::ClientContext& context) -> Pass {
  if (auto pass = TryEnter()) {
    return pass;
  }
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  context.Await([this] { return !IsClosed(); });
  [[maybe_unused]] const auto previous = _state.fetch_add(kPass);
  SDB_ASSERT(!(previous & kClosed));
  return Pass{this};
}

auto DrainGate::TryEnter() noexcept -> Pass {
  if (_state.fetch_add(kPass) & kClosed) {
    Leave();
    return {};
  }
  return Pass{this};
}

auto DrainGate::Close(duckdb::ClientContext& context) -> Closed {
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  [[maybe_unused]] const auto previous = _state.fetch_or(kClosed);
  SDB_ASSERT(!(previous & kClosed));
  absl::Cleanup reopen = [this] { _state.fetch_and(~kClosed); };
  context.Await([this] { return _state.load() == kClosed; });
  std::move(reopen).Cancel();
  return Closed{this};
}

void DrainGate::Leave() noexcept {
  const auto previous = _state.fetch_sub(kPass);
  SDB_ASSERT(previous >= kPass);
  if (previous == (kClosed | kPass)) {
    duckdb::ClientContext::Notify();
  }
}

void DrainGate::Open() noexcept {
  [[maybe_unused]] const auto previous = _state.fetch_and(~kClosed);
  SDB_ASSERT(previous & kClosed);
  duckdb::ClientContext::Notify();
}

}  // namespace sdb::utils
