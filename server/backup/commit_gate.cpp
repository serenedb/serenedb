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

#include "backup/commit_gate.h"

#include <absl/base/no_destructor.h>

#include <iresearch/utils/assert.hpp>

namespace sdb::backup {
CommitGate& CommitGate::Instance() {
  static absl::NoDestructor<CommitGate> gGate;
  return *gGate;
}

auto CommitGate::Enter() -> Pass {
  for (;;) {
    _in_flight.fetch_add(1);
    if (!_closed.load()) {
      return Pass{this};
    }
    Leave();
    absl::MutexLock lock{&_mutex};
    while (_closed.load()) {
      _cv.Wait(&_mutex);
    }
  }
}

bool CommitGate::Close(absl::FunctionRef<bool()> cancelled) noexcept {
  constexpr auto kPoll = absl::Milliseconds(100);
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(!_closed.load());
  _closed.store(true);
  while (_in_flight.load() != 0) {
    if (_cv.WaitWithTimeout(&_mutex, kPoll) && cancelled()) {
      _closed.store(false);
      _cv.SignalAll();
      return false;
    }
  }
  return true;
}

void CommitGate::Open() noexcept {
  absl::MutexLock lock{&_mutex};
  _closed.store(false);
  _cv.SignalAll();
}

void CommitGate::Leave() noexcept {
  [[maybe_unused]] const auto previous = _in_flight.fetch_sub(1);
  SDB_ASSERT(previous != 0);
  if (_closed.load()) {
    absl::MutexLock lock{&_mutex};
    _cv.SignalAll();
  }
}

void CommitGate::Pass::Release() noexcept {
  if (auto* gate = std::exchange(_gate, nullptr)) {
    gate->Leave();
  }
}

}  // namespace sdb::backup
