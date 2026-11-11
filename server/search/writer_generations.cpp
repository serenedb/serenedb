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

#include "search/writer_generations.h"

#include <absl/synchronization/mutex.h>

#include <iresearch/utils/assert.hpp>

namespace sdb::search {

auto WriterGenerations::Register() noexcept -> Writer {
  auto state = _state.load(std::memory_order_relaxed);
  unsigned slot = 0;
  do {
    slot = static_cast<unsigned>(state & kGeneration);
  } while (!_state.compare_exchange_weak(state, state + One(slot)));
  return Writer{this, Leaver{slot}};
}

void WriterGenerations::Drain(duckdb::ClientContext& context,
                              const Writer* own) {
  SDB_ASSERT(!own || !*own || own->get() == this);
  const auto others = [&](unsigned slot) {
    const bool mine = own && *own && own->get_deleter().slot == slot;
    return Writers(_state.load(), slot) - (mine ? 1 : 0);
  };
  absl::MutexLock lock{&duckdb::ClientContext::AwaitMutex()};
  const auto prior = static_cast<unsigned>(_state.load() & kGeneration);
  context.Await([&] { return others(prior ^ 1U) == 0; });
  _state.fetch_xor(kGeneration);
  context.Await([&] { return others(prior) == 0; });
}

void WriterGenerations::Leave(unsigned slot) noexcept {
  const auto previous = _state.fetch_sub(One(slot));
  SDB_ASSERT(Writers(previous, slot) > 0);
  if (slot != (previous & kGeneration) && Writers(previous, slot) <= 2) {
    duckdb::ClientContext::Notify();
  }
}

}  // namespace sdb::search
