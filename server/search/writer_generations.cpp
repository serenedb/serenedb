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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/fault_points.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <vector>

namespace sdb::search {
namespace {

struct Waiter {
  const WriterGenerations* drains = nullptr;
  std::span<const WriterGenerations* const> written;
};

class Waiters final {
 public:
  void Enter(const Waiter& waiter) {
    absl::MutexLock lock{&_mutex};
    if (ClosesCycle(waiter)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_T_R_DEADLOCK_DETECTED), ERR_MSG("deadlock detected"),
        ERR_DETAIL("The table this statement changes was written by a "
                   "transaction that waits for this transaction to end."));
    }
    _waiters.push_back(&waiter);
  }

  void Leave(const Waiter& waiter) {
    absl::MutexLock lock{&_mutex};
    std::erase(_waiters, &waiter);
  }

 private:
  bool ClosesCycle(const Waiter& start) const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    std::vector<const Waiter*> reached;
    std::vector<const WriterGenerations*> drained{start.drains};
    while (!drained.empty()) {
      const auto* table = drained.back();
      drained.pop_back();
      for (const auto* other : _waiters) {
        if (absl::c_linear_search(reached, other) ||
            !absl::c_linear_search(other->written, table)) {
          continue;
        }
        if (absl::c_linear_search(start.written, other->drains)) {
          return true;
        }
        reached.push_back(other);
        drained.push_back(other->drains);
      }
    }
    return false;
  }

  absl::Mutex _mutex{absl::kConstInit};
  std::vector<const Waiter*> _waiters ABSL_GUARDED_BY(_mutex);
};

constinit Waiters gWaiters;

}  // namespace

auto WriterGenerations::Register() noexcept -> Writer {
  auto state = _state.load(std::memory_order_relaxed);
  unsigned slot = 0;
  do {
    slot = static_cast<unsigned>(state & kGeneration);
  } while (!_state.compare_exchange_weak(state, state + One(slot)));
  return Writer{this, Leaver{slot}};
}

void WriterGenerations::Drain(
  duckdb::ClientContext& context, const Writer* own,
  std::span<const WriterGenerations* const> written) {
  SDB_ASSERT(!own || !*own || own->get() == this);
  const auto others = [&](unsigned slot) {
    const bool mine = own && *own && own->get_deleter().slot == slot;
    return Writers(_state.load(), slot) - (mine ? 1 : 0);
  };
  const Waiter waiter{.drains = this, .written = written};
  gWaiters.Enter(waiter);
  absl::Cleanup leave = [&] { gWaiters.Leave(waiter); };
  SDB_PARK_ONCE_ON_FAILURE("writer_drain_waits");
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
