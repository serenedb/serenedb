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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <utility>

#include "catalog/transaction.h"
#include "search/version_chain.h"

namespace sdb::search {

template<typename State>
VersionChain<State>::VersionChain(std::shared_ptr<const State> initial)
  : _head{new Version<State>{std::move(initial), 0, nullptr}},
    _tail{_head.load(std::memory_order_relaxed)} {}

template<typename State>
VersionChain<State>::~VersionChain() {
  auto* version = _head.load(std::memory_order_relaxed);
  while (version) {
    delete std::exchange(version,
                         version->prev.load(std::memory_order_relaxed));
  }
}

template<typename State>
size_t VersionChain<State>::Bytes(const Version<State>& version) noexcept {
  return sizeof(Version<State>) + VersionBytes(*version.state);
}

template<typename State>
void VersionChain<State>::Publish(std::shared_ptr<const State> state,
                                  Tick tick) {
  absl::MutexLock lock{&_mutex};
  auto* head = _head.load(std::memory_order_relaxed);
  SDB_ASSERT(tick > head->tick);
  auto* version = new Version<State>{std::move(state), tick, head};
  head->next = version;
  _head.store(version, std::memory_order_release);
  _older.fetch_add(1, std::memory_order_relaxed);
  const auto oldest = catalog::Transactions::Get().Oldest();
  while (_tail != version && _tail->next->tick <= oldest) {
    Unlink(_tail);
  }
  AddGarbage(*head);
}

template<typename State>
void VersionChain<State>::ReplaceInitial(std::shared_ptr<const State> state) {
  absl::MutexLock lock{&_mutex};
  auto* initial = _head.load(std::memory_order_relaxed);
  SDB_ASSERT(!initial->prev.load(std::memory_order_relaxed));
  auto* version = new Version<State>{std::move(state), initial->tick, nullptr};
  _tail = version;
  _head.store(version, std::memory_order_release);
  Retire(initial);
  AddGarbage(*initial);
}

template<typename State>
void VersionChain<State>::AddGarbage(const Version<State>& version) {
  _garbage_bytes += Bytes(version);
  if (_garbage_bytes > _garbage_trigger) {
    _garbage_trigger = std::numeric_limits<size_t>::max();
    catalog::Transactions::Get().RequestCollect();
  }
}

template<typename State>
void VersionChain<State>::Unlink(Version<State>* version) {
  auto* newer = version->next;
  auto* older = version->prev.load(std::memory_order_relaxed);
  newer->prev.store(older, std::memory_order_release);
  if (older) {
    older->next = newer;
  } else {
    _tail = newer;
  }
  _older.fetch_sub(1, std::memory_order_relaxed);
  Retire(version);
}

template<typename State>
void VersionChain<State>::Retire(Version<State>* version) {
  _retired.push_back({.epoch = catalog::Transactions::Get().Epoch(),
                      .version = std::unique_ptr<Version<State>>{version}});
  _retired_count.store(static_cast<uint32_t>(_retired.size()),
                       std::memory_order_relaxed);
}

template<typename State>
std::vector<std::unique_ptr<Version<State>>> VersionChain<State>::TakeFree() {
  const auto safe = catalog::Transactions::Get().SafeEpoch();
  const auto kept = absl::c_stable_partition(
    _retired, [&](const Retired& retired) { return retired.epoch > safe; });
  std::vector<std::unique_ptr<Version<State>>> freed;
  freed.reserve(static_cast<size_t>(_retired.end() - kept));
  for (auto it = kept; it != _retired.end(); ++it) {
    _garbage_bytes -= Bytes(*it->version);
    freed.push_back(std::move(it->version));
  }
  _retired.erase(kept, _retired.end());
  _retired_count.store(static_cast<uint32_t>(_retired.size()),
                       std::memory_order_relaxed);
  _garbage_trigger =
    std::max({kMinGarbageBytes, Bytes(Head()), 2 * _garbage_bytes});
  return freed;
}

template<typename State>
bool VersionChain<State>::FreeRetired() {
  if (_retired_count.load(std::memory_order_relaxed) == 0) {
    return false;
  }
  std::vector<std::unique_ptr<Version<State>>> freed;
  absl::MutexLock lock{&_mutex};
  freed = TakeFree();
  return !_retired.empty();
}

template<typename State>
void VersionChain<State>::Prune(std::span<const Tick> running, Tick published) {
  if (_older.load(std::memory_order_relaxed) == 0) {
    return;
  }
  std::vector<std::unique_ptr<Version<State>>> freed;
  absl::MutexLock lock{&_mutex};
  auto* newer = _head.load(std::memory_order_relaxed);
  auto* version = newer->prev.load(std::memory_order_relaxed);
  while (version) {
    auto* older = version->prev.load(std::memory_order_relaxed);
    const auto seen = absl::c_lower_bound(running, version->tick);
    if (newer->tick > published ||
        (seen != running.end() && *seen < newer->tick)) {
      newer = version;
    } else {
      Unlink(version);
    }
    version = older;
  }
  freed = TakeFree();
}

}  // namespace sdb::search
