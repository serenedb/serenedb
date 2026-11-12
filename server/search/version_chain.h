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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/constants.hpp>
#include <memory>
#include <span>
#include <vector>

#include "catalog/walk.h"

namespace sdb::search {

using Tick = duckdb::transaction_t;

template<typename State>
struct Version {
  Version(std::shared_ptr<const State> state, Tick tick, Version* older)
    : tick{tick}, state{std::move(state)}, prev{older} {}

  const Tick tick;
  const std::shared_ptr<const State> state;
  std::atomic<Version*> prev;
  Version* next = nullptr;
};

template<typename State>
class VersionChain final {
 public:
  explicit VersionChain(std::shared_ptr<const State> initial);
  ~VersionChain();
  VersionChain(const VersionChain&) = delete;
  VersionChain& operator=(const VersionChain&) = delete;

  const Version<State>& At(Tick snapshot, catalog::WalkCounters& walks) const {
    const catalog::WalkGuard guard{walks};
    const auto* version = _head.load(std::memory_order_acquire);
    while (version->tick > snapshot) {
      const auto* older = version->prev.load(std::memory_order_acquire);
      if (!older) {
        break;
      }
      version = older;
    }
    return *version;
  }

  std::shared_ptr<const State> Latest(catalog::WalkCounters& walks) const {
    const catalog::WalkGuard guard{walks};
    return _head.load(std::memory_order_acquire)->state;
  }

  const Version<State>& Head() const noexcept {
    return *_head.load(std::memory_order_acquire);
  }

  void Publish(std::shared_ptr<const State> state, Tick tick)
    ABSL_LOCKS_EXCLUDED(_mutex);
  void ReplaceInitial(std::shared_ptr<const State> state)
    ABSL_LOCKS_EXCLUDED(_mutex);
  void Prune(std::span<const Tick> running, Tick published)
    ABSL_LOCKS_EXCLUDED(_mutex);
  bool FreeRetired() ABSL_LOCKS_EXCLUDED(_mutex);

 private:
  struct Retired {
    uint64_t epoch = 0;
    std::unique_ptr<Version<State>> version;
  };

  static constexpr size_t kMinGarbageBytes = size_t{1} << 20;

  static size_t Bytes(const Version<State>& version) noexcept;
  void Unlink(Version<State>* version) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Retire(Version<State>* version) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void AddGarbage(const Version<State>& version)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  std::vector<std::unique_ptr<Version<State>>> TakeFree()
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  std::atomic<Version<State>*> _head;
  absl::Mutex _mutex;
  Version<State>* _tail ABSL_GUARDED_BY(_mutex);
  std::vector<Retired> _retired ABSL_GUARDED_BY(_mutex);
  size_t _garbage_bytes ABSL_GUARDED_BY(_mutex) = 0;
  size_t _garbage_trigger ABSL_GUARDED_BY(_mutex) = kMinGarbageBytes;
  std::atomic<uint32_t> _older{0};
  std::atomic<uint32_t> _retired_count{0};
};

}  // namespace sdb::search
