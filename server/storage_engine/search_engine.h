////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iresearch/formats/ann_build_env.hpp>
#include <memory>
#include <yaclib/algo/wait_group.hpp>

#include "absl/synchronization/mutex.h"
#include "basics/containers/flat_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "rest_server/database_path_feature.h"
#include "search/search_db_wal.h"

namespace sdb {
namespace search {

class InvertedIndexStorage;
class SearchTable;

class SearchEngine;
SearchEngine& GetSearchEngine();

uint32_t AnnAcquireWorkers(uint32_t want) noexcept;
void AnnReleaseWorkers(uint32_t n) noexcept;
const irs::AnnBuildEnv& AnnBuildEnv();

class SearchEngine final {
 public:
  inline static SearchEngine* gInstance = nullptr;

  static int MaxConcurrentCompactions() noexcept;
  static int MaxConcurrentMerges() noexcept;

  static uint32_t MaxAnnBuildWorkers() noexcept;

  static uint32_t MaxAnnWorkersPerBuild() noexcept;

  SearchEngine();
  ~SearchEngine();

  void start();
  void stop();

  std::filesystem::path GetPersistedPath(ObjectId database_id) const;

  SearchDbWal& GetDbWal(ObjectId database_id);

  template<class Storage>
  void StartTasks(const std::shared_ptr<Storage>& storage);

  bool IsStopping() const noexcept {
    return _stopping.load(std::memory_order_acquire);
  }

  void RequestStop() noexcept {
    _stopping.store(true, std::memory_order_release);
  }

  bool TryAcquireCompaction() noexcept {
    const int cap = MaxConcurrentMerges();
    auto cur = _running_compactions.load(std::memory_order_relaxed);
    while (cur < cap) {
      if (_running_compactions.compare_exchange_weak(
            cur, cur + 1, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return true;
      }
    }
    return false;
  }
  void ReleaseCompaction() noexcept {
    _running_compactions.fetch_sub(1, std::memory_order_release);
  }

  int FreeCompactionSlots() const noexcept {
    const int cur = _running_compactions.load(std::memory_order_acquire);
    return std::max(0, MaxConcurrentCompactions() - cur);
  }

  uint32_t AcquireAnnWorkers(uint32_t want) noexcept {
    const uint32_t cap = MaxAnnBuildWorkers();
    const uint32_t ceiling = std::clamp(want, 1U, MaxAnnWorkersPerBuild());
    auto cur = _running_ann_workers.load(std::memory_order_relaxed);
    for (;;) {
      const uint32_t grant =
        std::clamp(cur < cap ? cap - cur : 0U, 1U, ceiling);
      if (_running_ann_workers.compare_exchange_weak(
            cur, cur + grant, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return grant;
      }
    }
  }
  void ReleaseAnnWorkers(uint32_t n) noexcept {
    _running_ann_workers.fetch_sub(n, std::memory_order_release);
  }

 private:
  DatabasePathFeature& _dir_feature;
  absl::Mutex _db_wals_mu;
  containers::FlatHashMap<ObjectId, std::unique_ptr<SearchDbWal>> _db_wals;
  std::atomic<bool> _stopping{false};
  std::atomic<int> _running_compactions{0};
  std::atomic<uint32_t> _running_ann_workers{0};
  yaclib::WaitGroup<> _loops{1};
};

}  // namespace search
}  // namespace sdb
