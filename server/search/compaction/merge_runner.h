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

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/exe/executor.hpp>

#include "search/compaction/merge_registry.h"
#include "search/maintenance.h"

namespace sdb::search {

uint64_t MergeBytes(irs::CompactionView candidates) noexcept;

uint64_t MergeSizeCap(size_t slots, size_t merges) noexcept;

class MergeJob {
 public:
  virtual ~MergeJob() = default;

  virtual irs::CompactionView Candidates() const noexcept = 0;

  virtual yaclib::Future<MergeResult> Run(const std::atomic_bool& stop) = 0;
};

class MergeRunner {
 public:
  struct Relation {
    struct State {
      size_t running = 0;
      uint32_t paused = 0;
      uint32_t failures = 0;
      uint64_t round = 0;
      bool waiting = false;
      irs::containers::FlatHashMap<std::string, uint64_t> blocked;
    };

    void NudgeCompaction() noexcept {
      compaction_gen.fetch_add(1, std::memory_order_release);
    }
    uint64_t CompactionGeneration() const noexcept {
      return compaction_gen.load(std::memory_order_acquire);
    }

    uint64_t id = 0;
    std::atomic<uint64_t> compaction_gen{0};
    State state;
  };

  struct Resume {
    void operator()(Relation* relation) const noexcept;

    MergeRunner* runner = nullptr;
  };

  using Lease = std::unique_ptr<Relation, Resume>;

  explicit MergeRunner(yaclib::IExecutor& executor, size_t slots);
  ~MergeRunner();

  MergeRunner(const MergeRunner&) = delete;
  MergeRunner& operator=(const MergeRunner&) = delete;

  size_t MergesPerRound() const noexcept {
    return std::max<size_t>(1, _slots / 2) + 1;
  }

  size_t Wanted(const Relation& relation) const;
  std::vector<std::string> StartRound(Relation& relation);

  size_t Start(const std::shared_ptr<Relation>& relation,
               std::vector<std::unique_ptr<MergeJob>> jobs);
  void Forget(const Relation& relation);
  void StopRunning(const Relation& relation);
  Lease TakeOver(const std::shared_ptr<Relation>& relation);
  void Interrupt() noexcept;
  void Stop();

  [[nodiscard]] bool TryHoldBig() noexcept;
  void ReleaseBig() noexcept;

  uint64_t StartForeground(uint64_t relation_id, MergeOrigin origin,
                           irs::MergeStats inputs);
  void FinishForeground(uint64_t id, MergeOutcome outcome,
                        irs::MergeStats stats, std::string error);

  std::vector<MergeRecord> Active(uint64_t relation_id) const;
  std::vector<MergeRecord> History(uint64_t relation_id);

 private:
  struct Entry;

  using Running = irs::containers::FlatHashMap<Entry*, std::unique_ptr<Entry>>;
  using Waiting = std::vector<std::shared_ptr<Relation>>;

  auto& StateOf(auto& relation) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex) {
    return relation.state;
  }
  bool Admits(uint64_t bytes) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Hold() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  Waiting Release() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool NoneRunning() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  static bool NoneRunningFor(const Relation* relation);
  void Wait(const std::shared_ptr<Relation>& relation)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Unwait(const Relation& relation) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  Waiting TakeWaiting() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  template<typename Pred>
  void StopIf(Pred pred) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void UpdateGauges() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Launch(std::span<Entry* const> started);
  yaclib::Future<> Execute(Entry& entry);
  void Finish(Entry& entry, MergeResult result) noexcept;

  yaclib::IExecutor& _executor;
  const size_t _slots;
  mutable absl::Mutex _mutex;
  MergeRegistry _registry ABSL_GUARDED_BY(_mutex);
  bool _stopping ABSL_GUARDED_BY(_mutex) = false;
  size_t _merges ABSL_GUARDED_BY(_mutex) = 0;
  Running _running ABSL_GUARDED_BY(_mutex);
  Waiting _waiting ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
