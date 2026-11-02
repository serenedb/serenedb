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
#include <iresearch/index/compaction/merge_ticket.hpp>
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

enum class MergeLane : uint8_t {
  Small = 0,
  Big,
};

struct MergeEstimate {
  uint64_t rewrite_bytes = 0;
  double score = 0;
  MergeLane lane = MergeLane::Big;
};

MergeEstimate EstimateMerge(irs::CompactionView candidates) noexcept;

class MergeJob {
 public:
  virtual ~MergeJob() = default;

  virtual irs::CompactionView Candidates() const noexcept = 0;

  virtual bool Stale() const noexcept = 0;

  virtual yaclib::Future<MergeResult> Run(const std::atomic_bool& stop,
                                          size_t helpers) = 0;
};

class MergeScheduler {
 public:
  struct Options {
    size_t threads = 1;
    bool gauges = true;
  };

  struct Relation {
    void NudgeCompaction() noexcept {
      compaction_gen.fetch_add(1, std::memory_order_release);
    }
    uint64_t CompactionGeneration() const noexcept {
      return compaction_gen.load(std::memory_order_acquire);
    }

    uint64_t id = 0;
    std::atomic<uint64_t> compaction_gen{0};
    size_t queued = 0;
    size_t running = 0;
    size_t running_big = 0;
    uint32_t paused = 0;
    uint32_t failures = 0;
    uint64_t round = 0;
    irs::containers::FlatHashMap<std::string, uint64_t> blocked;
  };

  struct Resume {
    void operator()(Relation* relation) const noexcept;

    MergeScheduler* scheduler = nullptr;
  };

  using Lease = std::unique_ptr<Relation, Resume>;

  MergeScheduler(yaclib::IExecutor& executor, Options options);
  ~MergeScheduler();

  MergeScheduler(const MergeScheduler&) = delete;
  MergeScheduler& operator=(const MergeScheduler&) = delete;

  size_t Wanted(const Relation& relation) const;
  std::vector<std::string> StartRound(Relation& relation);

  void Enqueue(const std::shared_ptr<Relation>& relation,
               std::vector<std::unique_ptr<MergeJob>> jobs);
  void CancelQueued(const Relation& relation);
  void StopRunning(const Relation& relation);
  Lease TakeOver(const std::shared_ptr<Relation>& relation);
  void Interrupt() noexcept;
  void Stop();

  size_t RoundSize() const noexcept { return RelationCap() + 1; }

  [[nodiscard]] bool TryAcquireThread() noexcept;
  void ReleaseThread() noexcept;

  uint64_t StartForeground(uint64_t relation_id, MergeOrigin origin,
                           MergeInputs inputs);
  void FinishForeground(uint64_t id, MergeOutcome outcome,
                        irs::MergeStats stats, std::string error);

  std::vector<MergeRecord> Active(uint64_t relation_id) const;
  std::vector<MergeRecord> History(uint64_t relation_id);

 private:
  struct Entry;

  using Queue = std::vector<std::unique_ptr<Entry>>;
  using Running = irs::containers::FlatHashMap<Entry*, std::unique_ptr<Entry>>;

  size_t BigThreads() const noexcept;
  size_t RelationCap() const noexcept;
  size_t GrantBigThreads(size_t wanted) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool Admits(const Entry& entry) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool NoneRunning() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  static bool NoneRunningFor(const Relation* relation);
  void Drop(Queue& dropped) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  template<typename Pred>
  void DropIf(Queue& dropped, Pred pred) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  template<typename Pred>
  void StopIf(Pred pred) ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void UpdateGauges() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Dispatch(std::vector<Entry*>& started, Queue& stale)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  void Launch(std::span<Entry* const> started, const Queue& stale);
  yaclib::Future<> Execute(Entry& entry);
  void Finish(Entry& entry, MergeResult result) noexcept;

  yaclib::IExecutor& _executor;
  const Options _options;
  mutable absl::Mutex _mutex;
  MergeRegistry _registry ABSL_GUARDED_BY(_mutex);
  bool _stopping ABSL_GUARDED_BY(_mutex) = false;
  size_t _threads ABSL_GUARDED_BY(_mutex) = 0;
  size_t _big ABSL_GUARDED_BY(_mutex) = 0;
  size_t _small ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _dispatched ABSL_GUARDED_BY(_mutex) = 0;
  Queue _queue ABSL_GUARDED_BY(_mutex);
  Running _running ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
