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
#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iresearch/index/compaction/merge_ticket.hpp>
#include <memory>
#include <string>
#include <vector>
#include <yaclib/async/future.hpp>

#include "search/compaction/merge_registry.h"

namespace yaclib {

class IExecutor;

}  // namespace yaclib
namespace sdb::search {

enum class MergeLane : uint8_t {
  Small = 0,
  Big,
};

struct MergeEstimate {
  uint64_t bytes = 0;
  uint64_t rewrite_bytes = 0;
  size_t segments = 0;
  double score = 0;
  MergeLane lane = MergeLane::Big;
};

MergeEstimate EstimateMerge(irs::CompactionView candidates,
                            uint64_t small_bytes) noexcept;

class MergeJob {
 public:
  virtual ~MergeJob() = default;

  virtual irs::CompactionView Candidates() const noexcept = 0;

  virtual yaclib::Future<MergeOutcome> Run(const std::atomic_bool& stop,
                                           size_t helpers) = 0;
};

class MergeScheduler {
 public:
  struct Options {
    size_t threads = 1;
    size_t helpers_per_merge = 0;
    uint64_t small_bytes = uint64_t{64} << 20;
    absl::Duration aging = absl::Seconds(30);
    absl::Duration backoff = absl::Seconds(1);
    absl::Duration max_backoff = absl::Minutes(5);
    std::function<absl::Time()> now;
  };

  class Relation {
   public:
    Relation(uint64_t id, std::function<void()> nudge) noexcept
      : _id{id}, _nudge{std::move(nudge)} {}

    uint64_t Id() const noexcept { return _id; }

   private:
    friend class MergeScheduler;

    uint64_t _id;
    std::function<void()> _nudge;
    size_t _queued = 0;
    size_t _running = 0;
    size_t _running_big = 0;
    uint32_t _failures = 0;
    absl::flat_hash_map<std::string, absl::Time> _blocked;
  };

  struct QueuedMerge {
    uint64_t id = 0;
    uint64_t relation = 0;
    MergeEstimate estimate;
    std::vector<std::string> inputs;
    absl::Time queued;
    bool running = false;
    size_t helpers = 0;
  };

  MergeScheduler(yaclib::IExecutor& executor, Options options);
  ~MergeScheduler();

  MergeScheduler(const MergeScheduler&) = delete;
  MergeScheduler& operator=(const MergeScheduler&) = delete;

  std::shared_ptr<Relation> AddRelation(uint64_t id,
                                        std::function<void()> nudge);

  size_t Wanted(const Relation& relation) const;
  std::vector<std::string> Blocked(Relation& relation) const;
  size_t FreeThreads() const;

  void Enqueue(const std::shared_ptr<Relation>& relation,
               std::unique_ptr<MergeJob> job);
  void CancelQueued(const Relation& relation);
  void StopRunning(const Relation& relation);
  void Interrupt() noexcept;
  void Stop();

  size_t RoundSize() const noexcept { return RelationCap() + 1; }

  bool TryAcquireHelper() noexcept;
  void ReleaseHelper() noexcept;

  std::vector<QueuedMerge> Snapshot() const;

 private:
  struct Entry;
  class Start;

  using Queue = std::vector<std::unique_ptr<Entry>>;
  using Running = absl::flat_hash_map<Entry*, std::unique_ptr<Entry>>;

  size_t BigThreads() const noexcept;
  size_t RelationCap() const noexcept;
  bool Admits(const Entry& entry) const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  bool NoneRunning() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);
  absl::Time Now() const;
  void Pump();
  yaclib::Future<> Execute(Entry& entry);
  void Finish(Entry& entry, MergeOutcome outcome) noexcept;

  yaclib::IExecutor& _executor;
  const Options _options;
  mutable absl::Mutex _mutex;
  bool _stopping ABSL_GUARDED_BY(_mutex) = false;
  uint64_t _next_id ABSL_GUARDED_BY(_mutex) = 1;
  size_t _threads ABSL_GUARDED_BY(_mutex) = 0;
  size_t _big ABSL_GUARDED_BY(_mutex) = 0;
  size_t _small ABSL_GUARDED_BY(_mutex) = 0;
  Queue _queue ABSL_GUARDED_BY(_mutex);
  Running _running ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::search
