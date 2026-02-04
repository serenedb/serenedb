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
#include <absl/functional/any_invocable.h>

#include <concepts>
#include <functional>

#include "app/app_server.h"
#include "basics/assert.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

template<typename T>
concept IndexTaskType = requires(const T& task) {
  { T::ThreadGroup() } -> std::same_as<ThreadGroup>;
  { T::TaskName() } -> std::same_as<std::string_view>;
  { task.GetContinuos() } -> std::same_as<T>;  // span continuous tasks
};

class Task {
 public:
  Task(const std::shared_ptr<InvertedIndexShard>& inverted_index_shard)
    : _id{inverted_index_shard->GetId()},
      _inverted_index_shard{inverted_index_shard},
      _state{inverted_index_shard->GetState()},
      _engine{&GetSearchEngine()} {}

  template<IndexTaskType Self>
  void Schedule(this Self&& self, absl::Duration delay = {}) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "Scheduling task: ", Self::TaskName());
    self._engine->Queue(Self::ThreadGroup(), delay, std::move(self));
  }

  template<IndexTaskType Self>
  void ScheduleContinue(this const Self& self, absl::Duration delay = {}) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "Scheduling task: ", Self::TaskName());
    self._engine->Queue(Self::ThreadGroup(), delay, self.GetContinuos());
  }

 protected:
  ObjectId _id;
  std::weak_ptr<InvertedIndexShard> _inverted_index_shard;
  std::shared_ptr<ThreadPoolState> _state;
  SearchEngine* _engine;
};

class CommitTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Commit;
  }
  static constexpr std::string_view TaskName() noexcept { return "Commit"; }
  CommitTask(const std::shared_ptr<InvertedIndexShard>& inverted_index_shard)
    : Task{inverted_index_shard} {}

  CommitTask GetContinuos() const {
    auto self = _inverted_index_shard.lock();
    SDB_ASSERT(self);
    return CommitTask(self);
  }

  void operator()();
  void Finalize(
    std::shared_ptr<search::InvertedIndexShard> inverted_index_shard,
    CommitResult res);

 private:
  absl::Duration _commit_interval_msec;
  absl::Duration _consolidation_interval_msec;
  size_t _cleanup_interval_step;
  size_t _cleanup_interval_count;
};

class ConsolidationTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Consolidation;
  }
  static constexpr std::string_view TaskName() noexcept {
    return "Consolidate";
  }
  ConsolidationTask(
    const std::shared_ptr<InvertedIndexShard>& inverted_index_shard,
    std::function<bool()> flush_progress)
    : Task{inverted_index_shard}, _progress{std::move(flush_progress)} {}

  void operator()();
  ConsolidationTask GetContinuos() const {
    auto self = _inverted_index_shard.lock();
    SDB_ASSERT(self);
    return ConsolidationTask(self, _progress);
  }

 private:
  irs::MergeWriter::FlushProgress _progress;
  InvertedIndexShardMeta::ConsolidationPolicy _consolidation_policy;
  absl::Duration _consolidation_interval_msec;
};

}  // namespace sdb::search
