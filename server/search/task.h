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
#include <absl/time/time.h>

#include <concepts>
#include <functional>
#include <utility>
#include <yaclib/async/contract.hpp>
#include <yaclib/async/future.hpp>
#include <yaclib/async/promise.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

template<typename T>
concept IndexTaskType = requires(
  const T& task, std::shared_ptr<InvertedIndexShard>&& inverted_index_shard) {
  { T::ThreadGroup() } -> std::same_as<ThreadGroup>;
  { T::TaskName() } -> std::same_as<std::string_view>;
  {
    task.GetContinuos(std::move(inverted_index_shard))
  } -> std::same_as<T>;  // span continuous tasks
};

class Task {
 public:
  Task(const std::shared_ptr<InvertedIndexShard>& inverted_index_shard)
    : _id{inverted_index_shard->GetId()},
      _inverted_index_shard{inverted_index_shard},
      _state{inverted_index_shard->GetState()},
      _engine{&GetSearchEngine()} {}

  template<IndexTaskType Self>
  yaclib::Future<> Schedule(this Self&& self, absl::Duration delay = {}) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "Scheduling task: ", Self::TaskName());
    auto [future, promise] = yaclib::MakeContract();
    self._promise = std::move(promise);
    self._engine->Queue(Self::ThreadGroup(), delay, std::move(self));
    return std::move(future);
  }

  template<IndexTaskType Self>
  void ScheduleContinue(
    this const Self& self,
    std::shared_ptr<InvertedIndexShard>&& inverted_index_shard,
    absl::Duration delay = {}) {
    SDB_TRACE("xxxxx", Logger::SEARCH, "Scheduling task: ", Self::TaskName());
    self.GetContinuos(std::move(inverted_index_shard)).Schedule(delay).Detach();
  }

 protected:
  ObjectId _id;
  std::weak_ptr<InvertedIndexShard> _inverted_index_shard;
  std::shared_ptr<ThreadPoolState> _state;
  yaclib::Promise<> _promise;
  SearchEngine* _engine;
};

class RefreshTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Refresh;
  }
  static constexpr std::string_view TaskName() noexcept { return "Refresh"; }
  RefreshTask(const std::shared_ptr<InvertedIndexShard>& inverted_index_shard,
              bool wait)
    : Task{inverted_index_shard}, _wait{wait} {}

  RefreshTask GetContinuos(
    std::shared_ptr<InvertedIndexShard>&& inverted_index_shard) const {
    // continue is always no-wait as we rescheduling next background commit
    SDB_ASSERT(inverted_index_shard);
    return RefreshTask(inverted_index_shard, false);
  }

  void operator()();
  void Finalize(
    std::shared_ptr<search::InvertedIndexShard> inverted_index_shard,
    CommitResult res);

 private:
  absl::Duration _refresh_interval_msec{};
  absl::Duration _compaction_interval_msec{};
  size_t _cleanup_interval_step{0};
  size_t _cleanup_interval_count{0};
  bool _wait;
};

class CompactionTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Compaction;
  }
  static constexpr std::string_view TaskName() noexcept { return "Compact"; }
  CompactionTask(
    const std::shared_ptr<InvertedIndexShard>& inverted_index_shard,
    std::function<bool()> flush_progress)
    : Task{inverted_index_shard}, _progress{std::move(flush_progress)} {}

  void operator()();
  CompactionTask GetContinuos(
    std::shared_ptr<InvertedIndexShard>&& inverted_index_shard) const {
    SDB_ASSERT(inverted_index_shard);
    return CompactionTask(inverted_index_shard, _progress);
  }

 private:
  irs::MergeWriter::FlushProgress _progress;
  irs::CompactionPolicy _compaction_policy;
  absl::Duration _compaction_interval_msec;
};

}  // namespace sdb::search
