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

#include "app/app_server.h"
#include "search/data_store.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

template<typename T>
concept IndexTaskType = requires {
  { T::ThreadGroup() } -> std::same_as<ThreadGroup>;
  { T::TaskName() } -> std::same_as<std::string_view>;
};

class Task {
 public:
  Task(ObjectId id, std::shared_ptr<ThreadPoolState> state)
    : _id{id},
      _state{std::move(state)},
      _engine{&SerenedServer::Instance().getFeature<SearchEngine>()} {}

  template<IndexTaskType Self>
  void Schedule(this Self&& self, absl::Duration delay = {}) {
    self._engine->Queue(Self::ThreadGroup(), delay, std::move(self));
  }

 protected:
  ObjectId _id;
  std::shared_ptr<ThreadPoolState> _state;
  [[maybe_unused]] SearchEngine* _engine;
};

class CommitTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Commit;
  }
  static constexpr std::string_view TaskName() noexcept { return "Commit"; }
  CommitTask(DataStore::Transaction&& transaction)
    : Task{transaction.GetDataStore()->GetId(),
           transaction.GetDataStore()->GetState()},
      _transaction{std::move(transaction)} {}

  void operator()();
  void Finalize(CommitResult res);

 private:
  DataStore::Transaction _transaction;
  absl::Duration _commit_interval_msec;
  absl::Duration _consolidation_interval_msec;
};

class ConsolidationTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Consolidation;
  }
  static constexpr std::string_view TaskName() noexcept {
    return "Consolidate";
  }
  ConsolidationTask(ObjectId id, std::shared_ptr<DataStore> data_store,
                    std::shared_ptr<ThreadPoolState> state,
                    std::function<bool()>&& flush_progress)
    : Task{id, std::move(state)},
      _data_store{std::move(data_store)},
      _progress{std::move(flush_progress)} {}
  void operator()();

 private:
  std::shared_ptr<DataStore> _data_store;
  irs::MergeWriter::FlushProgress _progress;
  DataStoreMeta::ConsolidationPolicy _consolidation_policy;
  absl::Duration _consolidation_interval_msec;
};

}  // namespace sdb::search
