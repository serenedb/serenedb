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
  Task(IndexId id, std::shared_ptr<DataStore> data_store,
       std::shared_ptr<ThreadPoolState> state)
    : _id{id},
      _data_store{std::move(data_store)},
      _state{std::move(state)},
      _engine{&SerenedServer::Instance().getFeature<SearchEngine>()} {}

  template<IndexTaskType Self>
  void Schedule(this const Self& self, absl::Duration delay) {
    self._engine->queue(Self::ThreadGroup(), delay, self);
  }

 protected:
  IndexId _id;
  std::shared_ptr<DataStore> _data_store;
  std::shared_ptr<ThreadPoolState> _state;
  [[maybe_unused]] SearchEngine* _engine;
};

class CommitTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Commit;
  }
  static constexpr std::string_view TaskName() noexcept { return "Commit"; }
  CommitTask(IndexId id, std::shared_ptr<DataStore> data_store,
             std::shared_ptr<ThreadPoolState> state)
    : Task{id, std::move(data_store), std::move(state)} {}

  void operator()();
  void Finalize(DataStore& data_store, CommitResult& commit_res);

 private:
  size_t _cleanup_interval_count;
  absl::Duration _commit_interval_msec;
  absl::Duration _consolidation_interval_msec;
  size_t _cleanup_interval_step;
};

class ConsolidationTask : public Task {
 public:
  static constexpr ThreadGroup ThreadGroup() noexcept {
    return ThreadGroup::Consolidation;
  }
  static constexpr std::string_view TaskName() noexcept {
    return "Consolidate";
  }
  ConsolidationTask(IndexId id, std::shared_ptr<DataStore> data_store,
                    std::shared_ptr<ThreadPoolState> state,
                    std::function<bool()>&& flush_progress)
    : Task{id, std::move(data_store), std::move(state)},
      _progress{std::move(flush_progress)} {}
  void operator()();

 private:
  irs::MergeWriter::FlushProgress _progress;
  DataStoreMeta::ConsolidationPolicy _consolidation_policy;
  absl::Duration _consolidation_interval_msec;
};

}  // namespace sdb::search
