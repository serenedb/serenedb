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

#include <cstdint>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "search/compaction/copy_check.h"
#include "search/compaction/merge_planner.h"
#include "search/compaction/merge_policies.h"
#include "search/compaction/merge_registry.h"
#include "search/compaction/merge_scheduler.h"
#include "search/maintenance.h"
#include "search/store_stats.h"
#include "storage_engine/search_engine.h"

namespace duckdb {

class ClientContext;
class TaskScheduler;

}  // namespace duckdb
namespace sdb::search {

class InvertedIndexStorage;
class SearchTable;

}  // namespace sdb::search
namespace sdb::connector {

struct ExecutedMerge {
  search::MergeOutcome outcome = search::MergeOutcome::Running;
  irs::MergeStats stats;
  std::string error;
};

struct MergeRequest {
  search::MergeMode mode = search::MergeMode::Compact;
  uint32_t target_segments = 1;
  bool exact_target = false;
  std::vector<std::string> segments;
};

class MergeTarget {
 public:
  static MergeTarget Resolve(duckdb::ClientContext& context,
                             std::string_view relation);
  static MergeTarget Of(std::shared_ptr<search::SearchTable> table,
                        std::string name);
  static MergeTarget Of(
    std::shared_ptr<search::InvertedIndexStorage> index,
    std::shared_ptr<const catalog::InvertedIndexConfig> options,
    std::string name);

  const std::string& Name() const noexcept { return _name; }
  const std::string& Database() const noexcept { return _database; }
  bool ReadOnly() const noexcept { return _read_only; }
  bool Sorted() const noexcept { return _store->Sorted(); }
  const irs::IndexFieldOptions& FieldOptions() const noexcept {
    return *_options;
  }

  irs::DirectoryReader Snapshot() const;
  search::CompactionOptions PinOptions() const;
  std::string ColumnName(irs::field_id id) const;

  search::MergeRoundPlan Plan(const irs::IndexReader& snapshot,
                              const MergeRequest& request,
                              const search::CopyChecker& checker) const;
  ExecutedMerge Compact(const search::PlannedMerge& planned,
                        const irs::MergeProgress& progress,
                        const irs::IndexFieldOptions* field_options,
                        const irs::AnnBuildEnv* env, search::MergeOrigin origin,
                        irs::MergeExecutor& executor) const;
  search::MergeScheduler::Lease TakeOver() const {
    return search::GetSearchEngine().Merges().TakeOver(_store->MergeRelation());
  }
  void Publish() const;

 private:
  void SetDatabase(const duckdb::InCatalogEntry& entry);
  void SetStore();

  std::string _name;
  std::string _database;
  bool _read_only = false;
  std::shared_ptr<const catalog::InvertedIndexConfig> _options;
  std::shared_ptr<search::SearchTable> _table;
  std::shared_ptr<search::InvertedIndexStorage> _index;
  search::MaintainedStore* _store = nullptr;
  irs::containers::FlatHashMap<irs::field_id, std::string> _column_names;
};

class ManualCompaction {
 public:
  ManualCompaction(const MergeTarget& target, MergeRequest request,
                   duckdb::TaskScheduler& scheduler);

  bool Next(const irs::MergeProgress& progress, search::MergeOrigin origin,
            irs::MergeExecutor& executor, ExecutedMerge& merge);

 private:
  void Plan();

  const MergeTarget& _target;
  search::CompactionOptions _options;
  search::CopyChecker _checker;
  MergeRequest _request;
  search::MergeRoundPlan _plan;
  size_t _next = 0;
  search::MergeScheduler::Lease _lease;
  search::ManualCompactionSlot _slot;
};

}  // namespace sdb::connector
