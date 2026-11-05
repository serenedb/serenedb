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
#include <duckdb/main/client_context.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "search/compaction/merge_planner.h"
#include "search/compaction/merge_registry.h"
#include "search/compaction/merge_runner.h"
#include "search/maintenance.h"
#include "search/store.h"
#include "storage_engine/search_engine.h"
#include "utils/drain_gate.h"

namespace sdb::connector {

inline bool StatementRunning(const duckdb::ClientContext& client) noexcept {
  return !client.IsInterrupted() && !client.DeadlinePassed();
}

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
  static MergeTarget Of(std::shared_ptr<search::Store> table, std::string name);
  static MergeTarget Of(
    std::shared_ptr<search::Store> index,
    std::shared_ptr<const catalog::InvertedIndexConfig> options,
    std::string name);

  const std::string& Name() const noexcept { return _name; }
  const std::string& Database() const noexcept { return _database; }
  bool ReadOnly() const noexcept { return _read_only; }
  const irs::IndexFieldOptions& FieldOptions() const noexcept {
    return *_options;
  }

  irs::DirectoryReader Snapshot() const;
  std::shared_ptr<const irs::IndexFieldOptions> PinOptions() const;
  std::string ColumnName(irs::field_id id) const;

  search::MergeRoundPlan Plan(const irs::IndexReader& snapshot,
                              const MergeRequest& request,
                              const irs::IndexFieldOptions& field_options,
                              duckdb::TaskScheduler& scheduler) const;
  void Compact(const search::PlannedMerge& planned,
               const irs::MergeProgress& progress,
               const irs::IndexFieldOptions* field_options,
               const irs::AnnBuildEnv* env, search::MergeOrigin origin,
               utils::DrainGate::Pass maintenance,
               search::MergeResult& merged) const;
  search::MergeRunner::Lease TakeOver() const {
    return search::GetSearchEngine().Merges().TakeOver(_store->MergeRelation());
  }
  void Publish() const;

 private:
  void SetDatabase(const duckdb::InCatalogEntry& entry);

  std::string _name;
  std::string _database;
  bool _read_only = false;
  std::shared_ptr<const catalog::InvertedIndexConfig> _options;
  std::shared_ptr<search::Store> _store;
  irs::containers::FlatHashMap<irs::field_id, std::string> _column_names;
};

class ManualCompaction {
 public:
  ManualCompaction(const MergeTarget& target, MergeRequest request,
                   duckdb::ClientContext& context);

  bool Next(const irs::MergeProgress& progress, search::MergeOrigin origin,
            search::MergeResult& merge);

 private:
  void Plan();

  const MergeTarget& _target;
  std::shared_ptr<const irs::IndexFieldOptions> _options;
  duckdb::ClientContext& _context;
  MergeRequest _request;
  search::MergeRoundPlan _plan;
  size_t _next = 0;
  search::MergeRunner::Lease _lease;
  std::optional<search::ManualCompactionSlot> _slot;
};

}  // namespace sdb::connector
