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

#include "catalog/entry/inverted_index.h"
#include "search/compaction/merge_policies.h"
#include "search/compaction/merge_registry.h"
#include "search/compaction/merge_scheduler.h"
#include "search/maintenance.h"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::search {

class InvertedIndexStorage;
class SearchTable;

}  // namespace sdb::search
namespace sdb::connector {

struct MergeRun {
  irs::CompactionResult result{};
  std::string error{};
};

class MergeTarget {
 public:
  static MergeTarget Resolve(duckdb::ClientContext& context,
                             std::string_view relation);

  const std::string& Name() const noexcept { return _name; }
  const std::string& Database() const noexcept { return _database; }
  bool ReadOnly() const noexcept { return _read_only; }
  uint64_t RelationId() const noexcept { return _relation_id; }
  bool IsIndex() const noexcept { return _index != nullptr; }
  bool Sorted() const noexcept;
  uint32_t RowGroupSize() const noexcept { return _options->row_group_size; }
  const irs::IndexFieldOptions& FieldOptions() const noexcept {
    return *_options;
  }

  irs::DirectoryReader Snapshot() const;
  const search::TasksSettings& Settings() const;
  search::CompactionOptions PinOptions() const;
  std::string ColumnName(irs::field_id id) const;

  MergeRun Compact(const irs::CompactionPolicy& policy,
                   const irs::MergeProgress& progress,
                   const irs::IndexFieldOptions* field_options,
                   search::MergeOrigin origin,
                   irs::MergeExecutor* executor = nullptr,
                   irs::LoneSegment lone = irs::LoneSegment::Skip) const;
  search::MergeScheduler::Lease TakeOver() const;
  void Publish() const;
  void Refresh() const;
  irs::RemovedFiles Cleanup() const;

 private:
  void SetDatabase(const duckdb::InCatalogEntry& entry);

  std::string _name;
  std::string _database;
  bool _read_only = false;
  uint64_t _relation_id = 0;
  std::shared_ptr<const catalog::InvertedIndexConfig> _options;
  std::shared_ptr<search::SearchTable> _table;
  std::shared_ptr<search::InvertedIndexStorage> _index;
  irs::containers::FlatHashMap<irs::field_id, std::string> _column_names;
};

}  // namespace sdb::connector
