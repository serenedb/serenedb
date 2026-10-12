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

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <memory>
#include <string>
#include <string_view>

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
  uint64_t RelationId() const noexcept { return _relation_id; }
  bool IsIndex() const noexcept { return _index != nullptr; }
  uint32_t RowGroupSize() const noexcept { return _row_group_size; }

  irs::DirectoryReader Snapshot() const;
  const search::TasksSettings& Settings() const;
  search::CompactionOptions PinOptions() const;
  std::string ColumnName(irs::field_id id) const;

  MergeRun Compact(const irs::CompactionPolicy& policy,
                   const irs::MergeWriter::FlushProgress& progress,
                   const irs::IndexFieldOptions* field_options,
                   search::MergeOrigin origin,
                   irs::MergeExecutor* executor = nullptr) const;
  search::MergeScheduler::Lease TakeOver() const;
  void Publish() const;

 private:
  std::string _name;
  uint64_t _relation_id = 0;
  uint32_t _row_group_size = DEFAULT_ROW_GROUP_SIZE;
  std::shared_ptr<search::SearchTable> _table;
  std::shared_ptr<search::InvertedIndexStorage> _index;
  absl::flat_hash_map<irs::field_id, std::string> _column_names;
};

}  // namespace sdb::connector
