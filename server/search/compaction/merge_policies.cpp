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

#include "search/compaction/merge_policies.h"

#include <algorithm>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <utility>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::search {

// Background compaction policy (the TieredMergePolicy analog), built per
// target from its WITH-configured settings. `small` reduces the merge byte
// budget when the global slot gate is nearly full so the pool keeps draining
// under occupancy backpressure.
irs::CompactionPolicy MakeTierPolicy(const TasksSettings& settings,
                                     bool small) {
  irs::index_utils::CompactionTier tier;
  tier.max_segments = settings.compaction_max_segments;
  tier.max_segments_bytes = settings.compaction_max_segments_bytes;
  tier.floor_segment_bytes = settings.compaction_floor_segment_bytes;
  if (small) {
    tier.max_segments_bytes =
      std::max(2 * tier.floor_segment_bytes, tier.max_segments_bytes / 2);
  }
  return irs::index_utils::MakePolicy(tier);
}

CompactionOptions PinCompactionOptions(InvertedIndexStorage& idx) {
  // Cloned for this merge: the merge encodes against what it took for its
  // whole length, and an ALTER writes a new configuration rather than mutating
  // this one, while a DROP destroys the bound index but not this clone.
  auto& db = irs::DuckDBEngine::Instance().instance();
  for (auto& attached : duckdb::DatabaseManager::Get(db).GetDatabases()) {
    if (attached->oid != idx.GetDatabaseId()) {
      continue;
    }
    const auto entry =
      attached->GetCatalog()
        .Cast<catalog::SereneDBCatalog>()
        .FindIn<catalog::InvertedIndexEntry>(nullptr, idx.GetId());
    if (!entry) {
      break;
    }
    std::shared_ptr<const irs::IndexFieldOptions> options = entry->Config();
    const auto* raw = options.get();
    return {
      .alive = true, .keepalive = std::move(options), .field_options = raw};
  }
  return {};
}

CompactionOptions PinCompactionOptions(SearchTable& table) {
  std::shared_ptr<const irs::IndexFieldOptions> options = table.Config();
  const auto* raw = options.get();
  return {.alive = true, .keepalive = std::move(options), .field_options = raw};
}

}  // namespace sdb::search
