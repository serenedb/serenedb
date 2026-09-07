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

#include "sdb_metrics.h"

#include <array>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/metrics.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/inverted_index.h"
#include "catalog/log/duckdb_global_catalog.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kPerProcessMask = MaskFromNonNulls({
  GetIndex(&SdbMetrics::metric),
  GetIndex(&SdbMetrics::value),
  GetIndex(&SdbMetrics::description),
});

constexpr uint64_t kPerIndexMask = MaskFromNonNulls({
  GetIndex(&SdbMetrics::metric),
  GetIndex(&SdbMetrics::value),
  GetIndex(&SdbMetrics::description),
  GetIndex(&SdbMetrics::relation_id),
});

using search::StoreStats;

struct IndexMetricDesc {
  std::string_view metric;
  uint64_t StoreStats::* field;
  std::string_view description;
};

constexpr std::array<IndexMetricDesc, 12> kIndexMetrics = {{
  {"num_docs", &StoreStats::numDocs,
   "documents in the index (including deleted)"},
  {"num_live_docs", &StoreStats::numLiveDocs, "live (non-deleted) documents"},
  {"num_buffered_docs", &StoreStats::numBufferedDocs,
   "documents buffered in the writer, not yet committed"},
  {"num_segments", &StoreStats::numSegments, "index segments"},
  {"num_files", &StoreStats::numFiles, "files backing the index"},
  {"index_size", &StoreStats::indexSize, "on-disk index size in bytes"},
  {"num_failed_commits", &StoreStats::numFailedCommits,
   "failed commit operations"},
  {"num_failed_cleanups", &StoreStats::numFailedCleanups,
   "failed cleanup operations"},
  {"num_failed_consolidations", &StoreStats::numFailedConsolidations,
   "failed consolidation operations"},
  {"avg_commit_time_ms", &StoreStats::avgCommitTimeMs,
   "average time of the last few commits, in ms"},
  {"avg_cleanup_time_ms", &StoreStats::avgCleanupTimeMs,
   "average time of the last few cleanups, in ms"},
  {"avg_consolidation_time_ms", &StoreStats::avgConsolidationTimeMs,
   "average time of the last few consolidations, in ms"},
}};

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<SdbMetrics>::GetTableData() {
  std::vector<SdbMetrics> values;
  std::vector<uint64_t> masks;

  for (size_t i = 0; i < metrics::kGaugeCount; ++i) {
    const auto gauge = static_cast<metrics::Gauge>(i);
    values.emplace_back(std::string_view{metrics::Name(gauge)},
                        static_cast<uint64_t>(metrics::Get(gauge)),
                        std::string_view{metrics::Description(gauge)});
    masks.emplace_back(kPerProcessMask);
  }

  const auto wal_first = values.size();
  const auto wal = catalog::ClusterCatalogWalSize();
  values.emplace_back("catalog_wal_appended_bytes", wal.appended_bytes,
                      "bytes appended to the catalog wal since start");
  values.emplace_back("catalog_wal_size_on_disk", wal.size_on_disk,
                      "current catalog wal file size in bytes");
  masks.insert(masks.end(), values.size() - wal_first, kPerProcessMask);

  const auto emit = [&](const StoreStats& stats, Oid relation_id) {
    for (const auto& desc : kIndexMetrics) {
      values.emplace_back(desc.metric, stats.*desc.field, desc.description,
                          relation_id);
      masks.emplace_back(kPerIndexMask);
    }
  };
  for (const auto* index :
       catalog::DatabaseInvertedIndexes(nullptr, GetDatabaseId())) {
    const auto& storage = index->GetInvertedData();
    if (!storage) {
      continue;
    }
    const auto stats = storage->GetStats();
    const Oid relation_id = index->Definition().GetId().id();
    emit(stats, relation_id);
  }
  catalog::ScanDatabase(
    nullptr, GetDatabaseId(), duckdb::CatalogType::TABLE_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      const auto* table = catalog::EntryOf<catalog::SereneDBTableEntry>(&entry);
      if (!table || !table->IsSearchTable() || !table->GetSearchData()) {
        return;
      }
      const auto& store = *table->GetSearchData();
      emit(store.GetStats(), store.GetTableId().id());
    });

  auto result = CreateColumns<SdbMetrics>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], masks[row], row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
