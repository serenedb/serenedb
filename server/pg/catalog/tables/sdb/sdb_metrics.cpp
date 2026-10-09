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

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "search/store_stats.h"
#include "server/utils/metrics.h"

namespace sdb::pg {
namespace {

using search::StoreStats;

struct IndexMetric {
  std::string_view metric;
  uint64_t StoreStats::* field;
  std::string_view description;
};

constexpr IndexMetric kIndexMetrics[] = {
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
};

struct Metric {
  std::string_view metric;
  int64_t value;
  std::string_view description;
};

SystemRows<Metric> LoadMetrics(SystemScan& scan) {
  std::vector<Metric> rows;
  for (size_t i = 0; i < metrics::kGaugeCount; ++i) {
    const auto gauge = static_cast<metrics::Gauge>(i);
    rows.emplace_back(metrics::Name(gauge), metrics::Get(gauge),
                      metrics::Description(gauge));
  }
  auto& storage = scan.Database().GetAttached().GetStorageManager();
  const auto wal = storage.GetWAL();
  rows.emplace_back("catalog_wal_appended_bytes",
                    wal ? static_cast<int64_t>(wal->GetTotalWritten()) : 0,
                    "bytes appended to the catalog wal since start");
  rows.emplace_back("catalog_wal_size_on_disk",
                    wal ? static_cast<int64_t>(storage.GetWALSize()) : 0,
                    "current catalog wal file size in bytes");
  return rows;
}

constexpr duckdb::CatalogType kStoreTypes[] = {
  duckdb::CatalogType::INDEX_ENTRY, duckdb::CatalogType::TABLE_ENTRY};

constexpr SystemIndex kStoreIndexes[] = {
  {kSdbMetricsSql["relation_id"], SystemLookup::Object},
};

struct StoreMetric {
  const IndexMetric& metric;
  const StoreStats& stats;
  duckdb::idx_t relation;
};

class SdbMetrics final : public SystemTableScan<kSdbMetricsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<Metric>{&LoadMetrics, {}},
    CatalogSource{kStoreTypes, SystemSchemas::Skip, kStoreIndexes}};

  static constexpr auto kMetric = Shape<kSql, const Metric>(
    Col<"metric">(&Metric::metric), Col<"value">(&Metric::value),
    Col<"description">(&Metric::description));

  static constexpr auto kStoreMetric = Shape<kSql, const StoreMetric>(
    Col<"metric">([](const auto& row) { return row.metric.metric; }),
    Col<"value">([](const auto& row) { return row.stats.*row.metric.field; }),
    Col<"description">([](const auto& row) { return row.metric.description; }),
    Col<"relation_id">(&StoreMetric::relation));

  void Row(const Metric& metric) { Emit<kMetric>(metric); }

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (const auto* inverted =
          dynamic_cast<const catalog::InvertedIndexEntry*>(&index);
        inverted && inverted->Storage()) {
      Store(inverted->Storage()->GetStats(), index.oid);
    }
  }

  void Row(const duckdb::TableCatalogEntry& table) {
    if (const auto* search =
          dynamic_cast<const catalog::SearchTableEntry*>(&table)) {
      Store(search->Storage()->GetStats(), table.oid);
    }
  }

 private:
  void Store(const StoreStats& stats, duckdb::idx_t relation) {
    for (const auto& metric : kIndexMetrics) {
      Emit<kStoreMetric>({metric, stats, relation});
    }
  }
};

}  // namespace

SystemTable gSdbMetrics = SystemTableOf<SdbMetrics>();

}  // namespace sdb::pg
