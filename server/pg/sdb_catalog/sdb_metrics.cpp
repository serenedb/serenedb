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
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/metrics.h"
#include "catalog1/catalog.h"
#include "catalog1/entry/inverted_index.h"
#include "connector/inverted_store_index.h"
#include "search/inverted_index_storage.h"

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

using Stats = search::InvertedIndexStorage::Stats;

struct IndexMetricDesc {
  std::string_view metric;
  uint64_t Stats::* field;
  std::string_view description;
};

constexpr std::array<IndexMetricDesc, 12> kIndexMetrics = {{
  {"num_docs", &Stats::numDocs, "documents in the index (including deleted)"},
  {"num_live_docs", &Stats::numLiveDocs, "live (non-deleted) documents"},
  {"num_buffered_docs", &Stats::numBufferedDocs,
   "documents buffered in the writer, not yet committed"},
  {"num_segments", &Stats::numSegments, "index segments"},
  {"num_files", &Stats::numFiles, "files backing the index"},
  {"index_size", &Stats::indexSize, "on-disk index size in bytes"},
  {"num_failed_commits", &Stats::numFailedCommits, "failed commit operations"},
  {"num_failed_cleanups", &Stats::numFailedCleanups,
   "failed cleanup operations"},
  {"num_failed_consolidations", &Stats::numFailedConsolidations,
   "failed consolidation operations"},
  {"avg_commit_time_ms", &Stats::avgCommitTimeMs,
   "average time of the last few commits, in ms"},
  {"avg_cleanup_time_ms", &Stats::avgCleanupTimeMs,
   "average time of the last few cleanups, in ms"},
  {"avg_consolidation_time_ms", &Stats::avgConsolidationTimeMs,
   "average time of the last few consolidations, in ms"},
}};

}  // namespace

template<>
MaterializedData SystemTableSnapshot<SdbMetrics>::GetTableData() {
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
  auto& storage_manager =
    duckdb::Catalog::GetCatalog(_context, duckdb::Identifier::InvalidCatalog())
      .GetAttached()
      .GetStorageManager();
  auto wal = storage_manager.GetWAL();
  values.emplace_back("catalog_wal_appended_bytes",
                      wal ? wal->GetTotalWritten() : 0,
                      "bytes appended to the catalog wal since start");
  values.emplace_back("catalog_wal_size_on_disk",
                      wal ? storage_manager.GetWALSize() : 0,
                      "current catalog wal file size in bytes");
  masks.insert(masks.end(), values.size() - wal_first, kPerProcessMask);

  auto& context = _context;
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, duckdb::Identifier::InvalidCatalog());
  const auto visit_index = [&](duckdb::CatalogEntry& entry) {
    const auto* index =
      dynamic_cast<const catalog::InvertedIndexEntry*>(&entry);
    if (!index || !index->Storage()) {
      return;
    }
    const auto stats = index->Storage()->GetStats();
    const auto relation_id = static_cast<Oid>(index->oid);
    for (const auto& desc : kIndexMetrics) {
      values.emplace_back(desc.metric, stats.*desc.field, desc.description,
                          relation_id);
      masks.emplace_back(kPerIndexMask);
    }
  };
  VisitSchemas(context, catalog, [&](duckdb::SchemaCatalogEntry& schema) {
    schema.Scan(context, duckdb::CatalogType::INDEX_ENTRY, visit_index);
  });

  auto result = CreateColumns<SdbMetrics>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], masks[row], row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
