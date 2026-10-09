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

#include <absl/algorithm/container.h>
#include <absl/flags/commandlineflag.h>
#include <absl/flags/reflection.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <memory>
#include <ranges>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/progress_registry.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "server/utils/metrics.h"

namespace sdb::pg {
namespace {

std::string_view VarType(const absl::CommandLineFlag& flag) {
  if (flag.IsOfType<bool>()) {
    return "bool";
  }
  if (flag.IsOfType<int32_t>() || flag.IsOfType<int64_t>() ||
      flag.IsOfType<uint32_t>() || flag.IsOfType<uint64_t>()) {
    return "integer";
  }
  if (flag.IsOfType<float>() || flag.IsOfType<double>()) {
    return "real";
  }
  return "string";
}

constexpr std::string_view kSecretFlags[] = {"auth_password", "auth_api_key",
                                             "auth_bearer_token"};

SystemRows<const absl::CommandLineFlag*> LoadFlags(SystemScan&) {
  auto flags = std::views::values(absl::GetAllFlags()) |
               std::ranges::to<std::vector<const absl::CommandLineFlag*>>();
  absl::c_sort(flags, [](const absl::CommandLineFlag* lhs,
                         const absl::CommandLineFlag* rhs) {
    return lhs->Name() < rhs->Name();
  });
  return flags;
}

struct Flag {
  const absl::CommandLineFlag& flag;
  mutable std::optional<std::string> current;
  mutable std::optional<std::string> boot;

  const std::string& Current() const {
    if (!current) {
      current = flag.CurrentValue();
    }
    return *current;
  }

  const std::string& Boot() const {
    if (!boot) {
      boot = flag.DefaultValue();
    }
    return *boot;
  }
};

class SdbSettings final : public SystemTableScan<kSdbSettingsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<const absl::CommandLineFlag*>{&LoadFlags, {}}};

  static constexpr auto kFlag = Shape<kSql, const Flag>(
    Col<"name">(
      [](const auto& row) { return std::string_view{row.flag.Name()}; }),
    Col<"setting">([](const auto& row) -> std::string_view {
      if (absl::c_linear_search(kSecretFlags, row.flag.Name()) &&
          !row.Current().empty()) {
        return "***";
      }
      return row.Current();
    }),
    Col<"short_desc">([](const auto& row) { return row.flag.Help(); }),
    Col<"context">([](const auto&) { return std::string_view{"postmaster"}; }),
    Col<"vartype">([](const auto& row) { return VarType(row.flag); }),
    Col<"source">([](const auto& row) {
      return std::string_view{row.Current() == row.Boot() ? "default"
                                                          : "command line"};
    }),
    Col<"boot_val">([](const auto& row) -> const auto& { return row.Boot(); }),
    Col<"reset_val">([](const auto& row) -> const auto& { return row.Boot(); }),
    Col<"pending_restart">([](const auto&) { return false; }));

  void Row(const absl::CommandLineFlag* flag) { Emit<kFlag>({*flag, {}, {}}); }
};

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

SystemRows<ProgressSnapshot> LoadSnapshots(SystemScan&) {
  return ProgressRegistry::Instance().GetSnapshots();
}

constexpr std::tuple kProgressBase{Col<"pid">(&ProgressSnapshot::pid),
                                   Col<"datid">(&ProgressSnapshot::datid),
                                   Col<"usename">(&ProgressSnapshot::user),
                                   Col<"datname">(&ProgressSnapshot::database)};

constexpr std::tuple kProgressSession{
  Col<"state">([](const auto& s) {
    return std::string_view{s.query_start_us != 0 ? "active" : "idle"};
  }),
  Col<"query">(&ProgressSnapshot::query),
  Col<"backend_start_us">(&ProgressSnapshot::backend_start_us)};

constexpr std::tuple kProgressActive{
  Col<"query_start_us">(&ProgressSnapshot::query_start_us),
  Col<"rows_processed">(&ProgressSnapshot::rows_processed),
  Col<"rows_total">(&ProgressSnapshot::rows_total),
  Col<"tuples_processed">(&ProgressSnapshot::tuples_processed),
  Col<"bytes_processed">(&ProgressSnapshot::bytes_processed),
  Col<"percent">([](const auto& s) -> std::optional<double> {
    if (s.percent < 0) {
      return std::nullopt;
    }
    return s.percent;
  })};

constexpr std::tuple kProgressCommand{
  Col<"command">([](const auto& s) {
    return ProgressCommandName(static_cast<ProgressCommand>(s.command));
  }),
  Col<"io_type">([](const auto& s) {
    return ProgressIoTypeName(static_cast<ProgressIoType>(s.io_type));
  }),
  Col<"relid">(&ProgressSnapshot::relid),
  Col<"current_relid">(&ProgressSnapshot::current_relid),
  Col<"phase">([](const auto& s) {
    return ProgressPhaseName(static_cast<ProgressCommand>(s.command), s.phase);
  }),
  Col<"bytes_total">(&ProgressSnapshot::bytes_total),
  Col<"tuples_total">(&ProgressSnapshot::tuples_total),
  Col<"stage">(&ProgressSnapshot::stage),
  Col<"stages_total">(&ProgressSnapshot::stages_total),
  Col<"step">(&ProgressSnapshot::step),
  Col<"steps_total">(&ProgressSnapshot::steps_total),
  Col<"items_processed">(&ProgressSnapshot::items_processed),
  Col<"items_total">(&ProgressSnapshot::items_total)};

class SdbProgress final : public SystemTableScan<kSdbProgressSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<ProgressSnapshot>{&LoadSnapshots, {}}};

  static constexpr auto kHidden = Shape<kSql, const ProgressSnapshot>(
    kProgressBase, Col<"query">([](const auto&) {
      return std::string_view{"<insufficient privilege>"};
    }));

  static constexpr auto kIdle =
    Shape<kSql, const ProgressSnapshot>(kProgressBase, kProgressSession);

  static constexpr auto kActive = Shape<kSql, const ProgressSnapshot>(
    kProgressBase, kProgressSession, kProgressActive);

  static constexpr auto kCommand = Shape<kSql, const ProgressSnapshot>(
    kProgressBase, kProgressSession, kProgressActive, kProgressCommand);

  void Row(const ProgressSnapshot& s) {
    if (_closure && !_closure->is_superuser) {
      const auto* role = _roles->FindByName(s.user);
      if (!role || !_closure->MemberOf(role->first)) {
        Emit<kHidden>(s);
        return;
      }
    }
    if (s.query_start_us == 0) {
      Emit<kIdle>(s);
    } else if (static_cast<ProgressCommand>(s.command) ==
               ProgressCommand::None) {
      Emit<kActive>(s);
    } else {
      Emit<kCommand>(s);
    }
  }

 private:
  std::shared_ptr<const auth::RoleClosure> _closure = SessionClosure();
  std::shared_ptr<const auth::RoleGraph> _roles =
    _closure && !_closure->is_superuser ? auth::RolesOf(&Context()) : nullptr;
};

}  // namespace

SystemTable gSdbSettings = SystemTableOf<SdbSettings>();

SystemTable gSdbMetrics = SystemTableOf<SdbMetrics>();

SystemTable gSdbProgress = SystemTableOf<SdbProgress>();

}  // namespace sdb::pg
