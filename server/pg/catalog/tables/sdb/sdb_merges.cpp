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

#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <optional>
#include <string_view>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "search/compaction/merge_registry.h"

namespace sdb::pg {
namespace {

constexpr SystemSqlColumn kSdbMergesColumns[] = {
  {"merge_id", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"relation_id", kOid, 0, false, false, 0, 0, SystemKey::None},
  {"origin", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"state", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"lane", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"score", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"queued", kTimestamptz, 0, false, false, 0, 0, SystemKey::None},
  {"started", kTimestamptz, 0, false, false, 0, 0, SystemKey::None},
  {"elapsed_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"threads", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"inputs", kTextArray, kText, false, false, kCCollation, 1, SystemKey::None},
  {"input_docs", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"input_live_docs", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"input_bytes", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"estimated_bytes", kInt8, 0, false, false, 0, 0, SystemKey::None},
};

constexpr SystemSql kSdbMergesSql{.oid = kSdbMergesTable,
                                  .schema = "pg_catalog",
                                  .name = "sdb_merges",
                                  .relkind = 'r',
                                  .replident = 'n',
                                  .columns = kSdbMergesColumns};

constexpr duckdb::CatalogType kStoreTypes[] = {
  duckdb::CatalogType::INDEX_ENTRY, duckdb::CatalogType::TABLE_ENTRY};

constexpr SystemIndex kStoreIndexes[] = {
  {kSdbMergesSql["relation_id"], SystemLookup::Object},
};

std::optional<duckdb::timestamp_tz_t> At(int64_t micros) {
  if (micros == 0) {
    return std::nullopt;
  }
  return duckdb::timestamp_tz_t{micros};
}

using search::MergeRecord;

class SdbMerges final : public SystemTableScan<kSdbMergesSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kStoreTypes, SystemSchemas::Skip, kStoreIndexes}};

  static constexpr auto kMerge = Shape<kSql, const MergeRecord>(
    Col<"merge_id">(&MergeRecord::id),
    Col<"relation_id">(&MergeRecord::relation_id),
    Col<"origin">([](const auto& r) { return irs::SnakeCaseName(r.origin); }),
    Col<"state">([](const auto& r) {
      return std::string_view{r.Queued() ? "queued" : "running"};
    }),
    Col<"lane">([](const auto& r) {
      return r.queued_us == 0
               ? std::nullopt
               : std::optional<std::string_view>{irs::SnakeCaseName(r.lane)};
    }),
    Col<"score">([](const auto& r) {
      return r.queued_us == 0 ? std::nullopt : std::optional<double>{r.score};
    }),
    Col<"queued">([](const auto& r) { return At(r.queued_us); }),
    Col<"started">([](const auto& r) { return At(r.started_us); }),
    Col<"elapsed_ms">([](const auto& r) {
      if (r.Queued()) {
        return std::optional<double>{};
      }
      return std::optional<double>{
        static_cast<double>(absl::ToUnixMicros(absl::Now()) - r.started_us) /
        1000};
    }),
    Col<"threads">([](const auto& r) {
      return r.threads == 0 ? std::nullopt : std::optional<uint64_t>{r.threads};
    }),
    Col<"inputs">([](const auto& r) -> const auto& { return r.stats.inputs; }),
    Col<"input_docs">([](const auto& r) { return r.stats.input_docs; }),
    Col<"input_live_docs">(
      [](const auto& r) { return r.stats.input_live_docs; }),
    Col<"input_bytes">([](const auto& r) { return r.stats.input_bytes; }),
    Col<"estimated_bytes">([](const auto& r) { return r.EstimatedBytes(); }));

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (!Allows<"relation_id">(index.oid) ||
        index.index_type != catalog::kInvertedIndexTypeName ||
        !index.Cast<catalog::InvertedIndexEntry>().Storage()) {
      return;
    }
    Merges(index.oid);
  }

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"relation_id">(table.oid) ||
        !dynamic_cast<const catalog::SearchTableEntry*>(&table)) {
      return;
    }
    Merges(table.oid);
  }

 private:
  void Merges(duckdb::idx_t relation) {
    for (const auto& record :
         search::MergeRegistry::Instance().Active(relation)) {
      Emit<kMerge>(record);
    }
  }
};

}  // namespace

SystemTable gSdbMerges = SystemTableOf<SdbMerges>();

}  // namespace sdb::pg
