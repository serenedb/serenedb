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
#include <iresearch/index/merge/merge_profile.hpp>
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

constexpr SystemSqlColumn kSdbCompactHistoryColumns[] = {
  {"id", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"relation_id", kOid, 0, false, false, 0, 0, SystemKey::None},
  {"type", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"origin", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"outcome", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"started", kTimestamptz, 0, false, false, 0, 0, SystemKey::None},
  {"finished", kTimestamptz, 0, false, false, 0, 0, SystemKey::None},
  {"inputs", kTextArray, kText, false, false, kCCollation, 1, SystemKey::None},
  {"input_docs", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"input_live_docs", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"input_bytes", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"output", kText, 0, false, false, kCCollation, 0, SystemKey::None},
  {"output_docs", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"output_live_docs", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"output_bytes", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"rowid_min", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"rowid_max", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"total_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"prepare_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"norms_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"columns_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"columns_commit_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"ann_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"postings_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"commit_ms", kFloat8, 0, false, false, 0, 0, SystemKey::None},
  {"error", kText, 0, false, false, kCCollation, 0, SystemKey::None},
};

constexpr SystemSql kSdbCompactHistorySql{.oid = kSdbCompactHistoryTable,
                                          .schema = "pg_catalog",
                                          .name = "sdb_compact_history",
                                          .relkind = 'r',
                                          .replident = 'n',
                                          .columns = kSdbCompactHistoryColumns};

constexpr duckdb::CatalogType kStoreTypes[] = {
  duckdb::CatalogType::INDEX_ENTRY, duckdb::CatalogType::TABLE_ENTRY};

constexpr SystemIndex kStoreIndexes[] = {
  {kSdbCompactHistorySql["relation_id"], SystemLookup::Object},
};

std::optional<duckdb::timestamp_tz_t> At(int64_t micros) {
  if (micros == 0) {
    return std::nullopt;
  }
  return duckdb::timestamp_tz_t{micros};
}

using search::MergeRecord;

template<irs::MergeStep Step>
double StepMillis(const MergeRecord& record) {
  return static_cast<double>(record.stats.profile.Nanos(Step)) / 1e6;
}

constexpr std::tuple kSegment{
  Col<"id">(&MergeRecord::id),
  Col<"relation_id">(&MergeRecord::relation_id),
  Col<"type">([](const auto& r) { return irs::SnakeCaseName(r.event); }),
  Col<"started">([](const auto& r) { return At(r.started_us); }),
  Col<"finished">([](const auto& r) { return At(r.finished_us); }),
  Col<"output">(
    [](const auto& r) { return NonEmpty(std::string_view{r.stats.output}); }),
  Col<"output_docs">([](const auto& r) { return r.stats.output_docs; }),
  Col<"output_live_docs">(&MergeRecord::output_live_docs),
  Col<"output_bytes">([](const auto& r) { return r.stats.output_bytes; }),
  Col<"rowid_min">([](const auto& r) {
    return r.rowids.Empty() ? std::nullopt
                            : std::optional<int64_t>{r.rowids.first};
  }),
  Col<"rowid_max">([](const auto& r) {
    return r.rowids.Empty() ? std::nullopt
                            : std::optional<int64_t>{r.rowids.last};
  }),
  Col<"error">(
    [](const auto& r) { return NonEmpty(std::string_view{r.error}); })};

constexpr std::tuple kMergeOnly{
  Col<"origin">([](const auto& r) { return irs::SnakeCaseName(r.origin); }),
  Col<"outcome">([](const auto& r) { return irs::SnakeCaseName(r.outcome); }),
  Col<"inputs">([](const auto& r) -> const auto& { return r.stats.inputs; }),
  Col<"input_docs">([](const auto& r) { return r.stats.input_docs; }),
  Col<"input_live_docs">([](const auto& r) { return r.stats.input_live_docs; }),
  Col<"input_bytes">([](const auto& r) { return r.stats.input_bytes; }),
  Col<"total_ms">([](const auto& r) {
    return static_cast<double>(r.stats.profile.TotalNanos()) / 1e6;
  }),
  Col<"prepare_ms">(&StepMillis<irs::MergeStep::Prepare>),
  Col<"norms_ms">(&StepMillis<irs::MergeStep::Norms>),
  Col<"columns_ms">(&StepMillis<irs::MergeStep::Columns>),
  Col<"columns_commit_ms">(&StepMillis<irs::MergeStep::ColumnsCommit>),
  Col<"ann_ms">(&StepMillis<irs::MergeStep::Ann>),
  Col<"postings_ms">(&StepMillis<irs::MergeStep::Postings>),
  Col<"commit_ms">(&StepMillis<irs::MergeStep::Commit>)};

class SdbCompactHistory final : public SystemTableScan<kSdbCompactHistorySql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kStoreTypes, SystemSchemas::Skip, kStoreIndexes}};

  static constexpr auto kFlush = Shape<kSql, const MergeRecord>(kSegment);

  static constexpr auto kMerge =
    Shape<kSql, const MergeRecord>(kSegment, kMergeOnly);

  void Row(const duckdb::IndexCatalogEntry& index) {
    if (!Allows<"relation_id">(index.oid) ||
        index.index_type != catalog::kInvertedIndexTypeName ||
        !index.Cast<catalog::InvertedIndexEntry>().Storage()) {
      return;
    }
    Events(index.oid);
  }

  void Row(const duckdb::TableCatalogEntry& table) {
    if (!Allows<"relation_id">(table.oid) ||
        !dynamic_cast<const catalog::SearchTableEntry*>(&table)) {
      return;
    }
    Events(table.oid);
  }

 private:
  void Events(duckdb::idx_t relation) {
    for (const auto& record :
         search::MergeRegistry::Instance().History(relation)) {
      if (record.event == search::SegmentEvent::Flush) {
        Emit<kFlush>(record);
      } else {
        Emit<kMerge>(record);
      }
    }
  }
};

}  // namespace

SystemTable gSdbCompactHistory = SystemTableOf<SdbCompactHistory>();

}  // namespace sdb::pg
