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

#include <cstdint>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <iresearch/index/merge/merge_profile.hpp>
#include <iresearch/utils/enum_name.hpp>
#include <string_view>
#include <type_traits>

#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/sdb/merge_records.h"
#include "pg/catalog/tables/tables.h"
#include "search/compaction/merge_registry.h"
#include "storage_engine/search_engine.h"

namespace sdb::pg {
namespace {

constexpr SystemSqlColumn kSdbCompactHistoryColumns[] = {
  {"id", kInt8, 0, false, false, 0, 0, SystemKey::None},
  {"relation_id", kOid, 0, false, false, 0, 0, SystemKey::None},
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
  {"output_bytes", kInt8, 0, false, false, 0, 0, SystemKey::None},
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

constexpr SystemIndex kStoreIndexes[] = {
  {kSdbCompactHistorySql["relation_id"], SystemLookup::Object},
};

using search::MergeRecord;

double Millis(uint64_t nanos) { return static_cast<double>(nanos) / 1e6; }

template<irs::MergeStep Step>
constexpr auto kStepMillis =
  [](const MergeRecord& r) { return Millis(r.stats.profile.Nanos(Step)); };

constexpr std::tuple kMergeColumns{
  Col<"id">(&MergeRecord::id),
  Col<"relation_id">(&MergeRecord::relation_id),
  Col<"origin">([](const auto& r) { return irs::SnakeCaseName(r.origin); }),
  Col<"outcome">([](const auto& r) { return irs::SnakeCaseName(r.outcome); }),
  Col<"started">([](const auto& r) { return At(r.started_us); }),
  Col<"finished">([](const auto& r) { return At(r.finished_us); }),
  Col<"output">(
    [](const auto& r) { return NonEmpty(std::string_view{r.stats.output}); }),
  Col<"output_docs">([](const auto& r) { return r.stats.output_docs; }),
  Col<"output_bytes">([](const auto& r) { return r.stats.output_bytes; }),
  Col<"total_ms">(
    [](const auto& r) { return Millis(r.stats.profile.TotalNanos()); }),
  Col<"prepare_ms">(kStepMillis<irs::MergeStep::Prepare>),
  Col<"norms_ms">(kStepMillis<irs::MergeStep::Norms>),
  Col<"columns_ms">(kStepMillis<irs::MergeStep::Columns>),
  Col<"columns_commit_ms">(kStepMillis<irs::MergeStep::ColumnsCommit>),
  Col<"ann_ms">(kStepMillis<irs::MergeStep::Ann>),
  Col<"postings_ms">(kStepMillis<irs::MergeStep::Postings>),
  Col<"commit_ms">(kStepMillis<irs::MergeStep::Commit>),
  Col<"error">(
    [](const auto& r) { return NonEmpty(std::string_view{r.error}); })};

class SdbCompactHistory final : public SystemTableScan<kSdbCompactHistorySql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSource{kStoreTypes, SystemSchemas::Skip, kStoreIndexes}};

  static constexpr auto kMerge =
    Shape<kSql, const MergeRecord>(kMergeColumns, kMergeInputColumns);
  static_assert([]() consteval {
    for (const auto step : search::kMergeStepColumns) {
      if ((std::remove_cvref_t<decltype(kMerge)>::kProvided >>
             kSdbCompactHistorySql[step] &
           1) == 0) {
        return false;
      }
    }
    return true;
  }());

  void Row(const duckdb::IndexCatalogEntry& index) { Events(index); }

  void Row(const duckdb::TableCatalogEntry& table) { Events(table); }

 private:
  void Events(const auto& relation) {
    if (!Allows<"relation_id">(relation.oid) || !OwnsStore(relation)) {
      return;
    }
    for (const auto& record :
         search::GetSearchEngine().Merges().History(relation.oid)) {
      Emit<kMerge>(record);
    }
  }
};

}  // namespace

SystemTable gSdbCompactHistory = SystemTableOf<SdbCompactHistory>();

}  // namespace sdb::pg
