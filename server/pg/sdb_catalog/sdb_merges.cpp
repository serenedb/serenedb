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

#include "sdb_merges.h"

#include <absl/container/flat_hash_set.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <duckdb/catalog/catalog.hpp>
#include <string_view>
#include <vector>

#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "search/compaction/merge_registry.h"

namespace sdb::pg {
namespace {

absl::flat_hash_set<uint64_t> CurrentRelations(duckdb::ClientContext& context) {
  absl::flat_hash_set<uint64_t> relations;
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, duckdb::Identifier::InvalidCatalog());
  VisitSchemas(context, catalog, [&](duckdb::SchemaCatalogEntry& schema) {
    schema.Scan(context, duckdb::CatalogType::INDEX_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  const auto* index =
                    dynamic_cast<const catalog::InvertedIndexEntry*>(&entry);
                  if (index && index->Storage()) {
                    relations.insert(index->oid);
                  }
                });
    schema.Scan(context, duckdb::CatalogType::TABLE_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  if (dynamic_cast<const catalog::SearchTableEntry*>(&entry)) {
                    relations.insert(entry.oid);
                  }
                });
  });
  return relations;
}

std::vector<search::MergeRecord> Visible(
  std::vector<search::MergeRecord> records,
  const absl::flat_hash_set<uint64_t>& relations) {
  std::erase_if(records, [&](const search::MergeRecord& record) {
    return !relations.contains(record.relation_id);
  });
  return records;
}

Timestamptz At(int64_t micros) {
  return {.micros = micros, .is_null = micros == 0};
}

double Millis(uint64_t nanos) { return static_cast<double>(nanos) / 1e6; }

double StepMillis(const search::MergeRecord& record, irs::MergeStep step) {
  return Millis(record.stats.profile.Nanos(step));
}

std::vector<Text> Names(const search::MergeRecord& record) {
  std::vector<Text> names;
  names.reserve(record.stats.inputs.size());
  for (const auto& name : record.stats.inputs) {
    names.emplace_back(name);
  }
  return names;
}

Oid RelationOid(const search::MergeRecord& record) {
  return Oid{static_cast<uint32_t>(record.relation_id)};
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<SdbMerges>::GetTableData() {
  const auto records = Visible(search::MergeRegistry::Instance().Running(),
                               CurrentRelations(_context));
  const auto now = absl::ToUnixMicros(absl::Now());
  std::vector<std::vector<Text>> inputs;
  inputs.reserve(records.size());
  auto result = CreateColumns<SdbMerges>(records.size());
  for (size_t row = 0; row < records.size(); ++row) {
    const auto& record = records[row];
    const auto& names = inputs.emplace_back(Names(record));
    const SdbMerges value{
      .merge_id = record.id,
      .relation_id = RelationOid(record),
      .origin = search::MergeOriginName(record.origin),
      .started = At(record.started_us),
      .elapsed_ms = static_cast<double>(now - record.started_us) / 1000,
      .inputs = names,
      .input_docs = static_cast<int64_t>(record.stats.input_docs),
      .input_live_docs = static_cast<int64_t>(record.stats.input_live_docs),
      .input_bytes = static_cast<int64_t>(record.stats.input_bytes),
    };
    WriteData(result, value, 0, row, Roles());
  }
  return {std::move(result), records.size()};
}

template<>
MaterializedData SystemTableSnapshot<SdbMergeHistory>::GetTableData() {
  const auto records = Visible(search::MergeRegistry::Instance().History(),
                               CurrentRelations(_context));
  std::vector<std::vector<Text>> inputs;
  inputs.reserve(records.size());
  auto result = CreateColumns<SdbMergeHistory>(records.size());
  for (size_t row = 0; row < records.size(); ++row) {
    const auto& record = records[row];
    const auto& stats = record.stats;
    const auto& names = inputs.emplace_back(Names(record));
    const SdbMergeHistory value{
      .merge_id = record.id,
      .relation_id = RelationOid(record),
      .origin = search::MergeOriginName(record.origin),
      .outcome = search::MergeOutcomeName(record.outcome),
      .started = At(record.started_us),
      .finished = At(record.finished_us),
      .inputs = names,
      .input_docs = static_cast<int64_t>(stats.input_docs),
      .input_live_docs = static_cast<int64_t>(stats.input_live_docs),
      .input_bytes = static_cast<int64_t>(stats.input_bytes),
      .output = stats.output,
      .output_docs = static_cast<int64_t>(stats.output_docs),
      .output_bytes = static_cast<int64_t>(stats.output_bytes),
      .total_ms = Millis(stats.profile.TotalNanos()),
      .prepare_ms = StepMillis(record, irs::MergeStep::Prepare),
      .norms_ms = StepMillis(record, irs::MergeStep::Norms),
      .columns_ms = StepMillis(record, irs::MergeStep::Columns),
      .columns_commit_ms = StepMillis(record, irs::MergeStep::ColumnsCommit),
      .ann_ms = StepMillis(record, irs::MergeStep::Ann),
      .postings_ms = StepMillis(record, irs::MergeStep::Postings),
      .commit_ms = StepMillis(record, irs::MergeStep::Commit),
      .error = record.error,
    };
    uint64_t nulls = 0;
    if (stats.output.empty()) {
      nulls |= MaskFromNulls({GetIndex(&SdbMergeHistory::output)});
    }
    if (record.error.empty()) {
      nulls |= MaskFromNulls({GetIndex(&SdbMergeHistory::error)});
    }
    WriteData(result, value, nulls, row, Roles());
  }
  return {std::move(result), records.size()};
}

}  // namespace sdb::pg
